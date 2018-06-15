/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.mysql.antlr.listener;

import io.debezium.connector.mysql.antlr.MySqlAntlrDdlParser;
import io.debezium.ddl.parser.mysql.generated.MySqlParser;
import io.debezium.ddl.parser.mysql.generated.MySqlParserBaseListener;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.debezium.relational.ddl.AbstractDdlParser.withoutQuotes;

/**
 * Parser listeners that is parsing MySQL SELECT statements used for definition of VIEW.
 *
 * @author Roman Kuchár <kucharrom@gmail.com>.
 */
public class ViewSelectedColumnsParserListener extends MySqlParserBaseListener {

    private final MySqlAntlrDdlParser parserCtx;
    private final TableEditor tableEditor;

    private TableEditor selectTableEditor;
    private Map<TableId, Table> tableByAlias = new HashMap<>();

    public ViewSelectedColumnsParserListener(TableEditor tableEditor, MySqlAntlrDdlParser parserCtx) {
        this.tableEditor = tableEditor;
        this.parserCtx = parserCtx;
    }

    public List<Column> getSelectedColumns() {
        return selectTableEditor.columns();
    }

    @Override
    public void exitQuerySpecification(MySqlParser.QuerySpecificationContext ctx) {
        if (ctx.fromClause() != null) {
            parseQuerySpecification(ctx.selectElements());
        }
        super.exitQuerySpecification(ctx);
    }

    @Override
    public void exitQuerySpecificationNointo(MySqlParser.QuerySpecificationNointoContext ctx) {
        if (ctx.fromClause() != null) {
            parseQuerySpecification(ctx.selectElements());
        }
        super.exitQuerySpecificationNointo(ctx);
    }

    @Override
    public void exitAtomTableItem(MySqlParser.AtomTableItemContext ctx) {
        parserCtx.runIfNotNull(() -> {
            parseAtomTableItem(ctx, tableByAlias);
        }, tableEditor);
        super.exitAtomTableItem(ctx);
    }

    @Override
    public void exitSubqueryTableItem(MySqlParser.SubqueryTableItemContext ctx) {
        parserCtx.runIfNotNull(() -> {
            // parsing subselect
            String tableAlias = parserCtx.parseName(ctx.uid());
            TableId aliasTableId = parserCtx.resolveTableId(parserCtx.currentSchema(), tableAlias);
            selectTableEditor.tableId(aliasTableId);
            tableByAlias.put(aliasTableId, selectTableEditor.create());
        }, tableEditor);
        super.exitSubqueryTableItem(ctx);
    }

    private void parseQuerySpecification(MySqlParser.SelectElementsContext selectElementsContext) {
        parserCtx.runIfNotNull(() -> {
            selectTableEditor = parseSelectElements(selectElementsContext);
        }, tableEditor);
    }

    private void parseAtomTableItem(MySqlParser.TableSourceItemContext ctx, Map<TableId, Table> tableByAlias) {
        if (ctx instanceof MySqlParser.AtomTableItemContext) {
            MySqlParser.AtomTableItemContext atomTableItemContext = (MySqlParser.AtomTableItemContext) ctx;

            TableId tableId = parserCtx.parseQualifiedTableId(atomTableItemContext.tableName().fullId());

            Table table = tableByAlias.get(tableId);
            if (table == null) {
                table = parserCtx.databaseTables().forTable(tableId);
            }
            if (atomTableItemContext.alias != null) {
                TableId aliasTableId = parserCtx.resolveTableId(tableId.schema(), parserCtx.parseName(atomTableItemContext.alias));
                tableByAlias.put(aliasTableId, table);
            }
            else {
                tableByAlias.put(tableId, table);
            }
        }
    }

    private TableEditor parseSelectElements(MySqlParser.SelectElementsContext ctx) {
        TableEditor table = Table.editor();
        if (ctx.star != null) {
            tableByAlias.keySet().forEach(tableId -> {
                table.addColumns(tableByAlias.get(tableId).columns());
            });
        }
        else {
            ctx.selectElement().forEach(selectElementContext -> {
                if (selectElementContext instanceof MySqlParser.SelectStarElementContext) {
                    TableId tableId = parserCtx.parseQualifiedTableId(((MySqlParser.SelectStarElementContext) selectElementContext).fullId());
                    Table selectedTable = tableByAlias.get(tableId);
                    table.addColumns(selectedTable.columns());
                }
                else if (selectElementContext instanceof MySqlParser.SelectColumnElementContext) {
                    MySqlParser.SelectColumnElementContext selectColumnElementContext = (MySqlParser.SelectColumnElementContext) selectElementContext;
                    MySqlParser.FullColumnNameContext fullColumnNameContext = selectColumnElementContext.fullColumnName();

                    String schemaName = parserCtx.currentSchema();
                    String tableName = null;
                    String columnName;

                    columnName = parserCtx.parseName(fullColumnNameContext.uid());
                    if (fullColumnNameContext.dottedId(0) != null) {
                        // shift by 1
                        tableName = columnName;
                        if (fullColumnNameContext.dottedId(1) != null) {
                            // shift by 2
                            // final look of fullColumnName e.q. inventory.Persons.FirstName
                            schemaName = tableName;
                            tableName = withoutQuotes(fullColumnNameContext.dottedId(0).getText().substring(1));
                            columnName = withoutQuotes(fullColumnNameContext.dottedId(1).getText().substring(1));
                        }
                        else {
                            // final look of fullColumnName e.g. Persons.FirstName
                            columnName = withoutQuotes(fullColumnNameContext.dottedId(0).getText().substring(1));
                        }
                    }
                    String alias = columnName;
                    if (selectColumnElementContext.uid() != null) {
                        alias = parserCtx.parseName(selectColumnElementContext.uid());
                    }
                    if (tableName != null) {
                        Table selectedTable = tableByAlias.get(parserCtx.resolveTableId(schemaName, tableName));
                        addColumnFromTable(table, columnName, alias, selectedTable);
                    }
                    else {
                        for (Table selectedTable : tableByAlias.values()) {
                            addColumnFromTable(table, columnName, alias, selectedTable);
                        }
                    }
                }
            });
        }
        tableByAlias.clear();
        return table;
    }


    private MySqlParser.TableSourceItemContext getTableSourceItemContext(MySqlParser.TableSourceContext tableSourceContext) {
        if (tableSourceContext instanceof MySqlParser.TableSourceBaseContext) {
            return ((MySqlParser.TableSourceBaseContext) tableSourceContext).tableSourceItem();
        }
        else if (tableSourceContext instanceof MySqlParser.TableSourceNestedContext) {
            return ((MySqlParser.TableSourceNestedContext) tableSourceContext).tableSourceItem();
        }
        return null;
    }

    private void addColumnFromTable(TableEditor table, String columnName, String newColumnName, Table selectedTable) {
        for (Column column : selectedTable.columns()) {
            if (column.name().equals(columnName)) {
                table.addColumn(column.edit().name(newColumnName).create());
                break;
            }
        }
    }
}
