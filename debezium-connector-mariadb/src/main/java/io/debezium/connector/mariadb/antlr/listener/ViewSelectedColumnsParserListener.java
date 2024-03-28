/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb.antlr.listener;

import static io.debezium.relational.ddl.AbstractDdlParser.withoutQuotes;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.debezium.connector.mariadb.antlr.MariaDbAntlrDdlParser;
import io.debezium.ddl.parser.mysql.generated.MySqlParser;
import io.debezium.ddl.parser.mysql.generated.MySqlParserBaseListener;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;

/**
 * Parser listener that parses SELECT statements used for definitions of VIEWs.
 *
 * @author Chris Cranford
 */
public class ViewSelectedColumnsParserListener extends MySqlParserBaseListener {

    private final MariaDbAntlrDdlParser parser;
    private final TableEditor tableEditor;

    private TableEditor selectTableEditor;
    private Map<TableId, Table> tableByAlias = new HashMap<>();

    public ViewSelectedColumnsParserListener(TableEditor tableEditor, MariaDbAntlrDdlParser parser) {
        this.tableEditor = tableEditor;
        this.parser = parser;
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
        parser.runIfNotNull(() -> {
            parseAtomTableItem(ctx, tableByAlias);
        }, tableEditor);
        super.exitAtomTableItem(ctx);
    }

    @Override
    public void exitSubqueryTableItem(MySqlParser.SubqueryTableItemContext ctx) {
        parser.runIfNotNull(() -> {
            // parsing subselect
            String tableAlias = parser.parseName(ctx.uid());
            TableId aliasTableId = parser.resolveTableId(parser.currentSchema(), tableAlias);
            selectTableEditor.tableId(aliasTableId);
            tableByAlias.put(aliasTableId, selectTableEditor.create());
        }, tableEditor);
        super.exitSubqueryTableItem(ctx);
    }

    private void parseQuerySpecification(MySqlParser.SelectElementsContext selectElementsContext) {
        parser.runIfNotNull(() -> {
            selectTableEditor = parseSelectElements(selectElementsContext);
        }, tableEditor);
    }

    private void parseAtomTableItem(MySqlParser.TableSourceItemContext ctx, Map<TableId, Table> tableByAlias) {
        if (ctx instanceof MySqlParser.AtomTableItemContext) {
            MySqlParser.AtomTableItemContext atomTableItemContext = (MySqlParser.AtomTableItemContext) ctx;

            TableId tableId = parser.parseQualifiedTableId(atomTableItemContext.tableName().fullId());

            Table table = tableByAlias.get(tableId);
            if (table == null) {
                table = parser.databaseTables().forTable(tableId);
            }
            if (atomTableItemContext.alias != null) {
                TableId aliasTableId = parser.resolveTableId(tableId.catalog(), parser.parseName(atomTableItemContext.alias));
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
                    TableId tableId = parser.parseQualifiedTableId(((MySqlParser.SelectStarElementContext) selectElementContext).fullId());
                    Table selectedTable = tableByAlias.get(tableId);
                    table.addColumns(selectedTable.columns());
                }
                else if (selectElementContext instanceof MySqlParser.SelectColumnElementContext) {
                    MySqlParser.SelectColumnElementContext selectColumnElementContext = (MySqlParser.SelectColumnElementContext) selectElementContext;
                    MySqlParser.FullColumnNameContext fullColumnNameContext = selectColumnElementContext.fullColumnName();

                    String schemaName = parser.currentSchema();
                    String tableName = null;
                    String columnName;

                    columnName = parser.parseName(fullColumnNameContext.uid());
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
                        alias = parser.parseName(selectColumnElementContext.uid());
                    }
                    if (tableName != null) {
                        Table selectedTable = tableByAlias.get(parser.resolveTableId(schemaName, tableName));
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
