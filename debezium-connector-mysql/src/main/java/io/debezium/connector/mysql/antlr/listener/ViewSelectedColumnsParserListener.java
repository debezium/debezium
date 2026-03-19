/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.mysql.antlr.listener;

import static io.debezium.relational.ddl.AbstractDdlParser.withoutQuotes;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.debezium.connector.mysql.antlr.MySqlAntlrDdlParser;
import io.debezium.ddl.parser.mysql.generated.MySqlParser;
import io.debezium.ddl.parser.mysql.generated.MySqlParserBaseListener;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;

/**
 * Parser listener that is parsing MySQL SELECT statements used for definition of VIEW.
 *
 * @author Roman Kuchár <kucharrom@gmail.com>.
 */
public class ViewSelectedColumnsParserListener extends MySqlParserBaseListener {

    private final MySqlAntlrDdlParser parser;
    private final TableEditor tableEditor;

    private TableEditor selectTableEditor;
    private Map<TableId, Table> tableByAlias = new HashMap<>();

    public ViewSelectedColumnsParserListener(TableEditor tableEditor, MySqlAntlrDdlParser parser) {
        this.tableEditor = tableEditor;
        this.parser = parser;
    }

    public List<Column> getSelectedColumns() {
        return selectTableEditor.columns();
    }

    @Override
    public void exitQuerySpecification(MySqlParser.QuerySpecificationContext ctx) {
        if (ctx.fromClause() != null) {
            parseQuerySpecification(ctx.selectItemList());
        }
        super.exitQuerySpecification(ctx);
    }

    @Override
    public void exitSingleTable(MySqlParser.SingleTableContext ctx) {
        parser.runIfNotNull(() -> {
            parseSingleTable(ctx, tableByAlias);
        }, tableEditor);
        super.exitSingleTable(ctx);
    }

    @Override
    public void exitDerivedTable(MySqlParser.DerivedTableContext ctx) {
        parser.runIfNotNull(() -> {
            // parsing subselect (subquery with alias)
            if (ctx.tableAlias() != null && ctx.tableAlias().identifier() != null) {
                String tableAlias = parser.parseName(ctx.tableAlias().identifier());
                TableId aliasTableId = parser.resolveTableId(parser.currentSchema(), tableAlias);
                selectTableEditor.tableId(aliasTableId);
                tableByAlias.put(aliasTableId, selectTableEditor.create());
            }
        }, tableEditor);
        super.exitDerivedTable(ctx);
    }

    private void parseQuerySpecification(MySqlParser.SelectItemListContext selectItemListContext) {
        parser.runIfNotNull(() -> {
            selectTableEditor = parseSelectItemList(selectItemListContext);
        }, tableEditor);
    }

    private void parseSingleTable(MySqlParser.SingleTableContext ctx, Map<TableId, Table> tableByAlias) {
        TableId tableId = parser.parseQualifiedTableId(ctx.tableRef());

        Table table = tableByAlias.get(tableId);
        if (table == null) {
            table = parser.databaseTables().forTable(tableId);
        }

        // Check for table alias
        if (ctx.tableAlias() != null && ctx.tableAlias().identifier() != null) {
            String alias = parser.parseName(ctx.tableAlias().identifier());
            TableId aliasTableId = parser.resolveTableId(tableId.catalog(), alias);
            tableByAlias.put(aliasTableId, table);
        }
        else {
            tableByAlias.put(tableId, table);
        }
    }

    private TableEditor parseSelectItemList(MySqlParser.SelectItemListContext ctx) {
        TableEditor table = Table.editor();

        // Check for SELECT * (MULT_OPERATOR without any select items)
        if (ctx.MULT_OPERATOR() != null && (ctx.selectItem() == null || ctx.selectItem().isEmpty())) {
            tableByAlias.keySet().forEach(tableId -> {
                table.addColumns(tableByAlias.get(tableId).columns());
            });
        }
        else if (ctx.selectItem() != null) {
            ctx.selectItem().forEach(selectItemContext -> {
                // Check if it's a tableWild (e.g., table.*)
                if (selectItemContext.tableWild() != null) {
                    MySqlParser.TableWildContext tableWildCtx = selectItemContext.tableWild();
                    TableId tableId = parseTableWildIdentifier(tableWildCtx);
                    Table selectedTable = tableByAlias.get(tableId);
                    if (selectedTable != null) {
                        table.addColumns(selectedTable.columns());
                    }
                }
                // It's a regular expression (column reference or computed expression)
                else if (selectItemContext.expr() != null) {
                    parseSelectItemExpr(selectItemContext, table);
                }
            });
        }
        tableByAlias.clear();
        return table;
    }

    private TableId parseTableWildIdentifier(MySqlParser.TableWildContext ctx) {
        // tableWild: identifier DOT_SYMBOL (identifier DOT_SYMBOL)? MULT_OPERATOR
        String schemaName = parser.currentSchema();
        String tableName;

        List<MySqlParser.IdentifierContext> identifiers = ctx.identifier();
        if (identifiers.size() == 2) {
            // schema.table.*
            schemaName = parser.parseName(identifiers.get(0));
            tableName = parser.parseName(identifiers.get(1));
        }
        else {
            // table.*
            tableName = parser.parseName(identifiers.get(0));
        }

        return parser.resolveTableId(schemaName, tableName);
    }

    private void parseSelectItemExpr(MySqlParser.SelectItemContext selectItemContext, TableEditor table) {
        // Try to extract column reference from the expression
        MySqlParser.ExprContext exprCtx = selectItemContext.expr();

        // Attempt to parse simple column references
        String columnName = null;
        String tableName = null;
        String schemaName = parser.currentSchema();

        // Check if it's a simple identifier expression (column reference)
        if (isSimpleColumnReference(exprCtx)) {
            ColumnReference colRef = extractColumnReference(exprCtx);
            if (colRef != null) {
                columnName = colRef.columnName;
                tableName = colRef.tableName;
                if (colRef.schemaName != null) {
                    schemaName = colRef.schemaName;
                }
            }
        }

        // Determine the alias for the column
        String alias = columnName;
        if (selectItemContext.selectAlias() != null) {
            if (selectItemContext.selectAlias().identifier() != null) {
                alias = parser.parseName(selectItemContext.selectAlias().identifier());
            }
            else if (selectItemContext.selectAlias().textStringLiteral() != null) {
                alias = withoutQuotes(selectItemContext.selectAlias().textStringLiteral().getText());
            }
        }

        if (columnName != null) {
            if (tableName != null) {
                Table selectedTable = tableByAlias.get(parser.resolveTableId(schemaName, tableName));
                if (selectedTable != null) {
                    addColumnFromTable(table, columnName, alias, selectedTable);
                }
            }
            else {
                // Search across all tables
                for (Table selectedTable : tableByAlias.values()) {
                    if (addColumnFromTable(table, columnName, alias, selectedTable)) {
                        break;
                    }
                }
            }
        }
    }

    private boolean isSimpleColumnReference(MySqlParser.ExprContext exprCtx) {
        // Simple heuristic: if the expression text doesn't contain operators or functions,
        // it's likely a simple column reference
        String text = exprCtx.getText();
        return !text.contains("(") && !text.contains("+") && !text.contains("-") &&
                !text.contains("*") && !text.contains("/");
    }

    private static class ColumnReference {
        String schemaName;
        String tableName;
        String columnName;

        ColumnReference(String columnName) {
            this.columnName = columnName;
        }

        ColumnReference(String tableName, String columnName) {
            this.tableName = tableName;
            this.columnName = columnName;
        }

        ColumnReference(String schemaName, String tableName, String columnName) {
            this.schemaName = schemaName;
            this.tableName = tableName;
            this.columnName = columnName;
        }
    }

    private ColumnReference extractColumnReference(MySqlParser.ExprContext exprCtx) {
        // Try to extract from simpleIdentifier pattern
        String fullText = exprCtx.getText();
        String[] parts = fullText.split("\\.");

        if (parts.length == 1) {
            // Just column name
            return new ColumnReference(withoutQuotes(parts[0]));
        }
        else if (parts.length == 2) {
            // table.column
            return new ColumnReference(withoutQuotes(parts[0]), withoutQuotes(parts[1]));
        }
        else if (parts.length == 3) {
            // schema.table.column
            return new ColumnReference(withoutQuotes(parts[0]), withoutQuotes(parts[1]), withoutQuotes(parts[2]));
        }

        return null;
    }

    private boolean addColumnFromTable(TableEditor table, String columnName, String newColumnName, Table selectedTable) {
        for (Column column : selectedTable.columns()) {
            if (column.name().equals(columnName)) {
                table.addColumn(column.edit().name(newColumnName).create());
                return true;
            }
        }
        return false;
    }
}
