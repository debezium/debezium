/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.mysql.antlr.listener;

import java.util.List;
import java.util.stream.Collectors;

import org.antlr.v4.runtime.tree.ParseTreeListener;

import io.debezium.connector.mysql.antlr.MySqlAntlrDdlParser;
import io.debezium.ddl.parser.mysql.generated.MySqlParser;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;

/**
 * Parser listener that is parsing MySQL CREATE TABLE statements.
 *
 * @author Roman Kuchár <kucharrom@gmail.com>.
 */
public class CreateTableParserListener extends TableCommonParserListener {

    public CreateTableParserListener(MySqlAntlrDdlParser parser, List<ParseTreeListener> listeners) {
        super(parser, listeners);
    }

    @Override
    public void enterCreateTable(MySqlParser.CreateTableContext ctx) {
        TableId tableId = parser.parseQualifiedTableId(ctx.tableName());

        // Check if it's a LIKE statement (copy table)
        if (ctx.LIKE_SYMBOL() != null) {
            // This is handled in exitCreateTable for LIKE
            return;
        }

        if (parser.databaseTables().forTable(tableId) == null) {
            tableEditor = parser.databaseTables().editOrCreateTable(tableId);
            super.enterCreateTable(ctx);
        }
    }

    @Override
    public void exitCreateTable(MySqlParser.CreateTableContext ctx) {
        TableId tableId = parser.parseQualifiedTableId(ctx.tableName());

        // Handle LIKE (copy table)
        if (ctx.LIKE_SYMBOL() != null && ctx.tableRef() != null) {
            TableId originalTableId = parser.parseQualifiedTableId(ctx.tableRef());
            Table original = parser.databaseTables().forTable(originalTableId);
            if (original != null) {
                parser.databaseTables().overwriteTable(tableId, original.columns(), original.primaryKeyColumnNames(), original.defaultCharsetName(),
                        original.attributes());
                parser.signalCreateTable(tableId, ctx.getParent());
            }
            super.exitCreateTable(ctx);
            // Clear tableEditor to prevent stale state from affecting subsequent statements
            tableEditor = null;
            return;
        }

        // Handle regular CREATE TABLE
        parser.runIfNotNull(() -> {
            // Make sure that the table's character set has been set ...
            if (!tableEditor.hasDefaultCharsetName()) {
                tableEditor.setDefaultCharsetName(parser.charsetForTable(tableEditor.tableId()));
            }
            listeners.remove(columnDefinitionListener);
            columnDefinitionListener = null;
            // remove column definition parser listener
            final String defaultCharsetName = tableEditor.create().defaultCharsetName();
            tableEditor.setColumns(tableEditor.columns().stream()
                    .map(
                            column -> {
                                final ColumnEditor columnEditor = column.edit();
                                if (columnEditor.charsetNameOfTable() == null) {
                                    columnEditor.charsetNameOfTable(defaultCharsetName);
                                }
                                return columnEditor;
                            })
                    .map(ColumnEditor::create)
                    .collect(Collectors.toList()));
            parser.databaseTables().overwriteTable(tableEditor.create());
            parser.signalCreateTable(tableEditor.tableId(), ctx.getParent());
        }, tableEditor);
        super.exitCreateTable(ctx);
        // Clear tableEditor to prevent stale state from affecting subsequent statements
        tableEditor = null;
    }

    @Override
    public void enterDefaultCharset(MySqlParser.DefaultCharsetContext ctx) {
        parser.runIfNotNull(() -> {
            if (ctx.charsetName() != null) {
                String charsetName = parser.withoutQuotes(ctx.charsetName());
                if ("default".equalsIgnoreCase(charsetName)) {
                    charsetName = parser.charsetForTable(tableEditor.tableId());
                }
                tableEditor.setDefaultCharsetName(charsetName);
            }
        }, tableEditor);
        super.enterDefaultCharset(ctx);
    }

    @Override
    public void enterCreateTableOption(MySqlParser.CreateTableOptionContext ctx) {
        if (!parser.skipComments()) {
            parser.runIfNotNull(() -> {
                if (ctx.option != null && ctx.option.getType() == MySqlParser.COMMENT_SYMBOL && ctx.textStringLiteral() != null) {
                    tableEditor.setComment(parser.withoutQuotes(ctx.textStringLiteral().getText()));
                }
            }, tableEditor);
        }
        super.enterCreateTableOption(ctx);
    }

}
