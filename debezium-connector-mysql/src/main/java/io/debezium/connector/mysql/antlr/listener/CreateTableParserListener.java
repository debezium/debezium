/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.mysql.antlr.listener;

import java.util.List;
import java.util.stream.Collectors;

import org.antlr.v4.runtime.tree.ParseTreeListener;

import io.debezium.connector.mysql.MySqlDefaultValueConverter;
import io.debezium.connector.mysql.antlr.MySqlAntlrDdlParser;
import io.debezium.ddl.parser.mysql.generated.MySqlParser;
import io.debezium.ddl.parser.mysql.generated.MySqlParserBaseListener;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.Table;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;

/**
 * Parser listener that is parsing MySQL CREATE TABLE statements.
 *
 * @author Roman Kuchár <kucharrom@gmail.com>.
 */
public class CreateTableParserListener extends MySqlParserBaseListener {

    private final List<ParseTreeListener> listeners;

    private final MySqlAntlrDdlParser parser;
    private TableEditor tableEditor;
    private ColumnDefinitionParserListener columnDefinitionListener;
    private final MySqlDefaultValueConverter defaultValueConverter;

    public CreateTableParserListener(MySqlAntlrDdlParser parser, List<ParseTreeListener> listeners) {
        this.parser = parser;
        this.listeners = listeners;
        this.defaultValueConverter = new MySqlDefaultValueConverter(parser.getConverters());
    }

    @Override
    public void enterColumnCreateTable(MySqlParser.ColumnCreateTableContext ctx) {
        TableId tableId = parser.parseQualifiedTableId(ctx.tableName().fullId());
        if (parser.databaseTables().forTable(tableId) == null) {
            tableEditor = parser.databaseTables().editOrCreateTable(tableId);
            super.enterColumnCreateTable(ctx);
        }
    }

    @Override
    public void exitColumnCreateTable(MySqlParser.ColumnCreateTableContext ctx) {
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
            parser.signalCreateTable(tableEditor.tableId(), ctx);
        }, tableEditor);
        super.exitColumnCreateTable(ctx);
    }

    @Override
    public void exitCopyCreateTable(MySqlParser.CopyCreateTableContext ctx) {
        TableId tableId = parser.parseQualifiedTableId(ctx.tableName(0).fullId());
        TableId originalTableId = parser.parseQualifiedTableId(ctx.tableName(1).fullId());
        Table original = parser.databaseTables().forTable(originalTableId);
        if (original != null) {
            parser.databaseTables().overwriteTable(tableId, original.columns(), original.primaryKeyColumnNames(), original.defaultCharsetName());
            parser.signalCreateTable(tableId, ctx);
        }
        super.exitCopyCreateTable(ctx);
    }

    @Override
    public void enterColumnDeclaration(MySqlParser.ColumnDeclarationContext ctx) {
        parser.runIfNotNull(() -> {
            String columnName = parser.parseName(ctx.uid());
            ColumnEditor columnEditor = Column.editor().name(columnName);
            if (columnDefinitionListener == null) {
                columnDefinitionListener = new ColumnDefinitionParserListener(tableEditor, columnEditor, parser, listeners);
                listeners.add(columnDefinitionListener);
            }
            else {
                columnDefinitionListener.setColumnEditor(columnEditor);
            }
        }, tableEditor);
        super.enterColumnDeclaration(ctx);
    }

    @Override
    public void exitColumnDeclaration(MySqlParser.ColumnDeclarationContext ctx) {
        parser.runIfNotNull(() -> {
            tableEditor.addColumn(columnDefinitionListener.getColumn());
        }, tableEditor, columnDefinitionListener);
        super.exitColumnDeclaration(ctx);
    }

    @Override
    public void enterPrimaryKeyTableConstraint(MySqlParser.PrimaryKeyTableConstraintContext ctx) {
        parser.runIfNotNull(() -> {
            parser.parsePrimaryIndexColumnNames(ctx.indexColumnNames(), tableEditor);
        }, tableEditor);
        super.enterPrimaryKeyTableConstraint(ctx);
    }

    @Override
    public void enterUniqueKeyTableConstraint(MySqlParser.UniqueKeyTableConstraintContext ctx) {
        parser.runIfNotNull(() -> {
            if (!tableEditor.hasPrimaryKey()) {
                parser.parsePrimaryIndexColumnNames(ctx.indexColumnNames(), tableEditor);
            }
        }, tableEditor);
        super.enterUniqueKeyTableConstraint(ctx);
    }

    @Override
    public void enterTableOptionCharset(MySqlParser.TableOptionCharsetContext ctx) {
        parser.runIfNotNull(() -> {
            if (ctx.charsetName() != null) {
                tableEditor.setDefaultCharsetName(parser.withoutQuotes(ctx.charsetName()));
            }
        }, tableEditor);
        super.enterTableOptionCharset(ctx);
    }

    @Override
    public void enterTableOptionComment(MySqlParser.TableOptionCommentContext ctx) {
        if (!parser.skipComments()) {
            parser.runIfNotNull(() -> {
                if (ctx.COMMENT() != null) {
                    tableEditor.setComment(parser.withoutQuotes(ctx.STRING_LITERAL().getText()));
                }
            }, tableEditor);
        }
        super.enterTableOptionComment(ctx);
    }

}
