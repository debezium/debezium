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
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.Table;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;
import org.antlr.v4.runtime.tree.ParseTreeListener;

import java.util.List;

/**
 * Parser listeners that is parsing MySQL CREATE TABLE statements.
 *
 * @author Roman Kuchár <kucharrom@gmail.com>.
 */
public class CreateTableParserListener extends MySqlParserBaseListener {

    private final List<ParseTreeListener> listeners;

    private final MySqlAntlrDdlParser parserCtx;
    private TableEditor tableEditor;
    private ColumnDefinitionParserListener columnDefinitionListener;

    public CreateTableParserListener(MySqlAntlrDdlParser parserCtx, List<ParseTreeListener> listeners) {
        this.parserCtx = parserCtx;
        this.listeners = listeners;
    }

    @Override
    public void enterColumnCreateTable(MySqlParser.ColumnCreateTableContext ctx) {
        TableId tableId = parserCtx.parseQualifiedTableId(ctx.tableName().fullId());
        tableEditor = parserCtx.databaseTables().editOrCreateTable(tableId);
        super.enterColumnCreateTable(ctx);
    }

    @Override
    public void exitColumnCreateTable(MySqlParser.ColumnCreateTableContext ctx) {
        parserCtx.runIfNotNull(() -> {
            // Make sure that the table's character set has been set ...
            if (!tableEditor.hasDefaultCharsetName()) {
                tableEditor.setDefaultCharsetName(parserCtx.currentDatabaseCharset());
            }
            listeners.remove(columnDefinitionListener);
            columnDefinitionListener = null;
            // remove column definition parser listener
            parserCtx.databaseTables().overwriteTable(tableEditor.create());
            parserCtx.signalCreateTable(tableEditor.tableId(), ctx);
        }, tableEditor);
        super.exitColumnCreateTable(ctx);
    }

    @Override
    public void exitCopyCreateTable(MySqlParser.CopyCreateTableContext ctx) {
        TableId tableId = parserCtx.parseQualifiedTableId(ctx.tableName(0).fullId());
        TableId originalTableId = parserCtx.parseQualifiedTableId(ctx.tableName(1).fullId());
        Table original = parserCtx.databaseTables().forTable(originalTableId);
        if (original != null) {
            parserCtx.databaseTables().overwriteTable(tableId, original.columns(), original.primaryKeyColumnNames(), original.defaultCharsetName());
            parserCtx.signalCreateTable(tableId, ctx);
        }
        super.exitCopyCreateTable(ctx);
    }

    @Override
    public void enterColumnDeclaration(MySqlParser.ColumnDeclarationContext ctx) {
        parserCtx.runIfNotNull(() -> {
            String columnName = parserCtx.parseName(ctx.uid());
            ColumnEditor columnEditor = Column.editor().name(columnName);
            if (columnDefinitionListener == null) {
                columnDefinitionListener = new ColumnDefinitionParserListener(tableEditor, columnEditor, parserCtx.dataTypeResolver());
                listeners.add(columnDefinitionListener);
            } else {
                columnDefinitionListener.setColumnEditor(columnEditor);
            }
        }, tableEditor);
        super.enterColumnDeclaration(ctx);
    }

    @Override
    public void exitColumnDeclaration(MySqlParser.ColumnDeclarationContext ctx) {
        parserCtx.runIfNotNull(() -> {
            tableEditor.addColumn(columnDefinitionListener.getColumn());
        }, tableEditor, columnDefinitionListener);
        super.exitColumnDeclaration(ctx);
    }

    @Override
    public void enterPrimaryKeyTableConstraint(MySqlParser.PrimaryKeyTableConstraintContext ctx) {
        parserCtx.runIfNotNull(() -> {
            parserCtx.parsePrimaryIndexColumnNames(ctx.indexColumnNames(), tableEditor);
        }, tableEditor);
        super.enterPrimaryKeyTableConstraint(ctx);
    }

    @Override
    public void enterUniqueKeyTableConstraint(MySqlParser.UniqueKeyTableConstraintContext ctx) {
        parserCtx.runIfNotNull(() -> {
            if (!tableEditor.hasPrimaryKey()) {
                parserCtx.parsePrimaryIndexColumnNames(ctx.indexColumnNames(), tableEditor);
            }
        }, tableEditor);
        super.enterUniqueKeyTableConstraint(ctx);
    }

    @Override
    public void enterTableOptionCharset(MySqlParser.TableOptionCharsetContext ctx) {
        parserCtx.runIfNotNull(() -> {
            String charsetName = parserCtx.withoutQuotes(ctx.charsetName());
            tableEditor.setDefaultCharsetName(charsetName);
        }, tableEditor);
        super.enterTableOptionCharset(ctx);
    }
}