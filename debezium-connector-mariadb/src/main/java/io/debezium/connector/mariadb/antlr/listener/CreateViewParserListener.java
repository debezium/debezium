/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb.antlr.listener;

import java.util.List;

import org.antlr.v4.runtime.tree.ParseTreeListener;

import io.debezium.connector.mariadb.antlr.MariaDbAntlrDdlParser;
import io.debezium.ddl.parser.mariadb.generated.MariaDBParser;
import io.debezium.ddl.parser.mariadb.generated.MariaDBParserBaseListener;
import io.debezium.relational.Column;
import io.debezium.relational.TableEditor;

/**
 * Parser listener for CREATE VIEW statements.
 *
 * @author Chris Cranford
 */
public class CreateViewParserListener extends MariaDBParserBaseListener {

    private final MariaDbAntlrDdlParser parser;
    private final List<ParseTreeListener> listeners;

    private TableEditor tableEditor;
    private ViewSelectedColumnsParserListener selectColumnsListener;

    public CreateViewParserListener(MariaDbAntlrDdlParser parser, List<ParseTreeListener> listeners) {
        this.parser = parser;
        this.listeners = listeners;
    }

    @Override
    public void enterCreateView(MariaDBParser.CreateViewContext ctx) {
        if (!parser.skipViews()) {
            tableEditor = parser.databaseTables().editOrCreateTable(parser.parseQualifiedTableId(ctx.fullId()));
            // create new columns just with specified name for now
            if (ctx.uidList() != null) {
                ctx.uidList().uid().stream().map(parser::parseName).forEach(columnName -> {
                    tableEditor.addColumn(Column.editor().name(columnName).create());
                });
            }
            selectColumnsListener = new ViewSelectedColumnsParserListener(tableEditor, parser);
            listeners.add(selectColumnsListener);
        }
        super.enterCreateView(ctx);
    }

    @Override
    public void exitCreateView(MariaDBParser.CreateViewContext ctx) {
        parser.runIfNotNull(() -> {
            tableEditor.addColumns(selectColumnsListener.getSelectedColumns());
            // Make sure that the table's character set has been set ...
            if (!tableEditor.hasDefaultCharsetName()) {
                tableEditor.setDefaultCharsetName(parser.charsetForTable(tableEditor.tableId()));
            }
            parser.databaseTables().overwriteTable(tableEditor.create());
            listeners.remove(selectColumnsListener);
        }, tableEditor);
        // signal view even if it was skipped
        parser.signalCreateView(parser.parseQualifiedTableId(ctx.fullId()), ctx);
        super.exitCreateView(ctx);
    }
}
