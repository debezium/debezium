/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.mysql.antlr.listener;

import java.util.List;

import org.antlr.v4.runtime.tree.ParseTreeListener;

import io.debezium.connector.mysql.antlr.MySqlAntlrDdlParser;
import io.debezium.ddl.parser.mysql.generated.MySqlParser;
import io.debezium.ddl.parser.mysql.generated.MySqlParserBaseListener;
import io.debezium.relational.Column;
import io.debezium.relational.TableEditor;

/**
 * Parser listener that is parsing MySQL CREATE VIEW statements.
 *
 * @author Roman Kuchár <kucharrom@gmail.com>.
 */
public class CreateViewParserListener extends MySqlParserBaseListener {

    private final MySqlAntlrDdlParser parser;
    private final List<ParseTreeListener> listeners;

    private TableEditor tableEditor;
    private ViewSelectedColumnsParserListener selectColumnsListener;

    public CreateViewParserListener(MySqlAntlrDdlParser parser, List<ParseTreeListener> listeners) {
        this.parser = parser;
        this.listeners = listeners;
    }

    @Override
    public void enterCreateView(MySqlParser.CreateViewContext ctx) {
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
    public void exitCreateView(MySqlParser.CreateViewContext ctx) {
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
