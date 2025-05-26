/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.mysql.antlr.listener;

import java.util.List;

import org.antlr.v4.runtime.tree.ParseTreeListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.antlr.AntlrDdlParser;
import io.debezium.connector.mysql.antlr.MySqlAntlrDdlParser;
import io.debezium.ddl.parser.mysql.generated.MySqlParser;
import io.debezium.ddl.parser.mysql.generated.MySqlParserBaseListener;
import io.debezium.relational.Column;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;
import io.debezium.text.ParsingException;

/**
 * Parser listener that is parsing MySQL ALTER VIEW statements.
 *
 * @author Roman Kuchár <kucharrom@gmail.com>.
 */
public class AlterViewParserListener extends MySqlParserBaseListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(AlterViewParserListener.class);
    private final MySqlAntlrDdlParser parser;
    private final List<ParseTreeListener> listeners;

    private TableEditor tableEditor;
    private ViewSelectedColumnsParserListener selectColumnsListener;

    public AlterViewParserListener(MySqlAntlrDdlParser parser, List<ParseTreeListener> listeners) {
        this.parser = parser;
        this.listeners = listeners;
    }

    @Override
    public void enterAlterView(MySqlParser.AlterViewContext ctx) {
        if (!parser.skipViews()) {
            TableId tableId = parser.parseQualifiedTableId(ctx.fullId());

            tableEditor = parser.databaseTables().editTable(tableId);
            if (tableEditor == null) {
                LOGGER.trace("Trying to alter view {}, which does not exist. Query: {}", tableId, AntlrDdlParser.getText(ctx));
                throw new ParsingException(null, "Trying to alter view " + tableId.toString() + ", which does not exist.");
            }
            // alter view will override existing columns for a new one
            tableEditor.columnNames().forEach(tableEditor::removeColumn);
            // create new columns just with specified name for now
            if (ctx.uidList() != null) {
                ctx.uidList().uid().stream().map(parser::parseName).forEach(columnName -> {
                    tableEditor.addColumn(Column.editor().name(columnName).create());
                });
            }
            selectColumnsListener = new ViewSelectedColumnsParserListener(tableEditor, parser);
            listeners.add(selectColumnsListener);
        }
        super.enterAlterView(ctx);
    }

    @Override
    public void exitAlterView(MySqlParser.AlterViewContext ctx) {
        parser.runIfNotNull(() -> {
            tableEditor.addColumns(selectColumnsListener.getSelectedColumns());
            // Make sure that the table's character set has been set ...
            if (!tableEditor.hasDefaultCharsetName()) {
                tableEditor.setDefaultCharsetName(parser.currentDatabaseCharset());
            }
            parser.databaseTables().overwriteTable(tableEditor.create());
            listeners.remove(selectColumnsListener);
        }, tableEditor);
        // signal view even if it was skipped
        parser.signalAlterView(parser.parseQualifiedTableId(ctx.fullId()), null, ctx);
        super.exitAlterView(ctx);
    }
}
