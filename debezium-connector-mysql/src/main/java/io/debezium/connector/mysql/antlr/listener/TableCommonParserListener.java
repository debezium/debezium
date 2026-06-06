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
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.TableEditor;

/**
 * Parser listener that is parsing MySQL ALTER/CREATE TABLE common statements.
 *
 * @author Harvey Yue
 */
public class TableCommonParserListener extends MySqlParserBaseListener {

    protected final List<ParseTreeListener> listeners;
    protected final MySqlAntlrDdlParser parser;

    protected TableEditor tableEditor;
    protected ColumnDefinitionParserListener columnDefinitionListener;

    public TableCommonParserListener(MySqlAntlrDdlParser parser, List<ParseTreeListener> listeners) {
        this.parser = parser;
        this.listeners = listeners;
    }

    @Override
    public void enterColumnDefinition(MySqlParser.ColumnDefinitionContext ctx) {
        parser.runIfNotNull(() -> {
            String columnName = parser.removeRepeatedBacktick(parser.parseName(ctx.columnName().identifier()));
            ColumnEditor columnEditor = Column.editor().name(columnName);
            if (columnDefinitionListener == null) {
                columnDefinitionListener = new ColumnDefinitionParserListener(tableEditor, columnEditor, parser, listeners);
                listeners.add(columnDefinitionListener);
                // Manually invoke enterColumnDefinition since the parse tree walker has already
                // started dispatching this event before we added this listener to the list
                columnDefinitionListener.enterColumnDefinition(ctx);
            }
            else {
                columnDefinitionListener.setColumnEditor(columnEditor);
            }
        }, tableEditor);
        super.enterColumnDefinition(ctx);
    }

    @Override
    public void exitColumnDefinition(MySqlParser.ColumnDefinitionContext ctx) {
        // Column is added to table in ColumnDefinitionParserListener.exitColumnDefinition()
        // after all properties have been set
        super.exitColumnDefinition(ctx);
    }

    @Override
    public void enterTableConstraintDef(MySqlParser.TableConstraintDefContext ctx) {
        parser.runIfNotNull(() -> {
            // Check if it's a PRIMARY KEY constraint
            if (ctx.type != null && ctx.type.getType() == MySqlParser.PRIMARY_SYMBOL && ctx.keyListWithExpression() != null) {
                parser.parsePrimaryIndexColumnNames(ctx.keyListWithExpression(), tableEditor);
            }
            // Check if it's a UNIQUE KEY constraint
            else if (ctx.type != null && ctx.type.getType() == MySqlParser.UNIQUE_SYMBOL && ctx.keyListWithExpression() != null) {
                if (!tableEditor.hasPrimaryKey() && parser.isTableUniqueIndexIncluded(ctx.keyListWithExpression(), tableEditor)) {
                    parser.parseUniqueIndexColumnNames(ctx.keyListWithExpression(), tableEditor);
                }
            }
        }, tableEditor);
        super.enterTableConstraintDef(ctx);
    }
}
