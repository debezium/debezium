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
    public void enterColumnDeclaration(MySqlParser.ColumnDeclarationContext ctx) {
        parser.runIfNotNull(() -> {
            MySqlParser.FullColumnNameContext fullColumnNameContext = ctx.fullColumnName();
            List<MySqlParser.DottedIdContext> dottedIdContextList = fullColumnNameContext.dottedId();
            MySqlParser.UidContext uidContext = fullColumnNameContext.uid();
            if (!dottedIdContextList.isEmpty()) {
                uidContext = dottedIdContextList.get(dottedIdContextList.size() - 1).uid();
            }

            String columnName = parser.parseName(uidContext);
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
            if (!tableEditor.hasPrimaryKey() && parser.isTableUniqueIndexIncluded(ctx.indexColumnNames(), tableEditor)) {
                parser.parseUniqueIndexColumnNames(ctx.indexColumnNames(), tableEditor);
            }
        }, tableEditor);
        super.enterUniqueKeyTableConstraint(ctx);
    }
}
