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
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.TableEditor;

/**
 * Parser listener that parses CREATE TABLE and ALTER TABLE common statements.
 *
 * @author Chris Cranford
 */
public class TableCommonParserListener extends MariaDBParserBaseListener {

    protected final MariaDbAntlrDdlParser parser;
    protected final List<ParseTreeListener> listeners;

    protected TableEditor tableEditor;
    protected ColumnDefinitionParserListener columnDefinitionListener;

    public TableCommonParserListener(MariaDbAntlrDdlParser parser, List<ParseTreeListener> listeners) {
        this.parser = parser;
        this.listeners = listeners;
    }

    @Override
    public void enterColumnDeclaration(MariaDBParser.ColumnDeclarationContext ctx) {
        parser.runIfNotNull(() -> {
            MariaDBParser.UidContext fullColumnNameContext = ctx.uid();
            String columnName = parser.parseName(fullColumnNameContext);
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
    public void exitColumnDeclaration(MariaDBParser.ColumnDeclarationContext ctx) {
        parser.runIfNotNull(() -> {
            tableEditor.addColumn(columnDefinitionListener.getColumn());
        }, tableEditor, columnDefinitionListener);
        super.exitColumnDeclaration(ctx);
    }

    @Override
    public void enterPrimaryKeyTableConstraint(MariaDBParser.PrimaryKeyTableConstraintContext ctx) {
        parser.runIfNotNull(() -> {
            parser.parsePrimaryIndexColumnNames(ctx.indexColumnNames(), tableEditor);
        }, tableEditor);
        super.enterPrimaryKeyTableConstraint(ctx);
    }

    @Override
    public void enterUniqueKeyTableConstraint(MariaDBParser.UniqueKeyTableConstraintContext ctx) {
        parser.runIfNotNull(() -> {
            if (!tableEditor.hasPrimaryKey() && parser.isTableUniqueIndexIncluded(ctx.indexColumnNames(), tableEditor)) {
                parser.parseUniqueIndexColumnNames(ctx.indexColumnNames(), tableEditor);
            }
        }, tableEditor);
        super.enterUniqueKeyTableConstraint(ctx);
    }
}
