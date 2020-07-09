/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.antlr.listener;

import static io.debezium.antlr.AntlrDdlParser.getText;
import static io.debezium.connector.oracle.antlr.listener.ParserUtils.getColumnName;
import static io.debezium.connector.oracle.antlr.listener.ParserUtils.getTableName;

import java.util.ArrayList;
import java.util.List;

import org.antlr.v4.runtime.tree.ParseTreeListener;

import io.debezium.connector.oracle.antlr.OracleDdlParser;
import io.debezium.ddl.parser.oracle.generated.PlSqlParser;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;
import io.debezium.text.ParsingException;

/**
 * Parser listener that is parsing Oracle ALTER TABLE statements
 */
public class AlterTableParserListener extends BaseParserListener {

    private static final int STARTING_INDEX = 1;
    private TableEditor tableEditor;
    private String catalogName;
    private String schemaName;
    private OracleDdlParser parser;
    private final List<ParseTreeListener> listeners;
    private ColumnDefinitionParserListener columnDefinitionParserListener;
    private List<ColumnEditor> columnEditors;
    private int parsingColumnIndex = STARTING_INDEX;

    /**
     * Package visible Constructor
     *
     * @param catalogName Represents database name. If null, points to the current database
     * @param schemaName Schema/user name. If null, points to the current schema
     * @param parser Oracle Antlr parser
     * @param listeners registered listeners
     */
    AlterTableParserListener(final String catalogName, final String schemaName, final OracleDdlParser parser,
                             final List<ParseTreeListener> listeners) {
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.parser = parser;
        this.listeners = listeners;
    }

    @Override
    public void enterAlter_table(PlSqlParser.Alter_tableContext ctx) {
        TableId tableId = new TableId(catalogName, schemaName, getTableName(ctx.tableview_name()));
        // todo filter tables not in table.include.list
        tableEditor = parser.databaseTables().editTable(tableId);
        if (tableEditor == null) {
            throw new ParsingException(null, "Trying to alter table " + tableId.toString()
                    + ", which does not exist. Query: " + getText(ctx));
        }
        super.enterAlter_table(ctx);
    }

    @Override
    public void exitAlter_table(PlSqlParser.Alter_tableContext ctx) {
        parser.runIfNotNull(() -> {
            listeners.remove(columnDefinitionParserListener);
            parser.databaseTables().overwriteTable(tableEditor.create());
            // parser.signalAlterTable(tableEditor.tableId(), null, ctx.getParent());// todo?
        }, tableEditor);
        super.exitAlter_table(ctx);
    }

    @Override
    public void enterAdd_column_clause(PlSqlParser.Add_column_clauseContext ctx) {
        parser.runIfNotNull(() -> {
            List<PlSqlParser.Column_definitionContext> columns = ctx.column_definition();
            columnEditors = new ArrayList<>(columns.size());
            for (PlSqlParser.Column_definitionContext column : columns) {
                String columnName = getColumnName(column.column_name());
                ColumnEditor editor = Column.editor().name(columnName);
                columnEditors.add(editor);
            }
            columnDefinitionParserListener = new ColumnDefinitionParserListener(tableEditor, columnEditors.get(0), parser.dataTypeResolver());
            listeners.add(columnDefinitionParserListener);
        }, tableEditor);
        super.enterAdd_column_clause(ctx);
    }

    @Override
    public void exitAdd_column_clause(PlSqlParser.Add_column_clauseContext ctx) {
        parser.runIfNotNull(() -> {
            columnEditors.forEach(columnEditor -> tableEditor.addColumn(columnEditor.create()));
            listeners.remove(columnDefinitionParserListener);
            columnDefinitionParserListener = null;
        }, tableEditor, columnEditors);
        super.exitAdd_column_clause(ctx);
    }

    @Override
    public void exitColumn_definition(PlSqlParser.Column_definitionContext ctx) {
        parser.runIfNotNull(() -> {
            if (columnEditors != null) {
                // column editor list is not null when a multiple columns are parsed in one statement
                if (columnEditors.size() > parsingColumnIndex) {
                    // assign next column editor to parse another column definition
                    columnDefinitionParserListener.setColumnEditor(columnEditors.get(parsingColumnIndex++));
                }
                else {
                    // all columns parsed
                    // reset global variables for next parsed statement
                    columnEditors.forEach(columnEditor -> tableEditor.addColumn(columnEditor.create()));
                    columnEditors = null;
                    parsingColumnIndex = STARTING_INDEX;
                }
            }
        }, tableEditor, columnEditors);
        super.exitColumn_definition(ctx);
    }

    @Override
    public void enterDrop_column_clause(PlSqlParser.Drop_column_clauseContext ctx) {
        parser.runIfNotNull(() -> {
            List<PlSqlParser.Column_nameContext> columnNameContexts = ctx.column_name();
            columnEditors = new ArrayList<>(columnNameContexts.size());
            for (PlSqlParser.Column_nameContext columnNameContext : columnNameContexts) {
                String columnName = getColumnName(columnNameContext);
                tableEditor.removeColumn(columnName);
            }
        }, tableEditor);
        super.enterDrop_column_clause(ctx);
    }
}
