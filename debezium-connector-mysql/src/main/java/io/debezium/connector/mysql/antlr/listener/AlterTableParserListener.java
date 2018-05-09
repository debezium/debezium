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
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;
import io.debezium.text.ParsingException;
import org.antlr.v4.runtime.tree.ParseTreeListener;

import java.util.ArrayList;
import java.util.List;

import static io.debezium.antlr.AntlrDdlParser.getText;

/**
 * Parser listeners that is parsing MySQL ALTER TABLE statements.
 *
 * @author Roman Kuchár <kucharrom@gmail.com>.
 */
public class AlterTableParserListener extends MySqlParserBaseListener {

    private static final int STARTING_INDEX = 1;

    private final MySqlAntlrDdlParser parserCtx;
    private final List<ParseTreeListener> listeners;

    private TableEditor tableEditor;
    private ColumnDefinitionParserListener columnDefinitionListener;

    private List<ColumnEditor> columnEditors;
    private int parsingColumnIndex = STARTING_INDEX;

    public AlterTableParserListener(MySqlAntlrDdlParser parserCtx, List<ParseTreeListener> listeners) {
        this.parserCtx = parserCtx;
        this.listeners = listeners;
    }

    @Override
    public void enterAlterTable(MySqlParser.AlterTableContext ctx) {
        TableId tableId = parserCtx.parseQualifiedTableId(ctx.tableName().fullId());
        tableEditor = parserCtx.databaseTables().editTable(tableId);
        if (tableEditor == null) {
            throw new ParsingException(null, "Trying to alter table " + tableId.toString()
                    + ", which does not exists. Query: " + getText(ctx));
        }
        super.enterAlterTable(ctx);
    }

    @Override
    public void exitAlterTable(MySqlParser.AlterTableContext ctx) {
        parserCtx.runIfNotNull(() -> {
            listeners.remove(columnDefinitionListener);
            parserCtx.databaseTables().overwriteTable(tableEditor.create());
            parserCtx.signalAlterTable(tableEditor.tableId(), null, ctx.getParent());
        }, tableEditor);
        super.exitAlterTable(ctx);
    }

    @Override
    public void enterAlterByAddColumn(MySqlParser.AlterByAddColumnContext ctx) {
        parserCtx.runIfNotNull(() -> {
            String columnName = parserCtx.parseName(ctx.uid(0));
            ColumnEditor columnEditor = Column.editor().name(columnName);
            columnDefinitionListener = new ColumnDefinitionParserListener(tableEditor, columnEditor, parserCtx.dataTypeResolver());
            listeners.add(columnDefinitionListener);
        }, tableEditor);
        super.exitAlterByAddColumn(ctx);
    }

    @Override
    public void exitAlterByAddColumn(MySqlParser.AlterByAddColumnContext ctx) {
        parserCtx.runIfNotNull(() -> {
            Column column = columnDefinitionListener.getColumn();
            tableEditor.addColumn(column);

            String columnName = column.name();
            if (ctx.FIRST() != null) {
                tableEditor.reorderColumn(columnName, null);
            }
            else if (ctx.AFTER() != null) {
                String afterColumn = parserCtx.parseName(ctx.uid(1));
                tableEditor.reorderColumn(columnName, afterColumn);
            }
        }, tableEditor, columnDefinitionListener);
        super.exitAlterByAddColumn(ctx);
    }

    @Override
    public void enterAlterByAddColumns(MySqlParser.AlterByAddColumnsContext ctx) {
        // multiple columns are added. Initialize a list of column editors for them
        parserCtx.runIfNotNull(() -> {
            columnEditors = new ArrayList<>(ctx.uid().size());
            for (MySqlParser.UidContext uidContext : ctx.uid()) {
                String columnName = parserCtx.parseName(uidContext);
                columnEditors.add(Column.editor().name(columnName));
            }
            columnDefinitionListener = new ColumnDefinitionParserListener(tableEditor, columnEditors.get(0), parserCtx.dataTypeResolver());
            listeners.add(columnDefinitionListener);
        }, tableEditor);
        super.enterAlterByAddColumns(ctx);
    }

    @Override
    public void exitColumnDefinition(MySqlParser.ColumnDefinitionContext ctx) {
        parserCtx.runIfNotNull(() -> {
            if (columnEditors != null) {
                // column editor list is not null when a multiple columns are parsed in one statement
                if (columnEditors.size() > parsingColumnIndex) {
                    // assign next column editor to parse another column definition
                    columnDefinitionListener.setColumnEditor(columnEditors.get(parsingColumnIndex++));
                }
                else {
                    // all columns parsed
                    // reset global variables for next parsed statement
                    columnEditors = null;
                    parsingColumnIndex = STARTING_INDEX;
                }
            }
        }, tableEditor, columnEditors);
        super.exitColumnDefinition(ctx);
    }

    @Override
    public void exitAlterByAddColumns(MySqlParser.AlterByAddColumnsContext ctx) {
        parserCtx.runIfNotNull(() -> {
            columnEditors.forEach(columnEditor -> tableEditor.addColumn(columnEditor.create()));
        }, tableEditor, columnEditors);
        super.exitAlterByAddColumns(ctx);
    }

    @Override
    public void enterAlterByChangeColumn(MySqlParser.AlterByChangeColumnContext ctx) {
        parserCtx.runIfNotNull(() -> {
            String oldColumnName = parserCtx.parseName(ctx.oldColumn);
            Column existingColumn = tableEditor.columnWithName(oldColumnName);
            if (existingColumn != null) {
                columnDefinitionListener = new ColumnDefinitionParserListener(tableEditor, existingColumn.edit(), parserCtx.dataTypeResolver());
                listeners.add(columnDefinitionListener);
            }
            else {
                throw new ParsingException(null, "Trying to change column " + oldColumnName + " in "
                        + tableEditor.tableId().toString() + " table, which does not exists. Query: " + getText(ctx));
            }
        }, tableEditor);
        super.enterAlterByChangeColumn(ctx);
    }

    @Override
    public void exitAlterByChangeColumn(MySqlParser.AlterByChangeColumnContext ctx) {
        parserCtx.runIfNotNull(() -> {
            Column column = columnDefinitionListener.getColumn();
            tableEditor.addColumn(column);
            String newColumnName = parserCtx.parseName(ctx.newColumn);
            tableEditor.renameColumn(column.name(), newColumnName);

            if (ctx.FIRST() != null) {
                tableEditor.reorderColumn(newColumnName, null);
            }
            else if (ctx.afterColumn != null) {
                tableEditor.reorderColumn(newColumnName, parserCtx.parseName(ctx.afterColumn));
            }
        }, tableEditor, columnDefinitionListener);
        super.exitAlterByChangeColumn(ctx);
    }

    @Override
    public void enterAlterByModifyColumn(MySqlParser.AlterByModifyColumnContext ctx) {
        parserCtx.runIfNotNull(() -> {
            String columnName = parserCtx.parseName(ctx.uid(0));
            Column column = tableEditor.columnWithName(columnName);
            if (column != null) {
                columnDefinitionListener = new ColumnDefinitionParserListener(tableEditor, column.edit(), parserCtx.dataTypeResolver());
                listeners.add(columnDefinitionListener);
            }
            else {
                throw new ParsingException(null, "Trying to change column " + columnName + " in "
                        + tableEditor.tableId().toString() + " table, which does not exists. Query: " + getText(ctx));
            }
        }, tableEditor);
        super.enterAlterByModifyColumn(ctx);
    }

    @Override
    public void exitAlterByModifyColumn(MySqlParser.AlterByModifyColumnContext ctx) {
        parserCtx.runIfNotNull(() -> {
            Column column = columnDefinitionListener.getColumn();
            tableEditor.addColumn(column);

            if (ctx.FIRST() != null) {
                tableEditor.reorderColumn(column.name(), null);
            }
            else if (ctx.AFTER() != null) {
                String afterColumn = parserCtx.parseName(ctx.uid(1));
                tableEditor.reorderColumn(column.name(), afterColumn);
            }
        }, tableEditor, columnDefinitionListener);
        super.exitAlterByModifyColumn(ctx);
    }

    @Override
    public void enterAlterByDropColumn(MySqlParser.AlterByDropColumnContext ctx) {
        parserCtx.runIfNotNull(() -> {
            tableEditor.removeColumn(parserCtx.parseName(ctx.uid()));
        }, tableEditor);
        super.enterAlterByDropColumn(ctx);
    }

    @Override
    public void enterAlterByRename(MySqlParser.AlterByRenameContext ctx) {
        parserCtx.runIfNotNull(() -> {
            TableId newTableId = parserCtx.resolveTableId(parserCtx.currentSchema(), parserCtx.parseName(ctx.uid()));
            parserCtx.databaseTables().renameTable(tableEditor.tableId(), newTableId);
            // databaseTables are updated clear table editor so exitAlterTable will not update a table by table editor
            tableEditor = null;
        }, tableEditor);
        super.enterAlterByRename(ctx);
    }

    @Override
    public void enterAlterByChangeDefault(MySqlParser.AlterByChangeDefaultContext ctx) {
        parserCtx.runIfNotNull(() -> {
            String columnName = parserCtx.parseName(ctx.uid());
            Column column = tableEditor.columnWithName(columnName);
            if (column != null) {
                ColumnEditor columnEditor = column.edit();
                columnEditor.generated(ctx.DROP() != null);
            }
        }, tableEditor);
        super.enterAlterByChangeDefault(ctx);
    }

    @Override
    public void enterAlterByAddPrimaryKey(MySqlParser.AlterByAddPrimaryKeyContext ctx) {
        parserCtx.runIfNotNull(() -> {
            parserCtx.parsePrimaryIndexColumnNames(ctx.indexColumnNames(), tableEditor);
        }, tableEditor);
        super.enterAlterByAddPrimaryKey(ctx);
    }

    @Override
    public void enterAlterByDropPrimaryKey(MySqlParser.AlterByDropPrimaryKeyContext ctx) {
        parserCtx.runIfNotNull(() -> {
            tableEditor.setPrimaryKeyNames(new ArrayList<>());
        }, tableEditor);
        super.enterAlterByDropPrimaryKey(ctx);
    }

    @Override
    public void enterAlterByAddUniqueKey(MySqlParser.AlterByAddUniqueKeyContext ctx) {
        parserCtx.runIfNotNull(() -> {
            if (!tableEditor.hasPrimaryKey()) {
                // this may eventually get overwritten by a real PK
                parserCtx.parsePrimaryIndexColumnNames(ctx.indexColumnNames(), tableEditor);
            }
        }, tableEditor);
        super.enterAlterByAddUniqueKey(ctx);
    }
}