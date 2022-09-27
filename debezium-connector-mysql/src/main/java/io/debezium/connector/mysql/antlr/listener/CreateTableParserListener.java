/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.mysql.antlr.listener;

import java.util.List;
import java.util.stream.Collectors;

import org.antlr.v4.runtime.tree.ParseTreeListener;

import io.debezium.connector.mysql.antlr.MySqlAntlrDdlParser;
import io.debezium.ddl.parser.mysql.generated.MySqlParser;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.PeriodDateType;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;

/**
 * Parser listener that is parsing MySQL CREATE TABLE statements.
 *
 * @author Roman Kuchár <kucharrom@gmail.com>.
 */
public class CreateTableParserListener extends TableCommonParserListener {

    public CreateTableParserListener(MySqlAntlrDdlParser parser, List<ParseTreeListener> listeners) {
        super(parser, listeners);
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

            markSystemVersionedColumns();

            parser.databaseTables().overwriteTable(tableEditor.create());
            parser.signalCreateTable(tableEditor.tableId(), ctx);
        }, tableEditor);
        super.exitColumnCreateTable(ctx);
    }

    /**
     * If a system-versioned table has explicit period columns for row start/end dates then set them in the table schema
     */
    private void markSystemVersionedColumns() {
        final List<Column> periodColumns = tableEditor.columns().stream()
                .filter(column -> column.periodDateType().isPresent())
                .sorted((c, c2) -> {
                    PeriodDateType dateType = c.periodDateType().get();
                    PeriodDateType dateType2 = c2.periodDateType().get();
                    return dateType.compareTo(dateType2);
                })
                .collect(Collectors.toList());

        if (periodColumns.size() == 2) {
            String rowStart = periodColumns.get(0).name();
            String rowEnd = periodColumns.get(1).name();
            tableEditor.setSystemVersionColumns(rowStart, rowEnd);
        }
    }

    @Override
    public void exitCopyCreateTable(MySqlParser.CopyCreateTableContext ctx) {
        TableId tableId = parser.parseQualifiedTableId(ctx.tableName(0).fullId());
        TableId originalTableId = parser.parseQualifiedTableId(ctx.tableName(1).fullId());
        Table original = parser.databaseTables().forTable(originalTableId);
        if (original != null) {
            parser.databaseTables().overwriteTable(tableId, original.columns(), original.primaryKeyColumnNames(), original.defaultCharsetName(), original.attributes());
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
    public void exitPeriodDefinition(MySqlParser.PeriodDefinitionContext ctx) {
        tableEditor.setSystemVersionColumns(ctx.uid(0).getText(), ctx.uid(1).getText());
        super.exitPeriodDefinition(ctx);
    }

    @Override
    public void enterTableOptionSystemVersioning(MySqlParser.TableOptionSystemVersioningContext ctx) {
        super.enterTableOptionSystemVersioning(ctx);
    }

    @Override
    public void exitTableOptionSystemVersioning(MySqlParser.TableOptionSystemVersioningContext ctx) {
        if (ctx.SYSTEM_VERSIONING().getText().equals("SYSTEM VERSIONING")) {
            if (!tableEditor.isSystemVersioned()) {
                tableEditor.setSystemVersionColumns("row_start", "row_end");
            }
        }
        super.exitTableOptionSystemVersioning(ctx);
    }

    @Override
    public void enterSystemVersioningDefinitions(MySqlParser.SystemVersioningDefinitionsContext ctx) {
        super.enterSystemVersioningDefinitions(ctx);
    }

    @Override
    public void exitSystemVersioningDefinitions(MySqlParser.SystemVersioningDefinitionsContext ctx) {
        super.exitSystemVersioningDefinitions(ctx);
    }

    @Override
    public void enterAlterBySysVersioningPeriod(MySqlParser.AlterBySysVersioningPeriodContext ctx) {
        super.enterAlterBySysVersioningPeriod(ctx);
    }

    @Override
    public void exitAlterBySysVersioningPeriod(MySqlParser.AlterBySysVersioningPeriodContext ctx) {
        super.exitAlterBySysVersioningPeriod(ctx);
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
