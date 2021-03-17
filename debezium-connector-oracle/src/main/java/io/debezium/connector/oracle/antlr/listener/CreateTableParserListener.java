/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.antlr.listener;

import static io.debezium.connector.oracle.antlr.listener.ParserUtils.getColumnName;
import static io.debezium.connector.oracle.antlr.listener.ParserUtils.getTableName;

import java.util.List;
import java.util.stream.Collectors;

import org.antlr.v4.runtime.tree.ParseTreeListener;

import io.debezium.connector.oracle.antlr.OracleDdlParser;
import io.debezium.ddl.parser.oracle.generated.PlSqlParser;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.Table;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;

public class CreateTableParserListener extends BaseParserListener {

    private final List<ParseTreeListener> listeners;
    private TableEditor tableEditor;
    private String catalogName;
    private String schemaName;
    private OracleDdlParser parser;
    private ColumnDefinitionParserListener columnDefinitionParserListener;

    CreateTableParserListener(final String catalogName, final String schemaName, final OracleDdlParser parser,
                              final List<ParseTreeListener> listeners) {
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.parser = parser;
        this.listeners = listeners;
    }

    @Override
    public void enterCreate_table(PlSqlParser.Create_tableContext ctx) {
        if (ctx.relational_table() == null) {
            throw new IllegalArgumentException("Only relational tables are supported");
        }
        TableId tableId = new TableId(catalogName, schemaName, getTableName(ctx.tableview_name()));
        tableEditor = parser.databaseTables().editOrCreateTable(tableId);
        super.enterCreate_table(ctx);
    }

    @Override
    public void exitCreate_table(PlSqlParser.Create_tableContext ctx) {
        Table table = getTable();
        assert table != null;

        parser.runIfNotNull(() -> {
            listeners.remove(columnDefinitionParserListener);
            columnDefinitionParserListener = null;
            parser.databaseTables().overwriteTable(table);
            // parser.signalCreateTable(tableEditor.tableId(), ctx); todo ?
        }, tableEditor, table);

        super.exitCreate_table(ctx);
    }

    @Override
    public void enterColumn_definition(PlSqlParser.Column_definitionContext ctx) {
        parser.runIfNotNull(() -> {
            String columnName = ParserUtils.stripeQuotes(getColumnName(ctx.column_name()));
            ColumnEditor columnEditor = Column.editor().name(columnName);
            if (columnDefinitionParserListener == null) {
                columnDefinitionParserListener = new ColumnDefinitionParserListener(tableEditor, columnEditor, parser.dataTypeResolver());
                // todo: this explicit call is for the first column, should it be fixed?
                columnDefinitionParserListener.enterColumn_definition(ctx);
                listeners.add(columnDefinitionParserListener);
            }
            else {
                columnDefinitionParserListener.setColumnEditor(columnEditor);
            }
        }, tableEditor);
        super.enterColumn_definition(ctx);
    }

    @Override
    public void exitColumn_definition(PlSqlParser.Column_definitionContext ctx) {
        parser.runIfNotNull(() -> tableEditor.addColumn(columnDefinitionParserListener.getColumn()),
                tableEditor, columnDefinitionParserListener);
        super.exitColumn_definition(ctx);
    }

    @Override
    public void exitOut_of_line_constraint(PlSqlParser.Out_of_line_constraintContext ctx) {
        if (ctx.PRIMARY() != null) {
            List<String> pkColumnNames = ctx.column_name().stream()
                    .map(ParserUtils::getColumnName)
                    .collect(Collectors.toList());

            tableEditor.setPrimaryKeyNames(pkColumnNames);
        }
        super.exitOut_of_line_constraint(ctx);
    }

    private Table getTable() {
        return tableEditor != null ? tableEditor.create() : null;
    }
}
