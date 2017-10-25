/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import java.sql.Types;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

import io.debezium.connector.oracle.parser.PlSqlLexer;
import io.debezium.connector.oracle.parser.PlSqlParser;
import io.debezium.connector.oracle.parser.PlSqlParser.Column_definitionContext;
import io.debezium.connector.oracle.parser.PlSqlParser.Column_nameContext;
import io.debezium.connector.oracle.parser.PlSqlParser.Create_tableContext;
import io.debezium.connector.oracle.parser.PlSqlParser.Out_of_line_constraintContext;
import io.debezium.connector.oracle.parser.PlSqlParser.Precision_partContext;
import io.debezium.connector.oracle.parser.PlSqlParserBaseListener;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.Table;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;

public class OracleDdlParser {

    public Table parseCreateTable(TableId tableId, String createTableDdl) {
        if (!createTableDdl.endsWith(";")) {
            createTableDdl = createTableDdl + ";";
        }

        PlSqlLexer lexer = new PlSqlLexer(new ANTLRInputStream(toUpperCase(createTableDdl)));
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        PlSqlParser parser = new PlSqlParser(tokens);

        Create_tableContext ast = parser.create_table();
        CreateTableListener listener = new CreateTableListener(tableId);
        ParseTreeWalker.DEFAULT.walk(listener, ast);

        return listener.getTable();
    }

    // TODO excluded quoted identifiers
    private String toUpperCase(String ddl) {
        return ddl.toUpperCase(Locale.ENGLISH);
    }

    private static class CreateTableListener extends PlSqlParserBaseListener {

        private final TableEditor editor;

        public CreateTableListener(TableId tableId) {
            editor = Table.editor();
            editor.tableId(tableId);
        }

        public Table getTable() {
            return editor.create();
        }

        @Override
        public void exitColumn_definition(Column_definitionContext ctx) {
            ColumnEditor columnEditor = Column.editor();
            columnEditor.name(getColumnName(ctx.column_name()));

            if (ctx.datatype().native_datatype_element().INT() != null || ctx.datatype().native_datatype_element().INTEGER() != null) {
                columnEditor.jdbcType(Types.NUMERIC);
                columnEditor.type("NUMBER");
                columnEditor.length(38);
                columnEditor.scale(0);
            }
            else if (ctx.datatype().native_datatype_element().DATE() != null) {
                columnEditor.jdbcType(Types.DATE);
                columnEditor.type("DATE");
            }
            else if (ctx.datatype().native_datatype_element().TIMESTAMP() != null) {
                columnEditor.jdbcType(Types.TIMESTAMP);
                columnEditor.type("TIMESTAMP");
                columnEditor.length(6);
                columnEditor.scale(0);
            }
            else if (ctx.datatype().native_datatype_element().VARCHAR2() != null) {
                columnEditor.jdbcType(Types.VARCHAR);
                columnEditor.type("VARCHAR2");
            }
            else if (ctx.datatype().native_datatype_element().DECIMAL() != null) {
                columnEditor.jdbcType(Types.DECIMAL);
                columnEditor.type("DECIMAL");
            }
            else {
                throw new IllegalArgumentException("Unsupported column type: " + ctx.datatype().native_datatype_element().getText());
            }

            Precision_partContext precisionPart = ctx.datatype().precision_part();
            if (precisionPart != null) {
                columnEditor.length(Integer.valueOf(precisionPart.numeric(0).getText()));

                if (precisionPart.numeric().size() > 1) {
                    columnEditor.scale(Integer.valueOf(precisionPart.numeric(1).getText()));
                }
            }

            boolean hasNotNullConstraint = ctx.inline_constraint().stream()
                .filter(c -> c.NOT() != null)
                .findFirst()
                .isPresent();

            columnEditor.optional(!hasNotNullConstraint);

            editor.addColumn(columnEditor.create());

            super.exitColumn_definition(ctx);
        }

        @Override
        public void exitOut_of_line_constraint(Out_of_line_constraintContext ctx) {
            if(ctx.PRIMARY() != null) {
                List<String> pkColumnNames = ctx.column_name().stream()
                    .map(this::getColumnName)
                    .collect(Collectors.toList());

                editor.setPrimaryKeyNames(pkColumnNames);
            }

            super.exitOut_of_line_constraint(ctx);
        }

        private String getColumnName(Column_nameContext ctx) {
            return ctx.identifier().id_expression().getText();
        }
    }
}
