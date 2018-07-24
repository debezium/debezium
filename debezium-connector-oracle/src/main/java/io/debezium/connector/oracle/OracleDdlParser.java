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

import io.debezium.ddl.parser.oracle.generated.PlSqlLexer;
import io.debezium.ddl.parser.oracle.generated.PlSqlParser;
import io.debezium.ddl.parser.oracle.generated.PlSqlParser.Column_definitionContext;
import io.debezium.ddl.parser.oracle.generated.PlSqlParser.Column_nameContext;
import io.debezium.ddl.parser.oracle.generated.PlSqlParser.Create_tableContext;
import io.debezium.ddl.parser.oracle.generated.PlSqlParser.ExpressionContext;
import io.debezium.ddl.parser.oracle.generated.PlSqlParser.Out_of_line_constraintContext;
import io.debezium.ddl.parser.oracle.generated.PlSqlParser.Precision_partContext;
import io.debezium.ddl.parser.oracle.generated.PlSqlParser.Tableview_nameContext;
import io.debezium.ddl.parser.oracle.generated.PlSqlParser.Unit_statementContext;
import io.debezium.ddl.parser.oracle.generated.PlSqlParserBaseListener;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.SystemVariables;
import io.debezium.relational.Table;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.ddl.DdlChanges;
import io.debezium.relational.ddl.DdlParser;
import oracle.jdbc.OracleTypes;

public class OracleDdlParser implements DdlParser {

    private String catalogName;
    private String schemaName;

    @Override
    public void setCurrentDatabase(String databaseName) {
        this.catalogName = databaseName;
    }

    @Override
    public void setCurrentSchema(String schemaName) {
        this.schemaName = schemaName;
    }

    @Override
    public DdlChanges getDdlChanges() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public String terminator() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public SystemVariables systemVariables() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public void parse(String ddlContent, Tables databaseTables) {
        if (!ddlContent.endsWith(";")) {
            ddlContent = ddlContent + ";";
        }

        try {
            PlSqlLexer lexer = new PlSqlLexer(new ANTLRInputStream(toUpperCase(ddlContent)));
            CommonTokenStream tokens = new CommonTokenStream(lexer);
            PlSqlParser parser = new PlSqlParser(tokens);

            Unit_statementContext ast = parser.unit_statement();
            CreateTableListener createTablelistener = new CreateTableListener();
            ParseTreeWalker.DEFAULT.walk(createTablelistener, ast);

            if (createTablelistener.getTable() != null) {
                databaseTables.overwriteTable(createTablelistener.getTable());
            }
        }
        catch(Exception e) {
            throw new IllegalArgumentException("Couldn't parse DDL statement " + ddlContent, e);
        }
    }

    // TODO excluded quoted identifiers
    private String toUpperCase(String ddl) {
        return ddl.toUpperCase(Locale.ENGLISH);
    }

    private class CreateTableListener extends PlSqlParserBaseListener {

        private TableEditor editor;

        public Table getTable() {
            return editor != null ? editor.create() : null;
        }

        @Override
        public void enterCreate_table(Create_tableContext ctx) {
            if (ctx.relational_table() == null) {
                throw new IllegalArgumentException("Only relational tables are supported");
            }

            editor = Table.editor();
            editor.tableId(new TableId(catalogName, schemaName, getTableName(ctx.tableview_name())));

            super.enterCreate_table(ctx);
        }

        private String getTableName(Tableview_nameContext tableview_name) {
            if (tableview_name.id_expression() != null) {
                return tableview_name.id_expression().getText();
            }
            else {
                return tableview_name.identifier().id_expression().getText();
            }
        }

        @Override
        public void exitColumn_definition(Column_definitionContext ctx) {
            Precision_partContext precisionPart = ctx.datatype().precision_part();

            ColumnEditor columnEditor = Column.editor();
            columnEditor.name(getColumnName(ctx.column_name()));

            if (ctx.datatype().native_datatype_element() != null) {
                if (ctx.datatype().native_datatype_element().INT() != null
                        || ctx.datatype().native_datatype_element().INTEGER() != null
                        || ctx.datatype().native_datatype_element().SMALLINT() != null
                        || ctx.datatype().native_datatype_element().NUMERIC() != null
                        || ctx.datatype().native_datatype_element().DECIMAL() != null) {
                    // NUMERIC and DECIMAL types have by default zero scale
                    columnEditor
                        .jdbcType(Types.NUMERIC)
                        .type("NUMBER");

                    if (precisionPart == null) {
                        columnEditor.length(38)
                            .scale(0);
                    }
                    else {
                        setPrecision(precisionPart, columnEditor);
                        setScale(precisionPart, columnEditor);
                    }
                }
                else if (ctx.datatype().native_datatype_element().DATE() != null) {
                    // JDBC driver reports type as timestamp but name DATE
                    columnEditor
                        .jdbcType(Types.TIMESTAMP)
                        .type("DATE");
                }
                else if (ctx.datatype().native_datatype_element().TIMESTAMP() != null) {
                    if (ctx.datatype().WITH() != null
                            && ctx.datatype().TIME() != null
                            && ctx.datatype().ZONE() != null) {
                        if (ctx.datatype().LOCAL() != null) {
                            columnEditor
                                .jdbcType(OracleTypes.TIMESTAMPLTZ)
                                .type("TIMESTAMP WITH LOCAL TIME ZONE");
                        }
                        else {
                            columnEditor
                                .jdbcType(OracleTypes.TIMESTAMPTZ)
                                .type("TIMESTAMP WITH TIME ZONE");
                        }
                    }
                    else {
                        columnEditor
                            .jdbcType(Types.TIMESTAMP)
                            .type("TIMESTAMP");
                    }
                    columnEditor.length(6);
                }
                else if (ctx.datatype().native_datatype_element().VARCHAR2() != null) {
                    columnEditor
                        .jdbcType(Types.VARCHAR)
                        .type("VARCHAR2");

                    if (precisionPart == null) {
                        columnEditor.length(getVarCharDefaultLength());
                    }
                    else {
                        setPrecision(precisionPart, columnEditor);
                    }
                }
                else if (ctx.datatype().native_datatype_element().NVARCHAR2() != null) {
                    columnEditor
                        .jdbcType(Types.NVARCHAR)
                        .type("NVARCHAR2");

                    if (precisionPart == null) {
                        columnEditor.length(getVarCharDefaultLength());
                    }
                    else {
                        setPrecision(precisionPart, columnEditor);
                    }
                }
                else if (ctx.datatype().native_datatype_element().CHAR() != null) {
                    columnEditor
                        .jdbcType(Types.CHAR)
                        .type("CHAR")
                        .length(1);
                }
                else if (ctx.datatype().native_datatype_element().NCHAR() != null) {
                    columnEditor
                        .jdbcType(Types.NCHAR)
                        .type("NCHAR")
                        .length(1);
                }
                else if (ctx.datatype().native_datatype_element().BINARY_FLOAT() != null) {
                    columnEditor
                        .jdbcType(OracleTypes.BINARY_FLOAT)
                        .type("BINARY_FLOAT");
                }
                else if (ctx.datatype().native_datatype_element().BINARY_DOUBLE() != null) {
                    columnEditor
                        .jdbcType(OracleTypes.BINARY_DOUBLE)
                        .type("BINARY_DOUBLE");
                }
                // PRECISION keyword is mandatory
                else if (ctx.datatype().native_datatype_element().FLOAT() != null ||
                        (ctx.datatype().native_datatype_element().DOUBLE() != null && ctx.datatype().native_datatype_element().PRECISION() != null)) {
                    columnEditor
                        .jdbcType(Types.FLOAT)
                        .type("FLOAT")
                        .length(126);

                    // TODO float's precision is about bits not decimal digits; should be ok for now to over-size
                    if (precisionPart != null) {
                        setPrecision(precisionPart, columnEditor);
                    }
                }
                else if (ctx.datatype().native_datatype_element().REAL() != null) {
                    columnEditor
                        .jdbcType(Types.FLOAT)
                        .type("FLOAT")
                        // TODO float's precision is about bits not decimal digits; should be ok for now to over-size
                        .length(63);
                }
                else if (ctx.datatype().native_datatype_element().NUMBER() != null) {
                    columnEditor
                        .jdbcType(Types.NUMERIC)
                        .type("NUMBER");

                    if (precisionPart == null) {
                        columnEditor.length(38);
                    }
                    else {
                        setPrecision(precisionPart, columnEditor);
                        setScale(precisionPart, columnEditor);
                    }
                }
                else {
                    throw new IllegalArgumentException("Unsupported column type: " + ctx.datatype().native_datatype_element().getText());
                }
            }
            else if (ctx.datatype().INTERVAL() != null
                    && ctx.datatype().YEAR() != null
                    && ctx.datatype().TO() != null
                    && ctx.datatype().MONTH() != null) {
                columnEditor
                    .jdbcType(OracleTypes.INTERVALYM)
                    .type("INTERVAL YEAR TO MONTH")
                    .length(2);
                if (!ctx.datatype().expression().isEmpty()) {
                    columnEditor.length(Integer.valueOf((ctx.datatype().expression(0).getText())));
                }
            }
            else if (ctx.datatype().INTERVAL() != null
                    && ctx.datatype().DAY() != null
                    && ctx.datatype().TO() != null
                    && ctx.datatype().SECOND() != null) {
                columnEditor
                    .jdbcType(OracleTypes.INTERVALDS)
                    .type("INTERVAL DAY TO SECOND")
                    .length(2)
                    .scale(6);
                for (final ExpressionContext e: ctx.datatype().expression()) {
                    if (e.getSourceInterval().startsAfter(ctx.datatype().TO().getSourceInterval())) {
                        columnEditor.scale(Integer.valueOf(e.getText()));
                    }
                    else {
                        columnEditor.length(Integer.valueOf(e.getText()));
                    }
                }
                if (!ctx.datatype().expression().isEmpty()) {
                    columnEditor.length(Integer.valueOf((ctx.datatype().expression(0).getText())));
                }
            }
            else {
                throw new IllegalArgumentException("Unsupported column type: " + ctx.datatype().getText());
            }

            boolean hasNotNullConstraint = ctx.inline_constraint().stream()
                .filter(c -> c.NOT() != null)
                .findFirst()
                .isPresent();

            columnEditor.optional(!hasNotNullConstraint);

            editor.addColumn(columnEditor.create());

            super.exitColumn_definition(ctx);
        }

        private int getVarCharDefaultLength() {
            // TODO replace with value from select name, value  from v$parameter where name='max_string_size';
            return 4000;
        }

        private void setPrecision(Precision_partContext precisionPart, ColumnEditor columnEditor) {
            columnEditor.length(Integer.valueOf(precisionPart.numeric(0).getText()));
        }

        private void setScale(Precision_partContext precisionPart, ColumnEditor columnEditor) {
            if (precisionPart.numeric().size() > 1) {
                columnEditor.scale(Integer.valueOf(precisionPart.numeric(1).getText()));
            }
            else if (precisionPart.numeric_negative() != null) {
                columnEditor.scale(Integer.valueOf(precisionPart.numeric_negative().getText()));
            }
            else {
                columnEditor.scale(0);
            }
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
