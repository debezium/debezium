/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.antlr.listener;

import java.sql.Types;

import io.debezium.antlr.DataTypeResolver;
import io.debezium.ddl.parser.oracle.generated.PlSqlParser;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.TableEditor;

import oracle.jdbc.OracleTypes;

/**
 * This class parses column definitions of Oracle statements.
 */
public class ColumnDefinitionParserListener extends BaseParserListener {

    private final DataTypeResolver dataTypeResolver;
    private final TableEditor tableEditor;
    private ColumnEditor columnEditor;

    ColumnDefinitionParserListener(final TableEditor tableEditor, final ColumnEditor columnEditor,
                                   final DataTypeResolver dataTypeResolver) {
        this.dataTypeResolver = dataTypeResolver;
        this.tableEditor = tableEditor;
        this.columnEditor = columnEditor;
    }

    void setColumnEditor(ColumnEditor columnEditor) {
        this.columnEditor = columnEditor;
    }

    Column getColumn() {
        return columnEditor.create();
    }

    @Override
    public void enterColumn_definition(PlSqlParser.Column_definitionContext ctx) {
        resolveColumnDataType(ctx);
        super.enterColumn_definition(ctx);
    }

    @Override
    public void enterPrimary_key_clause(PlSqlParser.Primary_key_clauseContext ctx) {
        // this rule will be parsed only if no primary key is set in a table
        // otherwise the statement can't be executed due to multiple primary key error
        columnEditor.optional(false);
        tableEditor.addColumn(columnEditor.create());
        tableEditor.setPrimaryKeyNames(columnEditor.name());
        super.enterPrimary_key_clause(ctx);
    }

    // todo use dataTypeResolver instead
    private void resolveColumnDataType(PlSqlParser.Column_definitionContext ctx) {
        columnEditor.name(getColumnName(ctx.column_name()));

        PlSqlParser.Precision_partContext precisionPart = null;
        if (ctx.datatype() != null) {
            precisionPart = ctx.datatype().precision_part();
        }

        if (ctx.datatype() == null) {
            if (ctx.type_name() != null && "\"MDSYS\".\"SDO_GEOMETRY\"".equalsIgnoreCase(ctx.type_name().getText())) {
                columnEditor.jdbcType(Types.STRUCT).type("MDSYS.SDO_GEOMETRY");
            }
        }
        else if (ctx.datatype().native_datatype_element() != null) {
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

                if (precisionPart == null) {
                    columnEditor.length(6);
                }
                else {
                    setPrecision(precisionPart, columnEditor);
                }
            }
            // VARCHAR is the same as VARCHAR2 in Oracle
            else if (ctx.datatype().native_datatype_element().VARCHAR2() != null ||
                    ctx.datatype().native_datatype_element().VARCHAR() != null) {
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
            else if (ctx.datatype().native_datatype_element().BLOB() != null) {
                columnEditor
                        .jdbcType(Types.BLOB)
                        .type("BLOB");
            }
            else if (ctx.datatype().native_datatype_element().CLOB() != null) {
                columnEditor
                        .jdbcType(Types.CLOB)
                        .type("CLOB");
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
            for (final PlSqlParser.ExpressionContext e : ctx.datatype().expression()) {
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

        boolean hasNotNullConstraint = ctx.inline_constraint().stream().anyMatch(c -> c.NOT() != null);

        // todo move to enterExpression and apply type conversion
        if (ctx.DEFAULT() != null) {
            String defaultValue = ctx.expression().getText();
            columnEditor.defaultValue(defaultValue);
        }
        // todo move to nonNull
        columnEditor.optional(!hasNotNullConstraint);
    }

    private int getVarCharDefaultLength() {
        // TODO replace with value from select name, value from v$parameter where name='max_string_size';
        return 4000;
    }

    private void setPrecision(PlSqlParser.Precision_partContext precisionPart, ColumnEditor columnEditor) {
        columnEditor.length(Integer.valueOf(precisionPart.numeric(0).getText()));
    }

    private void setScale(PlSqlParser.Precision_partContext precisionPart, ColumnEditor columnEditor) {
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
}
