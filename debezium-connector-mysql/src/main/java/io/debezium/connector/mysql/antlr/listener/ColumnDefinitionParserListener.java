/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.mysql.antlr.listener;

import static io.debezium.antlr.AntlrDdlParser.getText;

import java.sql.Types;
import java.util.List;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import io.debezium.antlr.DataTypeResolver;
import io.debezium.connector.mysql.MySqlDefaultValuePreConverter;
import io.debezium.connector.mysql.MySqlValueConverters;
import io.debezium.ddl.parser.mysql.generated.MySqlParser;
import io.debezium.ddl.parser.mysql.generated.MySqlParser.DefaultValueContext;
import io.debezium.ddl.parser.mysql.generated.MySqlParserBaseListener;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.TableEditor;
import io.debezium.relational.ValueConverter;
import io.debezium.relational.ddl.DataType;

/**
 * Parser listener that is parsing column definition part of MySQL statements.
 *
 * @author Roman Kuchár <kucharrom@gmail.com>.
 */
public class ColumnDefinitionParserListener extends MySqlParserBaseListener {

    private final DataTypeResolver dataTypeResolver;
    private final TableEditor tableEditor;
    private ColumnEditor columnEditor;

    private final MySqlValueConverters converters;
    private final MySqlDefaultValuePreConverter defaultValuePreConverter = new MySqlDefaultValuePreConverter();

    public ColumnDefinitionParserListener(TableEditor tableEditor, ColumnEditor columnEditor, DataTypeResolver dataTypeResolver, MySqlValueConverters converters) {
        this.tableEditor = tableEditor;
        this.columnEditor = columnEditor;
        this.dataTypeResolver = dataTypeResolver;
        this.converters = converters;
    }

    public void setColumnEditor(ColumnEditor columnEditor) {
        this.columnEditor = columnEditor;
    }

    public ColumnEditor getColumnEditor() {
        return columnEditor;
    }

    public Column getColumn() {
        return columnEditor.create();
    }

    @Override
    public void enterColumnDefinition(MySqlParser.ColumnDefinitionContext ctx) {
        resolveColumnDataType(ctx.dataType());
        super.enterColumnDefinition(ctx);
    }

    @Override
    public void enterUniqueKeyColumnConstraint(MySqlParser.UniqueKeyColumnConstraintContext ctx) {
        if (!tableEditor.hasPrimaryKey()) {
            // take the first unique constrain if no primary key is set
            tableEditor.addColumn(columnEditor.create());
            tableEditor.setPrimaryKeyNames(columnEditor.name());
        }
        super.enterUniqueKeyColumnConstraint(ctx);
    }

    @Override
    public void enterPrimaryKeyColumnConstraint(MySqlParser.PrimaryKeyColumnConstraintContext ctx) {
        // this rule will be parsed only if no primary key is set in a table
        // otherwise the statement can't be executed due to multiple primary key error
        columnEditor.optional(false);
        tableEditor.addColumn(columnEditor.create());
        tableEditor.setPrimaryKeyNames(columnEditor.name());
        super.enterPrimaryKeyColumnConstraint(ctx);
    }

    @Override
    public void enterNullNotnull(MySqlParser.NullNotnullContext ctx) {
        columnEditor.optional(ctx.NOT() == null);
        super.enterNullNotnull(ctx);
    }

    @Override
    public void enterDefaultValue(DefaultValueContext ctx) {
        String sign = "";
        if (ctx.NULL_LITERAL() != null) {
            return;
        }
        if (ctx.unaryOperator() != null) {
            sign = ctx.unaryOperator().getText();
        }
        if (ctx.constant() != null) {
            if (ctx.constant().stringLiteral() != null) {
                columnEditor.defaultValue(sign + unquote(ctx.constant().stringLiteral().getText()));
            }
            else if (ctx.constant().decimalLiteral() != null) {
                columnEditor.defaultValue(sign + ctx.constant().decimalLiteral().getText());
            }
            else if (ctx.constant().BIT_STRING() != null) {
                columnEditor.defaultValue(unquoteBinary(ctx.constant().BIT_STRING().getText()));
            }
            else if (ctx.constant().booleanLiteral() != null) {
                columnEditor.defaultValue(ctx.constant().booleanLiteral().getText());
            }
            else if (ctx.constant().REAL_LITERAL() != null) {
                columnEditor.defaultValue(ctx.constant().REAL_LITERAL().getText());
            }
        }
        else if (ctx.timeDefinition() != null) {
            if (ctx.timeDefinition().CURRENT_TIMESTAMP() != null || ctx.timeDefinition().NOW() != null) {
                columnEditor.defaultValue("1970-01-01 00:00:00");
            }
            else {
                columnEditor.defaultValue(ctx.timeDefinition().getText());
            }
        }
        convertDefaultValueToSchemaType(columnEditor);
        super.enterDefaultValue(ctx);
    }

    @Override
    public void enterAutoIncrementColumnConstraint(MySqlParser.AutoIncrementColumnConstraintContext ctx) {
        columnEditor.autoIncremented(true);
        columnEditor.generated(true);
        super.enterAutoIncrementColumnConstraint(ctx);
    }

    private void resolveColumnDataType(MySqlParser.DataTypeContext dataTypeContext) {
        String charsetName = null;
        DataType dataType = dataTypeResolver.resolveDataType(dataTypeContext);

        if (dataTypeContext instanceof MySqlParser.StringDataTypeContext) {
            MySqlParser.StringDataTypeContext stringDataTypeContext = (MySqlParser.StringDataTypeContext) dataTypeContext;

            if (stringDataTypeContext.lengthOneDimension() != null) {
                Integer length = Integer.valueOf(stringDataTypeContext.lengthOneDimension().decimalLiteral().getText());
                columnEditor.length(length);
            }

            if (stringDataTypeContext.charsetName() != null) {
                charsetName = stringDataTypeContext.charsetName().getText();
            }
        }
        else if (dataTypeContext instanceof MySqlParser.NationalStringDataTypeContext) {
            MySqlParser.NationalStringDataTypeContext nationalStringDataTypeContext = (MySqlParser.NationalStringDataTypeContext) dataTypeContext;

            if (nationalStringDataTypeContext.lengthOneDimension() != null) {
                Integer length = Integer.valueOf(nationalStringDataTypeContext.lengthOneDimension().decimalLiteral().getText());
                columnEditor.length(length);
            }
        }
        else if (dataTypeContext instanceof MySqlParser.NationalVaryingStringDataTypeContext) {
            MySqlParser.NationalVaryingStringDataTypeContext nationalVaryingStringDataTypeContext = (MySqlParser.NationalVaryingStringDataTypeContext) dataTypeContext;

            if (nationalVaryingStringDataTypeContext.lengthOneDimension() != null) {
                Integer length = Integer.valueOf(nationalVaryingStringDataTypeContext.lengthOneDimension().decimalLiteral().getText());
                columnEditor.length(length);
            }
        }
        else if (dataTypeContext instanceof MySqlParser.DimensionDataTypeContext) {
            MySqlParser.DimensionDataTypeContext dimensionDataTypeContext = (MySqlParser.DimensionDataTypeContext) dataTypeContext;

            Integer length = null;
            Integer scale = null;
            if (dimensionDataTypeContext.lengthOneDimension() != null) {
                length = Integer.valueOf(dimensionDataTypeContext.lengthOneDimension().decimalLiteral().getText());
            }

            if (dimensionDataTypeContext.lengthTwoDimension() != null) {
                List<MySqlParser.DecimalLiteralContext> decimalLiterals = dimensionDataTypeContext.lengthTwoDimension().decimalLiteral();
                length = Integer.valueOf(decimalLiterals.get(0).getText());
                scale = Integer.valueOf(decimalLiterals.get(1).getText());
            }

            if (dimensionDataTypeContext.lengthTwoOptionalDimension() != null) {
                List<MySqlParser.DecimalLiteralContext> decimalLiterals = dimensionDataTypeContext.lengthTwoOptionalDimension().decimalLiteral();
                length = Integer.valueOf(decimalLiterals.get(0).getText());

                if (decimalLiterals.size() > 1) {
                    scale = Integer.valueOf(decimalLiterals.get(1).getText());
                }
            }
            if (length != null) {
                columnEditor.length(length);
            }
            if (scale != null) {
                columnEditor.scale(scale);
            }
        }
        else if (dataTypeContext instanceof MySqlParser.CollectionDataTypeContext) {
            MySqlParser.CollectionDataTypeContext collectionDataTypeContext = (MySqlParser.CollectionDataTypeContext) dataTypeContext;
            if (collectionDataTypeContext.charsetName() != null) {
                charsetName = collectionDataTypeContext.charsetName().getText();
            }

            if (dataType.name().toUpperCase().equals("SET")) {
                // After DBZ-132, it will always be comma seperated
                columnEditor.length(Math.max(0, collectionDataTypeContext.collectionOption().size() * 2 - 1)); // number of options + number of commas
            }
            else {
                columnEditor.length(1);
            }
        }

        String dataTypeName = dataType.name().toUpperCase();

        if (dataTypeName.equals("ENUM") || dataTypeName.equals("SET")) {
            // type expression has to be set, because the value converter needs to know the enum or set options
            columnEditor.type(dataTypeName, getText(dataTypeContext));
        }
        else {
            columnEditor.type(dataTypeName);
        }

        int jdbcDataType = dataType.jdbcType();
        columnEditor.jdbcType(jdbcDataType);

        if (columnEditor.length() == -1) {
            columnEditor.length((int) dataType.length());
        }
        if (!columnEditor.scale().isPresent() && dataType.scale() != Column.UNSET_INT_VALUE) {
            columnEditor.scale(dataType.scale());
        }
        if (Types.NCHAR == jdbcDataType || Types.NVARCHAR == jdbcDataType) {
            // NCHAR and NVARCHAR columns always uses utf8 as charset
            columnEditor.charsetName("utf8");
        }
        else {
            columnEditor.charsetName(charsetName);
        }
    }

    private void convertDefaultValueToSchemaType(ColumnEditor columnEditor) {
        final Column column = columnEditor.create();
        // if converters is not null and the default value is not null, we need to convert default value
        if (converters != null && columnEditor.defaultValue() != null) {
            Object defaultValue = columnEditor.defaultValue();
            final SchemaBuilder schemaBuilder = converters.schemaBuilder(column);
            if (schemaBuilder == null) {
                return;
            }
            final Schema schema = schemaBuilder.build();
            //In order to get the valueConverter for this column, we have to create a field;
            //The index value -1 in the field will never used when converting default value;
            //So we can set any number here;
            final Field field = new Field(column.name(), -1, schema);
            final ValueConverter valueConverter = converters.converter(column, field);
            if (defaultValue instanceof String) {
                defaultValue = defaultValuePreConverter.convert(column, (String)defaultValue);
            }
            defaultValue = valueConverter.convert(defaultValue);
            columnEditor.defaultValue(defaultValue);
        }
    }

    private String unquote(String stringLiteral) {
        return stringLiteral.substring(1, stringLiteral.length() - 1);
    }

    private String unquoteBinary(String stringLiteral) {
        return stringLiteral.substring(2, stringLiteral.length() - 1);
    }
}