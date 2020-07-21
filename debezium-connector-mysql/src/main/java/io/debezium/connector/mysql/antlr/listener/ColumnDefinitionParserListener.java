/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.mysql.antlr.listener;

import java.sql.Types;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.antlr.v4.runtime.tree.ParseTreeListener;

import io.debezium.antlr.AntlrDdlParser;
import io.debezium.antlr.DataTypeResolver;
import io.debezium.connector.mysql.antlr.MySqlAntlrDdlParser;
import io.debezium.ddl.parser.mysql.generated.MySqlParser;
import io.debezium.ddl.parser.mysql.generated.MySqlParserBaseListener;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.TableEditor;
import io.debezium.relational.ddl.DataType;

/**
 * Parser listener that is parsing column definition part of MySQL statements.
 *
 * @author Roman Kuchár <kucharrom@gmail.com>.
 */
public class ColumnDefinitionParserListener extends MySqlParserBaseListener {

    private final MySqlAntlrDdlParser parser;
    private final DataTypeResolver dataTypeResolver;
    private final TableEditor tableEditor;
    private ColumnEditor columnEditor;
    private boolean uniqueColumn;
    private AtomicReference<Boolean> optionalColumn = new AtomicReference<>();
    private DefaultValueParserListener defaultValueListener;

    private final List<ParseTreeListener> listeners;

    /**
     * Whether to convert the column's default value into the corresponding schema type or not. This is done for column
     * definitions of ALTER TABLE statements but not for CREATE TABLE. In case of the latter, the default value
     * conversion is handled by the CREATE TABLE statement listener itself, as a default character set given at the
     * table level might have to be applied.
     */
    private final boolean convertDefault;

    public ColumnDefinitionParserListener(TableEditor tableEditor, ColumnEditor columnEditor, MySqlAntlrDdlParser parser,
                                          List<ParseTreeListener> listeners, boolean convertDefault) {
        this.tableEditor = tableEditor;
        this.columnEditor = columnEditor;
        this.parser = parser;
        this.dataTypeResolver = parser.dataTypeResolver();
        this.listeners = listeners;
        this.convertDefault = convertDefault;
    }

    public ColumnDefinitionParserListener(TableEditor tableEditor, ColumnEditor columnEditor, MySqlAntlrDdlParser parser,
                                          List<ParseTreeListener> listeners) {
        this(tableEditor, columnEditor, parser, listeners, true);
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
        uniqueColumn = false;
        optionalColumn = new AtomicReference<>();
        resolveColumnDataType(ctx.dataType());
        parser.runIfNotNull(() -> {
            defaultValueListener = new DefaultValueParserListener(columnEditor, parser.getConverters(), optionalColumn, convertDefault);
            listeners.add(defaultValueListener);
        }, tableEditor);
        super.enterColumnDefinition(ctx);
    }

    @Override
    public void exitColumnDefinition(MySqlParser.ColumnDefinitionContext ctx) {
        if (optionalColumn.get() != null) {
            columnEditor.optional(optionalColumn.get().booleanValue());
        }
        if (uniqueColumn && !tableEditor.hasPrimaryKey()) {
            // take the first unique constrain if no primary key is set
            tableEditor.addColumn(columnEditor.create());
            tableEditor.setPrimaryKeyNames(columnEditor.name());
        }
        defaultValueListener.convertDefaultValue(false);
        parser.runIfNotNull(() -> {
            listeners.remove(defaultValueListener);
        }, tableEditor);
        super.exitColumnDefinition(ctx);
    }

    @Override
    public void enterUniqueKeyColumnConstraint(MySqlParser.UniqueKeyColumnConstraintContext ctx) {
        uniqueColumn = true;
        super.enterUniqueKeyColumnConstraint(ctx);
    }

    @Override
    public void enterPrimaryKeyColumnConstraint(MySqlParser.PrimaryKeyColumnConstraintContext ctx) {
        // this rule will be parsed only if no primary key is set in a table
        // otherwise the statement can't be executed due to multiple primary key error
        optionalColumn.set(Boolean.FALSE);
        tableEditor.addColumn(columnEditor.create());
        tableEditor.setPrimaryKeyNames(columnEditor.name());
        super.enterPrimaryKeyColumnConstraint(ctx);
    }

    @Override
    public void enterNullNotnull(MySqlParser.NullNotnullContext ctx) {
        optionalColumn.set(Boolean.valueOf(ctx.NOT() == null));
        super.enterNullNotnull(ctx);
    }

    @Override
    public void enterAutoIncrementColumnConstraint(MySqlParser.AutoIncrementColumnConstraintContext ctx) {
        columnEditor.autoIncremented(true);
        columnEditor.generated(true);
        super.enterAutoIncrementColumnConstraint(ctx);
    }

    @Override
    public void enterSerialDefaultColumnConstraint(MySqlParser.SerialDefaultColumnConstraintContext ctx) {
        serialColumn();
        super.enterSerialDefaultColumnConstraint(ctx);
    }

    private void resolveColumnDataType(MySqlParser.DataTypeContext dataTypeContext) {
        String charsetName = null;
        DataType dataType = dataTypeResolver.resolveDataType(dataTypeContext);

        if (dataTypeContext instanceof MySqlParser.StringDataTypeContext) {
            // Same as LongVarcharDataTypeContext but with dimension handling
            MySqlParser.StringDataTypeContext stringDataTypeContext = (MySqlParser.StringDataTypeContext) dataTypeContext;

            if (stringDataTypeContext.lengthOneDimension() != null) {
                Integer length = Integer.valueOf(stringDataTypeContext.lengthOneDimension().decimalLiteral().getText());
                columnEditor.length(length);
            }

            if (stringDataTypeContext.charsetName() != null) {
                charsetName = stringDataTypeContext.charsetName().getText();
            }
        }
        else if (dataTypeContext instanceof MySqlParser.LongVarcharDataTypeContext) {
            // Same as StringDataTypeContext but without dimension handling
            MySqlParser.LongVarcharDataTypeContext longVarcharTypeContext = (MySqlParser.LongVarcharDataTypeContext) dataTypeContext;

            if (longVarcharTypeContext.charsetName() != null) {
                charsetName = longVarcharTypeContext.charsetName().getText();
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
                // After DBZ-132, it will always be comma separated
                int optionsSize = collectionDataTypeContext.collectionOptions().collectionOption().size();
                columnEditor.length(Math.max(0, optionsSize * 2 - 1)); // number of options + number of commas
            }
            else {
                columnEditor.length(1);
            }
        }

        String dataTypeName = dataType.name().toUpperCase();

        if (dataTypeName.equals("ENUM") || dataTypeName.equals("SET")) {
            // type expression has to be set, because the value converter needs to know the enum or set options
            MySqlParser.CollectionDataTypeContext collectionDataTypeContext = (MySqlParser.CollectionDataTypeContext) dataTypeContext;

            List<String> collectionOptions = collectionDataTypeContext.collectionOptions().collectionOption().stream()
                    .map(AntlrDdlParser::getText)
                    .collect(Collectors.toList());

            columnEditor.type(dataTypeName);
            columnEditor.enumValues(collectionOptions);
        }
        else if (dataTypeName.equals("SERIAL")) {
            // SERIAL is an alias for BIGINT UNSIGNED NOT NULL AUTO_INCREMENT UNIQUE
            columnEditor.type("BIGINT UNSIGNED");
            serialColumn();
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

    private void serialColumn() {
        if (optionalColumn.get() == null) {
            optionalColumn.set(Boolean.FALSE);
        }
        uniqueColumn = true;
        columnEditor.autoIncremented(true);
        columnEditor.generated(true);
    }
}
