/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.mysql.antlr.listener;

import java.util.concurrent.atomic.AtomicReference;

import io.debezium.connector.mysql.MySqlDefaultValueConverter;
import io.debezium.connector.mysql.MySqlValueConverters;
import io.debezium.ddl.parser.mysql.generated.MySqlParser.CurrentTimestampContext;
import io.debezium.ddl.parser.mysql.generated.MySqlParser.DefaultValueContext;
import io.debezium.ddl.parser.mysql.generated.MySqlParserBaseListener;
import io.debezium.relational.ColumnEditor;

/**
 * Parser listener that is parsing default value definition part of MySQL statements.
 *
 * @author Jiri Pechanec
 */
public class DefaultValueParserListener extends MySqlParserBaseListener {

    private final ColumnEditor columnEditor;
    private final AtomicReference<Boolean> optionalColumn;

    private final MySqlDefaultValueConverter defaultValueConverter;

    /**
     * Whether to convert the column's default value into the corresponding schema type or not. This is done for column
     * definitions of ALTER TABLE statements but not for CREATE TABLE. In case of the latter, the default value
     * conversion is handled by the CREATE TABLE statement listener itself, as a default character set given at the
     * table level might have to be applied.
     */
    private final boolean convertDefault;
    private boolean converted;

    public DefaultValueParserListener(ColumnEditor columnEditor, MySqlValueConverters converters,
                                      AtomicReference<Boolean> optionalColumn, boolean convertDefault) {
        this.columnEditor = columnEditor;
        this.defaultValueConverter = new MySqlDefaultValueConverter(converters);
        this.optionalColumn = optionalColumn;
        this.convertDefault = convertDefault;
        this.converted = false;
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
        else if (ctx.currentTimestamp() != null && !ctx.currentTimestamp().isEmpty()) {
            if (ctx.currentTimestamp().size() > 1 || (ctx.ON() == null && ctx.UPDATE() == null)) {
                final CurrentTimestampContext currentTimestamp = ctx.currentTimestamp(0);
                if (currentTimestamp.CURRENT_TIMESTAMP() != null || currentTimestamp.NOW() != null) {
                    columnEditor.defaultValue("1970-01-01 00:00:00");
                }
                else {
                    columnEditor.defaultValue(currentTimestamp.getText());
                }
            }
        }
        convertDefaultValue(true);
        super.enterDefaultValue(ctx);
    }

    public void convertDefaultValue(boolean skipIfUnknownOptional) {
        // For CREATE TABLE are all column default values converted only after charset is known.
        if (convertDefault) {
            if (!converted && (optionalColumn.get() != null || !skipIfUnknownOptional)) {
                convertDefaultValueToSchemaType(columnEditor);
                converted = true;
            }
        }
    }

    private void convertDefaultValueToSchemaType(ColumnEditor columnEditor) {
        if (optionalColumn.get() != null) {
            columnEditor.optional(optionalColumn.get().booleanValue());
        }

        defaultValueConverter.setColumnDefaultValue(columnEditor);
    }

    private String unquote(String stringLiteral) {
        return stringLiteral.substring(1, stringLiteral.length() - 1);
    }

    private String unquoteBinary(String stringLiteral) {
        return stringLiteral.substring(2, stringLiteral.length() - 1);
    }

}
