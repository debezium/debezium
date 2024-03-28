/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb.antlr.listener;

import java.util.concurrent.atomic.AtomicReference;

import io.debezium.ddl.parser.mysql.generated.MySqlParser.CurrentTimestampContext;
import io.debezium.ddl.parser.mysql.generated.MySqlParser.DefaultValueContext;
import io.debezium.ddl.parser.mysql.generated.MySqlParserBaseListener;
import io.debezium.relational.ColumnEditor;

/**
 * Parser listener that parses default value definitions.
 *
 * @author Chris Cranford
 */
public class DefaultValueParserListener extends MySqlParserBaseListener {

    private final ColumnEditor columnEditor;
    private final AtomicReference<Boolean> optionalColumn;

    private boolean converted;

    public DefaultValueParserListener(ColumnEditor columnEditor, AtomicReference<Boolean> optionalColumn) {
        this.columnEditor = columnEditor;
        this.optionalColumn = optionalColumn;
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
                if (ctx.constant().stringLiteral().COLLATE() == null) {
                    columnEditor.defaultValueExpression(sign + unquote(ctx.constant().stringLiteral().getText()));
                }
                else {
                    columnEditor.defaultValueExpression(
                            sign + unquote(ctx.constant().stringLiteral().STRING_LITERAL(0).getText()));
                }
            }
            else if (ctx.constant().decimalLiteral() != null) {
                columnEditor.defaultValueExpression(sign + ctx.constant().decimalLiteral().getText());
            }
            else if (ctx.constant().BIT_STRING() != null) {
                columnEditor.defaultValueExpression(unquoteBinary(ctx.constant().BIT_STRING().getText()));
            }
            else if (ctx.constant().booleanLiteral() != null) {
                columnEditor.defaultValueExpression(ctx.constant().booleanLiteral().getText());
            }
            else if (ctx.constant().REAL_LITERAL() != null) {
                columnEditor.defaultValueExpression(ctx.constant().REAL_LITERAL().getText());
            }
        }
        else if (ctx.currentTimestamp() != null && !ctx.currentTimestamp().isEmpty()) {
            if (ctx.currentTimestamp().size() > 1 || (ctx.ON() == null && ctx.UPDATE() == null)) {
                final CurrentTimestampContext currentTimestamp = ctx.currentTimestamp(0);
                if (currentTimestamp.CURRENT_TIMESTAMP() != null || currentTimestamp.NOW() != null) {
                    columnEditor.defaultValueExpression("1970-01-01 00:00:00");
                }
                else {
                    columnEditor.defaultValueExpression(currentTimestamp.getText());
                }
            }
        }
        // Default value is calculated.
        // We thus handle it as NULL.
        else if (ctx.expression() != null) {
            columnEditor.defaultValueExpression(null);
        }
        exitDefaultValue(true);
        super.enterDefaultValue(ctx);
    }

    public void exitDefaultValue(boolean skipIfUnknownOptional) {
        boolean isOptionalColumn = optionalColumn.get() != null;
        if (!converted && (isOptionalColumn || !skipIfUnknownOptional)) {
            if (isOptionalColumn) {
                columnEditor.optional(optionalColumn.get().booleanValue());
            }
            converted = true;
        }
    }

    private String unquote(String stringLiteral) {
        if (stringLiteral != null && ((stringLiteral.startsWith("'") && stringLiteral.endsWith("'"))
                || (stringLiteral.startsWith("\"") && stringLiteral.endsWith("\"")))) {
            return stringLiteral.substring(1, stringLiteral.length() - 1);
        }
        return stringLiteral;
    }

    private String unquoteBinary(String stringLiteral) {
        return stringLiteral.substring(2, stringLiteral.length() - 1);
    }
}
