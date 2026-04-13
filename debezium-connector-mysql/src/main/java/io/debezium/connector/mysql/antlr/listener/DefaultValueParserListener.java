/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.mysql.antlr.listener;

import java.util.concurrent.atomic.AtomicReference;

import io.debezium.ddl.parser.mysql.generated.MySqlParser;
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

    private boolean converted;

    public DefaultValueParserListener(ColumnEditor columnEditor, AtomicReference<Boolean> optionalColumn) {
        this.columnEditor = columnEditor;
        this.optionalColumn = optionalColumn;
        this.converted = false;
    }

    @Override
    public void enterColumnAttribute(MySqlParser.ColumnAttributeContext ctx) {
        // Check if this is a DEFAULT value attribute
        if (ctx.value != null && ctx.value.getType() == MySqlParser.DEFAULT_SYMBOL) {
            handleDefaultValue(ctx);
        }
        super.enterColumnAttribute(ctx);
    }

    private void handleDefaultValue(MySqlParser.ColumnAttributeContext ctx) {
        // Handle expression-based defaults (MySQL 8.0.13+)
        if (ctx.exprWithParentheses() != null) {
            // Default value is calculated/expression - handle as NULL
            columnEditor.defaultValueExpression(null);
            exitDefaultValue(true);
            return;
        }

        // Handle nowOrSignedLiteral
        if (ctx.nowOrSignedLiteral() != null) {
            MySqlParser.NowOrSignedLiteralContext literalCtx = ctx.nowOrSignedLiteral();

            // Check for NOW() / CURRENT_TIMESTAMP
            if (literalCtx.now() != null) {
                columnEditor.defaultValueExpression("1970-01-01 00:00:00");
                exitDefaultValue(true);
                return;
            }

            // Handle signedLiteralOrNull
            if (literalCtx.signedLiteralOrNull() != null) {
                MySqlParser.SignedLiteralOrNullContext signedLitCtx = literalCtx.signedLiteralOrNull();

                // Check for NULL
                if (signedLitCtx.nullAsLiteral() != null) {
                    exitDefaultValue(true);
                    return;
                }

                // Handle signedLiteral
                if (signedLitCtx.signedLiteral() != null) {
                    MySqlParser.SignedLiteralContext signed = signedLitCtx.signedLiteral();

                    // Extract sign if present
                    String sign = "";
                    if (signed.PLUS_OPERATOR() != null) {
                        sign = "+";
                    }
                    else if (signed.MINUS_OPERATOR() != null) {
                        sign = "-";
                    }

                    // Handle signed number
                    if (signed.ulong_number() != null) {
                        columnEditor.defaultValueExpression(sign + signed.ulong_number().getText());
                        exitDefaultValue(true);
                        return;
                    }

                    // Handle literal (text, numeric, temporal, etc.)
                    if (signed.literal() != null) {
                        handleLiteral(signed.literal(), sign);
                        exitDefaultValue(true);
                        return;
                    }
                }
            }
        }

        exitDefaultValue(true);
    }

    private void handleLiteral(MySqlParser.LiteralContext literalCtx, String sign) {
        // Text literal
        if (literalCtx.textLiteral() != null) {
            String text = literalCtx.textLiteral().getText();
            columnEditor.defaultValueExpression(sign + unquote(text));
            return;
        }

        // Numeric literal
        if (literalCtx.numLiteral() != null) {
            columnEditor.defaultValueExpression(sign + literalCtx.numLiteral().getText());
            return;
        }

        // Boolean literal
        if (literalCtx.boolLiteral() != null) {
            columnEditor.defaultValueExpression(literalCtx.boolLiteral().getText());
            return;
        }

        // Hex/Bin number
        if (literalCtx.HEX_NUMBER() != null) {
            columnEditor.defaultValueExpression(unquoteBinary(literalCtx.HEX_NUMBER().getText()));
            return;
        }

        if (literalCtx.BIN_NUMBER() != null) {
            columnEditor.defaultValueExpression(unquoteBinary(literalCtx.BIN_NUMBER().getText()));
            return;
        }

        // Temporal literal
        if (literalCtx.temporalLiteral() != null) {
            columnEditor.defaultValueExpression(sign + literalCtx.temporalLiteral().getText());
            return;
        }

        // Null literal - do nothing, already handled
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
        if (stringLiteral != null && stringLiteral.length() > 2) {
            return stringLiteral.substring(2, stringLiteral.length() - 1);
        }
        return stringLiteral;
    }

}
