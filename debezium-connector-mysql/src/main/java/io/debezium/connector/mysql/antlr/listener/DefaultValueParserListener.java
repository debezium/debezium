/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.mysql.antlr.listener;

import java.util.concurrent.atomic.AtomicReference;

import io.debezium.connector.mysql.antlr.MySqlAntlrDdlParser;
import io.debezium.ddl.parser.mysql.generated.MySqlParser;
import io.debezium.ddl.parser.mysql.generated.MySqlParserBaseListener;
import io.debezium.relational.ColumnEditor;

/**
 * Parser listener that is parsing default value definition part of MySQL statements.
 *
 * @author Jiri Pechanec
 */
public class DefaultValueParserListener extends MySqlParserBaseListener {

    private final ColumnDefinitionParserListener columnDefinitionListener;
    private final AtomicReference<Boolean> optionalColumn;
    private final MySqlAntlrDdlParser parser;

    private boolean converted;

    public DefaultValueParserListener(ColumnDefinitionParserListener columnDefinitionListener,
                                      AtomicReference<Boolean> optionalColumn, MySqlAntlrDdlParser parser) {
        this.columnDefinitionListener = columnDefinitionListener;
        this.optionalColumn = optionalColumn;
        this.parser = parser;
        this.converted = false;
    }

    /**
     * Get the current active ColumnEditor dynamically.
     * This ensures we always write to the correct editor, even when processing
     * multiple columns in a single ALTER TABLE statement.
     */
    private ColumnEditor getColumnEditor() {
        return columnDefinitionListener.getColumnEditor();
    }

    @Override
    public void enterColumnAttribute(MySqlParser.ColumnAttributeContext ctx) {
        // Check if this is a DEFAULT value attribute
        if (ctx.DEFAULT_SYMBOL() != null || (ctx.value != null && ctx.value.getType() == MySqlParser.DEFAULT_SYMBOL)) {
            handleDefaultValue(ctx);
        }
        super.enterColumnAttribute(ctx);
    }

    private void handleDefaultValue(MySqlParser.ColumnAttributeContext ctx) {
        // Handle expression-based defaults (MySQL 8.0.13+)
        if (ctx.exprWithParentheses() != null) {
            // Default value is calculated/expression - handle as NULL
            getColumnEditor().defaultValueExpression(null);
            exitDefaultValue(true);
            return;
        }

        // Handle nowOrSignedLiteral
        if (ctx.nowOrSignedLiteral() != null) {
            MySqlParser.NowOrSignedLiteralContext literalCtx = ctx.nowOrSignedLiteral();

            // Check for NOW() / CURRENT_TIMESTAMP
            if (literalCtx.now() != null) {
                getColumnEditor().defaultValueExpression("1970-01-01 00:00:00");
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
                        getColumnEditor().defaultValueExpression(sign + signed.ulong_number().getText());
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
            getColumnEditor().defaultValueExpression(sign + parser.parseTextLiteral(literalCtx.textLiteral()));
            return;
        }

        // Numeric literal
        if (literalCtx.numLiteral() != null) {
            getColumnEditor().defaultValueExpression(sign + literalCtx.numLiteral().getText());
            return;
        }

        // Boolean literal
        if (literalCtx.boolLiteral() != null) {
            getColumnEditor().defaultValueExpression(literalCtx.boolLiteral().getText());
            return;
        }

        // Hex/Bin number
        if (literalCtx.HEX_NUMBER() != null) {
            getColumnEditor().defaultValueExpression(unquoteBinary(literalCtx.HEX_NUMBER().getText()));
            return;
        }

        if (literalCtx.BIN_NUMBER() != null) {
            getColumnEditor().defaultValueExpression(unquoteBinary(literalCtx.BIN_NUMBER().getText()));
            return;
        }

        // Temporal literal
        if (literalCtx.temporalLiteral() != null) {
            getColumnEditor().defaultValueExpression(sign + literalCtx.temporalLiteral().getText());
            return;
        }

        // Null literal - do nothing, already handled
    }

    public void exitDefaultValue(boolean skipIfUnknownOptional) {
        boolean isOptionalColumn = optionalColumn.get() != null;
        if (!converted && (isOptionalColumn || !skipIfUnknownOptional)) {
            if (isOptionalColumn) {
                getColumnEditor().optional(optionalColumn.get().booleanValue());
            }
            converted = true;
        }
    }

    private String unquoteBinary(String stringLiteral) {
        if (stringLiteral != null && stringLiteral.length() > 2) {
            return stringLiteral.substring(2, stringLiteral.length() - 1);
        }
        return stringLiteral;
    }

}
