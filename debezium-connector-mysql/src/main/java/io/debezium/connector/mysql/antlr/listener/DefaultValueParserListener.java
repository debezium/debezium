/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.mysql.antlr.listener;

import java.sql.Types;
import java.util.concurrent.atomic.AtomicReference;

import io.debezium.connector.binlog.util.BinaryLiteralDefaultValueUtils;
import io.debezium.connector.binlog.util.BinaryLiteralDefaultValueUtils.BinaryLiteral;
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
        // The last default of a column definition applies
        parser.binaryLiteralDefaults().remove(getColumnEditor().name());

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

                if (getColumnEditor().jdbcType() == Types.BIT) {
                    getColumnEditor().defaultValueExpression(BitDefaultValueParser.parse(signedLitCtx, parser));
                    exitDefaultValue(true);
                    return;
                }

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
        if (literalCtx.HEX_NUMBER() != null || literalCtx.BIN_NUMBER() != null) {
            getColumnEditor().defaultValueExpression(parseBinaryLiteral(literalCtx, getColumnEditor(), parser));
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

    static String parseBinaryLiteral(MySqlParser.LiteralContext literalCtx, ColumnEditor columnEditor, MySqlAntlrDdlParser parser) {
        final byte[] bytes = literalCtx.HEX_NUMBER() != null
                ? BinaryLiteralDefaultValueUtils.parseHexadecimal(literalCtx.HEX_NUMBER().getText())
                : BinaryLiteralDefaultValueUtils.parseBitValue(literalCtx.BIN_NUMBER().getText());
        final String introducer = literalCtx.UNDERSCORE_CHARSET() != null ? literalCtx.UNDERSCORE_CHARSET().getText() : null;
        // MySQL treats X'41' like 0x41, as a number in a numeric context
        final BinaryLiteral literal = new BinaryLiteral(bytes, true, introducer);
        // The character set of the column may only be known at the end of the statement, which converts the literal again
        parser.binaryLiteralDefaults().put(columnEditor.name(), literal);
        return BinaryLiteralDefaultValueUtils.toDefaultValueExpression(columnEditor, literal, parser.currentDatabaseCharset(),
                parser::getJavaEncodingForCharSet);
    }

}
