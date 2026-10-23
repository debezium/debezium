/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb.antlr.listener;

import java.sql.Types;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.UnaryOperator;

import io.debezium.connector.binlog.util.BinaryLiteralDefaultValueUtils;
import io.debezium.connector.binlog.util.BinaryLiteralDefaultValueUtils.BinaryLiteral;
import io.debezium.connector.mariadb.antlr.MariaDbAntlrDdlParser;
import io.debezium.ddl.parser.mariadb.generated.MariaDBParser.CurrentTimestampContext;
import io.debezium.ddl.parser.mariadb.generated.MariaDBParser.DefaultValueContext;
import io.debezium.ddl.parser.mariadb.generated.MariaDBParserBaseListener;
import io.debezium.relational.ColumnEditor;

/**
 * Parser listener that parses default value definitions.
 *
 * @author Chris Cranford
 */
public class DefaultValueParserListener extends MariaDBParserBaseListener {

    private final ColumnEditor columnEditor;
    private final AtomicReference<Boolean> optionalColumn;
    private final MariaDbAntlrDdlParser parser;

    private boolean converted;

    public DefaultValueParserListener(ColumnEditor columnEditor, AtomicReference<Boolean> optionalColumn, MariaDbAntlrDdlParser parser) {
        this.columnEditor = columnEditor;
        this.optionalColumn = optionalColumn;
        this.parser = parser;
        this.converted = false;
    }

    @Override
    public void enterDefaultValue(DefaultValueContext ctx) {
        // The last default of a column definition applies
        parser.binaryLiteralDefaults().remove(columnEditor.name());
        if (columnEditor.jdbcType() == Types.BIT) {
            columnEditor.defaultValueExpression(BitDefaultValueParser.parse(ctx, parser));
            exitDefaultValue(true);
            super.enterDefaultValue(ctx);
            return;
        }
        String sign = "";
        if (ctx.NULL_LITERAL() != null) {
            return;
        }
        if (ctx.unaryOperator() != null) {
            sign = ctx.unaryOperator().getText();
        }
        if (ctx.constant() != null) {
            if (ctx.constant().stringLiteral() != null) {
                columnEditor.defaultValueExpression(sign + parser.parseStringLiteral(ctx.constant().stringLiteral()));
            }
            else if (ctx.constant().decimalLiteral() != null) {
                columnEditor.defaultValueExpression(sign + ctx.constant().decimalLiteral().getText());
            }
            else if (ctx.constant().hexadecimalLiteral() != null) {
                final var literal = ctx.constant().hexadecimalLiteral();
                final var text = literal.HEXADECIMAL_LITERAL().getText();
                final var introducer = literal.STRING_CHARSET_NAME() != null ? literal.STRING_CHARSET_NAME().getText() : null;
                // Unlike 0x41, MariaDB treats X'41' as a string, also in a numeric context
                columnEditor.defaultValueExpression(toDefaultValueExpression(
                        new BinaryLiteral(BinaryLiteralDefaultValueUtils.parseHexadecimal(text), !text.endsWith("'"), introducer)));
            }
            else if (ctx.constant().BIT_STRING() != null) {
                columnEditor.defaultValueExpression(toDefaultValueExpression(
                        new BinaryLiteral(BinaryLiteralDefaultValueUtils.parseBitValue(ctx.constant().BIT_STRING().getText()), true, null)));
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

    private String toDefaultValueExpression(BinaryLiteral literal) {
        // The character set of the column may only be known at the end of the statement, which converts the literal again
        parser.binaryLiteralDefaults().put(columnEditor.name(), literal);
        return BinaryLiteralDefaultValueUtils.toDefaultValueExpression(columnEditor, literal, parser.currentDatabaseCharset(), javaEncodings(parser));
    }

    static UnaryOperator<String> javaEncodings(MariaDbAntlrDdlParser parser) {
        final var charsetRegistry = parser.getCharsetRegistry();
        return charsetName -> charsetRegistry == null ? null : charsetRegistry.getJavaEncodingForCharSet(charsetName);
    }
}
