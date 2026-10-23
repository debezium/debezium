/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb.antlr.listener;

import static io.debezium.connector.binlog.util.BitDefaultValueUtils.parseBinaryLiteral;
import static io.debezium.connector.binlog.util.BitDefaultValueUtils.parseNumber;
import static io.debezium.connector.binlog.util.BitDefaultValueUtils.unquote;

import java.io.ByteArrayOutputStream;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import io.debezium.antlr.mysql.SqlMode;
import io.debezium.antlr.mysql.SqlModes;
import io.debezium.connector.binlog.jdbc.BinlogSystemVariables;
import io.debezium.connector.binlog.util.BitDefaultValueUtils;
import io.debezium.connector.mariadb.antlr.MariaDbAntlrDdlParser;
import io.debezium.ddl.parser.mariadb.generated.MariaDBParser;

/**
 * Normalizes BIT literals before the default value converter loses their literal kind.
 */
final class BitDefaultValueParser {

    private BitDefaultValueParser() {
    }

    static String parse(MariaDBParser.DefaultValueContext context, MariaDbAntlrDdlParser parser) {
        final var literal = context.constant();
        if (literal == null) {
            return null;
        }
        final var sign = context.unaryOperator() == null ? "" : context.unaryOperator().getText();
        if (!sign.isEmpty() && !"+".equals(sign) && !"-".equals(sign)) {
            // Other unary operators are expressions, not literal defaults.
            return null;
        }
        if (!sign.isEmpty() && literal.decimalLiteral() == null && literal.REAL_LITERAL() == null) {
            return null;
        }

        final BigInteger value;
        if (literal.decimalLiteral() != null || literal.REAL_LITERAL() != null) {
            final var number = context.getText();
            // MariaDB uses REAL_LITERAL for both exact decimals and approximate, exponent-form numbers.
            final boolean approximate = number.indexOf('e') >= 0 || number.indexOf('E') >= 0;
            if (approximate) {
                final double doubleValue = Double.parseDouble(number);
                if (!Double.isFinite(doubleValue) || doubleValue >= 0x1p63 || doubleValue < -0x1p63) {
                    // MariaDB's Field_bit::store(double) casts to signed long long without checking the range.
                    // The undefined conversion can yield different defaults across server builds; see the
                    // related INSERT bug https://jira.mariadb.org/browse/MDEV-35715.
                    // Omit the schema default rather than guess the server result. Row values are unaffected.
                    return null;
                }
            }
            value = parseNumber(number, approximate);
        }
        else if (literal.hexadecimalLiteral() != null) {
            // Unlike MySQL, MariaDB accepts leading zero bytes in X'...' beyond eight bytes.
            value = parseBinaryLiteral(literal.hexadecimalLiteral().HEXADECIMAL_LITERAL().getText(), 16);
        }
        else if (literal.BIT_STRING() != null) {
            value = parseBinaryLiteral(literal.BIT_STRING().getText(), 2);
        }
        else if (literal.booleanLiteral() != null) {
            value = literal.booleanLiteral().TRUE() != null ? BigInteger.ONE : BigInteger.ZERO;
        }
        else if (literal.stringLiteral() != null) {
            final var sqlMode = parser.systemVariables().getVariable("sql_mode");
            final boolean ansiQuotes = sqlMode != null && SqlModes.sqlModeFromString(sqlMode).contains(SqlMode.AnsiQuotes);
            if (literal.stringLiteral().STRING_LITERAL().stream()
                    .anyMatch(part -> part.getText().startsWith("`") || (ansiQuotes && part.getText().startsWith("\"")))) {
                // The grammar also classifies quoted column references as string literals.
                return null;
            }
            value = parseTextLiteral(literal.stringLiteral(), parser);
        }
        else {
            return null;
        }

        return BitDefaultValueUtils.asBinaryString(value);
    }

    private static BigInteger parseTextLiteral(MariaDBParser.StringLiteralContext literal, MariaDbAntlrDdlParser parser) {
        final var sqlMode = parser.systemVariables().getVariable("sql_mode");
        final boolean backslashEscapes = sqlMode == null || !SqlModes.sqlModeFromString(sqlMode).contains(SqlMode.NoBackslashEscapes);
        final var bytes = new ByteArrayOutputStream();
        if (literal.START_NATIONAL_STRING_LITERAL() != null) {
            bytes.writeBytes(unquote(literal.START_NATIONAL_STRING_LITERAL().getText().substring(1), backslashEscapes).getBytes(StandardCharsets.UTF_8));
        }
        final var parts = literal.STRING_LITERAL();
        for (int index = 0; index < parts.size(); index++) {
            // An introducer labels the original bytes; it does not transcode them to the named character set.
            final var variable = index == 0 && literal.STRING_CHARSET_NAME() != null
                    ? BinlogSystemVariables.CHARSET_NAME_CLIENT
                    : BinlogSystemVariables.CHARSET_NAME_CONNECTION;
            bytes.writeBytes(unquote(parts.get(index).getText(), backslashEscapes).getBytes(charset(parser, variable)));
        }
        return new BigInteger(1, bytes.toByteArray());
    }

    private static Charset charset(MariaDbAntlrDdlParser parser, String variable) {
        final var charsetName = parser.systemVariables().getVariable(variable);
        final var registry = parser.getCharsetRegistry();
        final var encoding = charsetName == null || registry == null ? null : registry.getJavaEncodingForCharSet(charsetName);
        return encoding == null ? StandardCharsets.UTF_8 : Charset.forName(encoding);
    }
}
