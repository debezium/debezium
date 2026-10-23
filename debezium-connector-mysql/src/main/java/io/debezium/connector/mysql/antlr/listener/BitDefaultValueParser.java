/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.antlr.listener;

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
import io.debezium.connector.mysql.antlr.MySqlAntlrDdlParser;
import io.debezium.ddl.parser.mysql.generated.MySqlParser;

/**
 * Normalizes BIT literals to the binary digit strings expected by the default value converter.
 * The literal kind must be preserved until this conversion: 10, b'10', x'10', and '10' have different values.
 */
final class BitDefaultValueParser {

    private BitDefaultValueParser() {
    }

    static String parse(MySqlParser.SignedLiteralOrNullContext context, MySqlAntlrDdlParser parser) {
        final var signed = context.signedLiteral();
        if (signed == null) {
            return null;
        }

        final BigInteger value;
        final var literal = signed.literal();
        if (signed.ulong_number() != null) {
            final var number = signed.ulong_number();
            if (number.HEX_NUMBER() != null) {
                final var magnitude = parseBinaryLiteral(number.HEX_NUMBER().getText(), 16);
                value = signed.MINUS_OPERATOR() != null ? magnitude.negate() : magnitude;
            }
            else {
                value = parseNumber(signed.getText(), number.FLOAT_NUMBER() != null);
            }
        }
        else if (literal.numLiteral() != null) {
            final var number = literal.numLiteral();
            value = parseNumber(number.getText(), number.FLOAT_NUMBER() != null);
        }
        else if (literal.HEX_NUMBER() != null) {
            value = parseBinaryLiteral(literal.HEX_NUMBER().getText(), 16);
        }
        else if (literal.BIN_NUMBER() != null) {
            value = parseBinaryLiteral(literal.BIN_NUMBER().getText(), 2);
        }
        else if (literal.boolLiteral() != null) {
            value = literal.boolLiteral().TRUE_SYMBOL() != null ? BigInteger.ONE : BigInteger.ZERO;
        }
        else if (literal.textLiteral() != null) {
            value = parseTextLiteral(literal.textLiteral(), parser);
        }
        else {
            return null;
        }

        return BitDefaultValueUtils.asBinaryString(value);
    }

    private static BigInteger parseBinaryLiteral(String literal, int radix) {
        final var digits = literal.endsWith("'") ? literal.substring(2, literal.length() - 1) : literal.substring(2);
        // Oversized binary strings accepted in non-strict mode saturate, even when padded with leading zeroes.
        if (digits.length() > (radix == 16 ? Long.SIZE / 4 : Long.SIZE)) {
            return BitDefaultValueUtils.UNSIGNED_LONG_MAX;
        }
        return BitDefaultValueUtils.parseBinaryLiteral(literal, radix);
    }

    private static BigInteger parseTextLiteral(MySqlParser.TextLiteralContext literal, MySqlAntlrDdlParser parser) {
        final var sqlMode = parser.systemVariables().getVariable("sql_mode");
        final boolean backslashEscapes = sqlMode == null || !SqlModes.sqlModeFromString(sqlMode).contains(SqlMode.NoBackslashEscapes);
        final var bytes = new ByteArrayOutputStream();
        if (literal.NCHAR_TEXT() != null) {
            bytes.writeBytes(unquote(literal.NCHAR_TEXT().getText().substring(1), backslashEscapes).getBytes(StandardCharsets.UTF_8));
        }
        final var parts = literal.textStringLiteral();
        for (int index = 0; index < parts.size(); index++) {
            // An introducer labels the original bytes; it does not transcode them to the named character set.
            final var variable = index == 0 && literal.UNDERSCORE_CHARSET() != null
                    ? BinlogSystemVariables.CHARSET_NAME_CLIENT
                    : BinlogSystemVariables.CHARSET_NAME_CONNECTION;
            bytes.writeBytes(unquote(parts.get(index).getText(), backslashEscapes).getBytes(charset(parser, variable)));
        }
        return new BigInteger(1, bytes.toByteArray());
    }

    private static Charset charset(MySqlAntlrDdlParser parser, String variable) {
        final var charsetName = parser.systemVariables().getVariable(variable);
        final var encoding = charsetName == null ? null : parser.getJavaEncodingForCharSet(charsetName);
        return encoding == null ? StandardCharsets.UTF_8 : Charset.forName(encoding);
    }

}
