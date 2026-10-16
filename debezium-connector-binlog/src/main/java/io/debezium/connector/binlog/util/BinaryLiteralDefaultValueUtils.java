/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog.util;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.sql.Types;
import java.util.HexFormat;
import java.util.Set;
import java.util.function.UnaryOperator;

import io.debezium.relational.ColumnEditor;

/**
 * Converts hexadecimal and bit-value literals that are defaults of columns other than BIT to the default value
 * expressions expected by the default value converter. The literal kind must be preserved until this conversion:
 * 0x41 is the number 65 in a numeric column, but the string 'A' in a character column.
 */
public final class BinaryLiteralDefaultValueUtils {

    private static final Set<Integer> NUMERIC_TYPES = Set.of(Types.TINYINT, Types.SMALLINT, Types.INTEGER, Types.BIGINT,
            Types.REAL, Types.FLOAT, Types.DOUBLE, Types.NUMERIC, Types.DECIMAL, Types.BOOLEAN);
    private static final Set<Integer> BINARY_TYPES = Set.of(Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY, Types.BLOB);
    private static final String BINARY_INTRODUCER = "_binary";

    private BinaryLiteralDefaultValueUtils() {
    }

    /**
     * Returns the bytes of a hexadecimal literal such as {@code 0x41} or {@code X'41'}. An odd number of digits is padded
     * with a leading zero.
     */
    public static byte[] parseHexadecimal(String literal) {
        final String digits = digits(literal);
        return HexFormat.of().parseHex(digits.length() % 2 == 0 ? digits : "0" + digits);
    }

    /**
     * Returns the bytes of a bit-value literal such as {@code 0b1000001} or {@code b'1000001'}. The bits are padded with
     * leading zeroes to whole bytes.
     */
    public static byte[] parseBitValue(String literal) {
        final String digits = digits(literal);
        final byte[] bytes = new byte[(digits.length() + Byte.SIZE - 1) / Byte.SIZE];
        for (int index = 0; index < digits.length(); index++) {
            if (digits.charAt(digits.length() - 1 - index) == '1') {
                bytes[bytes.length - 1 - index / Byte.SIZE] |= (byte) (1 << (index % Byte.SIZE));
            }
        }
        return bytes;
    }

    /**
     * Converts a literal to the default value expression of the column.
     *
     * @param column the column; never null
     * @param literal the literal; never null
     * @param defaultCharsetName the character set to use if neither the column nor its table character set is known;
     *            may be null
     * @param javaEncodings returns the Java encoding of a database character set, or null if it is unknown
     * @return the default value expression, or null if the bytes are not valid in the character set of the value
     */
    public static String toDefaultValueExpression(ColumnEditor column, BinaryLiteral literal, String defaultCharsetName,
                                                  UnaryOperator<String> javaEncodings) {
        if (literal.number() && literal.introducer() == null && NUMERIC_TYPES.contains(column.jdbcType())) {
            return new BigInteger(1, literal.bytes()).toString();
        }
        if (BINARY_TYPES.contains(column.jdbcType())) {
            // The bytes are stored as they are, and the default value of a binary column is converted from a UTF-8 string
            return decode(literal.bytes(), StandardCharsets.UTF_8);
        }
        if (literal.introducer() != null && !BINARY_INTRODUCER.equalsIgnoreCase(literal.introducer())) {
            return decode(literal.bytes(), charset(literal.introducer().substring(1), javaEncodings));
        }
        // Without an introducer, the bytes are interpreted in the character set of the column
        String charsetName = column.charsetName() != null ? column.charsetName() : column.charsetNameOfTable();
        if (charsetName == null) {
            charsetName = defaultCharsetName;
        }
        return decode(literal.bytes(), charset(charsetName, javaEncodings));
    }

    /**
     * A hexadecimal or bit-value literal that is the default of a column.
     *
     * @param bytes the bytes of the literal; never null
     * @param number whether the literal is a number in a numeric context, as {@code 0x41} is; a literal with a character
     *            set introducer is always a string
     * @param introducer the character set introducer of the literal, such as {@code _latin1}; may be null
     */
    public record BinaryLiteral(byte[] bytes, boolean number, String introducer) {
    }

    private static Charset charset(String charsetName, UnaryOperator<String> javaEncodings) {
        final String encoding = charsetName == null ? null : javaEncodings.apply(charsetName);
        if (encoding == null || !Charset.isSupported(encoding)) {
            return StandardCharsets.UTF_8;
        }
        return Charset.forName(encoding);
    }

    private static String decode(byte[] bytes, Charset charset) {
        try {
            return charset.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes))
                    .toString();
        }
        catch (CharacterCodingException e) {
            // The value cannot be represented, for example the bytes of a binary column that are not valid UTF-8
            return null;
        }
    }

    private static String digits(String literal) {
        return literal.endsWith("'") ? literal.substring(2, literal.length() - 1) : literal.substring(2);
    }
}
