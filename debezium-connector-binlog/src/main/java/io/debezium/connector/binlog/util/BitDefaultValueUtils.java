/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog.util;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;

/**
 * Converts BIT default literals while their numeric, binary, or string representation is still known.
 */
public final class BitDefaultValueUtils {

    public static final BigInteger UNSIGNED_LONG_MAX = BigInteger.ONE.shiftLeft(Long.SIZE).subtract(BigInteger.ONE);

    private BitDefaultValueUtils() {
    }

    public static String asBinaryString(BigInteger value) {
        // Negative defaults accepted by BIT(64) retain their unsigned 64-bit representation.
        return value.and(UNSIGNED_LONG_MAX).toString(2);
    }

    public static BigInteger parseNumber(String value, boolean approximate) {
        if (approximate) {
            // Truncate approximate numbers, clamping to the signed long range as MySQL does.
            // MariaDB callers must reject out-of-range values before using this conversion.
            return BigInteger.valueOf((long) Double.parseDouble(value));
        }
        return new BigDecimal(value).setScale(0, RoundingMode.HALF_UP).toBigIntegerExact();
    }

    public static BigInteger parseBinaryLiteral(String literal, int radix) {
        final var digits = literal.endsWith("'") ? literal.substring(2, literal.length() - 1) : literal.substring(2);
        return digits.isEmpty() ? BigInteger.ZERO : new BigInteger(digits, radix);
    }

    public static String unquote(String literal, boolean backslashEscapes) {
        final var result = new StringBuilder();
        final char quote = literal.charAt(0);
        for (int index = 1; index < literal.length() - 1; index++) {
            final char character = literal.charAt(index);
            if (character == quote && index + 1 < literal.length() - 1 && literal.charAt(index + 1) == quote) {
                result.append(quote);
                index++;
            }
            else if (character == '\\' && backslashEscapes && index + 1 < literal.length() - 1) {
                final char escaped = literal.charAt(++index);
                switch (escaped) {
                    case '0' -> result.append('\0');
                    case 'b' -> result.append('\b');
                    case 'n' -> result.append('\n');
                    case 'r' -> result.append('\r');
                    case 't' -> result.append('\t');
                    case 'Z' -> result.append('\u001a');
                    case '%', '_' -> result.append('\\').append(escaped);
                    default -> result.append(escaped);
                }
            }
            else {
                result.append(character);
            }
        }
        return result.toString();
    }
}
