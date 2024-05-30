/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import java.math.BigDecimal;

/**
 * A converter for unsigned integer types.<p></p>
 *
 * It is intended to convert any integer type value from binlog into the current representation of unsigned
 * numeric values. The binlog store unsigned numeric values as {@code insertion value - max data type boundary - 1},
 * therefore, to calculate the correct unsigned numeric representation, the calculation needs to be inverted
 * as {@code insertion value + max data type boundary + 1}.<p></p>
 *
 * See DBZ-228 for more information.
 *
 * @author Omar Al-Safi
 * @author Chris Cranford
 */
public class BinlogUnsignedIntegerConverter {

    /**
     * Maximum values for unsigned integer types, used to calculate actual value from binlog.
     * See <a href="https://dev.mysql.com/doc/refman/8.2/en/integer-types.html">integer-types</a>
     */
    private static final short TINYINT_MAX_VALUE = 255;
    private static final int SMALLINT_MAX_VALUE = 65535;
    private static final int MEDIUMINT_MAX_VALUE = 16777215;
    private static final long INT_MAX_VALUE = 4294967295L;
    private static final BigDecimal BIGINT_MAX_VALUE = new BigDecimal("18446744073709551615");

    private static final short TINYINT_CORRECTION = TINYINT_MAX_VALUE + 1;
    private static final int SMALLINT_CORRECTION = SMALLINT_MAX_VALUE + 1;
    private static final int MEDIUMINT_CORRECTION = MEDIUMINT_MAX_VALUE + 1;
    private static final long INT_CORRECTION = INT_MAX_VALUE + 1;
    private static final BigDecimal BIGINT_CORRECTION = BIGINT_MAX_VALUE.add(BigDecimal.ONE);

    /**
     * Convert original value insertion of type 'TINYINT' into the correct TINYINT UNSIGNED representation
     * Note: Unsigned TINYINT (8-bit) is represented in 'Short' 16-bit data type.
     * Reference: https://kafka.apache.org/0102/javadoc/org/apache/kafka/connect/data/Schema.Type.html
     *
     * @param originalNumber {@link Short} the original insertion value
     * @return {@link Short} the correct representation of the original insertion value
     */
    public static short convertUnsignedTinyint(short originalNumber) {
        if (originalNumber < 0) {
            return (short) (originalNumber + TINYINT_CORRECTION);
        }
        else {
            return originalNumber;
        }
    }

    /**
     * Convert original value insertion of type 'SMALLINT' into the correct SMALLINT UNSIGNED representation
     * Note: Unsigned SMALLINT (16-bit) is represented in 'Integer' 32-bit data type.
     * Reference: https://kafka.apache.org/0102/javadoc/org/apache/kafka/connect/data/Schema.Type.html
     *
     * @param originalNumber {@link Integer} the original insertion value
     * @return {@link Integer} the correct representation of the original insertion value
     */
    public static int convertUnsignedSmallint(int originalNumber) {
        if (originalNumber < 0) {
            return originalNumber + SMALLINT_CORRECTION;
        }
        else {
            return originalNumber;
        }
    }

    /**
     * Convert original value insertion of type 'MEDIUMINT' into the correct MEDIUMINT UNSIGNED representation
     * Note: Unsigned MEDIUMINT (24-bit) is represented in 'Integer' 32-bit data type since the MAX value of
     * Unsigned MEDIUMINT 16777215 < Max value of Integer 2147483647
     *
     * @param originalNumber {@link Integer} the original insertion value
     * @return {@link Integer} the correct representation of the original insertion value
     */
    public static int convertUnsignedMediumint(int originalNumber) {
        if (originalNumber < 0) {
            return originalNumber + MEDIUMINT_CORRECTION;
        }
        else {
            return originalNumber;
        }
    }

    /**
     * Convert original value insertion of type 'INT' into the correct INT UNSIGNED representation
     * Note: Unsigned INT (32-bit) is represented in 'Long' 64-bit data type.
     * Reference: https://kafka.apache.org/0102/javadoc/org/apache/kafka/connect/data/Schema.Type.html
     *
     * @param originalNumber {@link Long} the original insertion value
     * @return {@link Long} the correct representation of the original insertion value
     */
    public static long convertUnsignedInteger(long originalNumber) {
        if (originalNumber < 0) {
            return originalNumber + INT_CORRECTION;
        }
        else {
            return originalNumber;
        }
    }

    /**
     * Convert original value insertion of type 'BIGINT' into the correct BIGINT UNSIGNED representation
     * Note: Unsigned BIGINT (64-bit) is represented in 'BigDecimal' data type. Reference:
     * https://kafka.apache.org/0102/javadoc/org/apache/kafka/connect/data/Schema.Type.html
     *
     * @param originalNumber {@link BigDecimal} the original insertion value
     * @return {@link BigDecimal} the correct representation of the original insertion value
     */
    public static BigDecimal convertUnsignedBigint(BigDecimal originalNumber) {
        if (originalNumber.compareTo(BigDecimal.ZERO) == -1) {
            return originalNumber.add(BIGINT_CORRECTION);
        }
        else {
            return originalNumber;
        }
    }

    private BinlogUnsignedIntegerConverter() {
    }
}
