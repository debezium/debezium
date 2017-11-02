/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

import java.math.BigDecimal;

/**
 * A set of Convert method.
 *
 * @author MaoXiang Pan
 */
public class NumberConversions {

    public static final Short SHORT_TRUE = new Short((short) 1);
    public static final Short SHORT_FALSE = new Short((short) 0);

    public static final Integer INTEGER_TRUE = new Integer(1);
    public static final Integer INTEGER_FALSE = new Integer(0);

    public static final Long LONG_TRUE = new Long(1L);
    public static final Long LONG_FALSE = new Long(0L);

    public static final Float FLOAT_TRUE = new Float(1.0);
    public static final Float FLOAT_FALSE = new Float(0.0);

    public static final Double DOUBLE_TRUE = new Double(1.0d);
    public static final Double DOUBLE_FALSE = new Double(0.0d);

    public static final byte[] BYTE_ZERO = new byte[0];

    public static final BigDecimal BIGDECIMAL_ZERO = BigDecimal.valueOf(0);
    public static final BigDecimal BIGDECIMAL_ONE = BigDecimal.valueOf(1);

    /**
     * Convert boolean object to bigDecimal object.
     *
     * @param data a boolean object
     * @return bigDecimal 0 or 1
     * @throws NullPointerException If {@code data} is {@code null}
     */
    public static BigDecimal getBigDecimal(Boolean data) {
        return data.booleanValue() ? BIGDECIMAL_ONE : BIGDECIMAL_ZERO;
    }

    /**
     * Convert boolean object to short object.
     *
     * @param data A boolean object
     * @return Short 0 or 1
     * @throws NullPointerException If {@code data} is {@code null}
     */
    public static Short getShort(Boolean data) {
        return data.booleanValue() ? SHORT_TRUE : SHORT_FALSE;
    }

    /**
     * Convert boolean object to Integer
     *
     * @param data A boolean object
     * @return Integer 0 or 1
     * @throws NullPointerException If {@code data} is {@code null}
     */
    public static Integer getInteger(Boolean data) {
        return data.booleanValue() ? INTEGER_TRUE : INTEGER_FALSE;
    }

    /**
     * Convert boolean object to long object.
     *
     * @param data A boolean object
     * @return Long 0 or 1
     * @throws NullPointerException If {@code data} is {@code null}
     */
    public static Long getLong(Boolean data) {
        return data.booleanValue() ? LONG_TRUE : LONG_FALSE;
    }

    /**
     * Convert boolean object to float object
     *
     * @param data A boolean object.
     * @return Float 0.0 or 1.0
     * @throws NullPointerException If {@code data} is {@code null}
     */
    public static Float getFloat(Boolean data) {
        return data.booleanValue() ? FLOAT_TRUE : FLOAT_FALSE;
    }

    /**
     * Convert boolean object to double object
     *
     * @param data A boolean object.
     * @return Double 0.0 or 1.0
     * @throws NullPointerException If {@code data} is {@code null}
     */
    public static Double getDouble(Boolean data) {
        return data.booleanValue() ? DOUBLE_TRUE : DOUBLE_FALSE;
    }
}
