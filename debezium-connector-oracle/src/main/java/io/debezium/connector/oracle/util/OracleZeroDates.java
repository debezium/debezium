/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.util;

import java.util.HexFormat;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.debezium.connector.oracle.OracleValueConverters;

import oracle.sql.DATE;
import oracle.sql.Datum;
import oracle.sql.TIMESTAMP;
import oracle.sql.TIMESTAMPLTZ;
import oracle.sql.TIMESTAMPTZ;

/**
 * Utility methods for detecting Oracle zero dates.<p></p>
 *
 * Oracle does not allow storing a {@code DATE} or {@code TIMESTAMP} (with or without a time zone) with a month
 * or day of {@code 0} via SQL,
 * but some OCI clients can write such values directly in their internal form, e.g. {@code 0000-00-00 00:00:00}
 * is stored as {@code 100,100,0,0,1,1,1}. These values cannot be represented by {@code java.time}, so they
 * must be detected from their raw representation before conversion.<p></p>
 *
 * Only a zero month or day denotes a zero date. A zero year with a valid month and day (Oracle year 1 BC
 * represented by {@code TO_DATE('0000-...')}) is a legal value and is not treated as a zero date.
 *
 * @author Chris Cranford
 */
public final class OracleZeroDates {

    /**
     * Oracle's internal {@code DATE} representation of {@code 0000-00-00 00:00:00}.
     */
    public static final byte[] ZERO_DATE_BYTES = new byte[]{ 100, 100, 0, 0, 1, 1, 1 };

    /**
     * Oracle's internal {@code TIMESTAMP} representation of {@code 0000-00-00 00:00:00.000000000}.
     */
    public static final byte[] ZERO_TIMESTAMP_BYTES = new byte[]{ 100, 100, 0, 0, 1, 1, 1, 0, 0, 0, 0 };

    /**
     * Oracle's internal {@code TIMESTAMP WITH TIME ZONE} representation of {@code 0000-00-00 00:00:00.000000000 +00:00}.
     */
    public static final byte[] ZERO_TIMESTAMP_TZ_BYTES = new byte[]{ 100, 100, 0, 0, 1, 1, 1, 0, 0, 0, 0, 20, 60 };

    private static final int MONTH_BYTE = 2;
    private static final int DAY_BYTE = 3;

    private static final Pattern TO_DATE_OR_TIMESTAMP = Pattern.compile("TO_(?:DATE|TIMESTAMP|TIMESTAMP_TZ)\\('([^']*)'.*\\)", Pattern.CASE_INSENSITIVE);
    private static final Pattern DATE_TEXT = Pattern.compile("^\\s*-?\\d{1,4}-(\\d{1,2})-(\\d{1,2})(?:\\D.*)?$");

    /**
     * Returns whether the Oracle internal {@code DATE} (7 bytes), {@code TIMESTAMP} or {@code TIMESTAMP WITH LOCAL
     * TIME ZONE} (11 bytes), or {@code TIMESTAMP WITH TIME ZONE} (13 bytes) representation is a zero date, i.e.
     * its month or day is {@code 0}.
     *
     * @param bytes the raw value bytes, may be {@code null}
     * @return {@code true} if the bytes represent a zero date, {@code false} otherwise
     */
    public static boolean isZeroDate(byte[] bytes) {
        if (bytes == null || (bytes.length != 7 && bytes.length != 11 && bytes.length != 13)) {
            return false;
        }
        return bytes[MONTH_BYTE] == 0 || bytes[DAY_BYTE] == 0;
    }

    /**
     * Returns whether the supplied value is an Oracle zero date. Supports the raw byte form, the
     * {@link DATE}, {@link TIMESTAMP}, {@link TIMESTAMPTZ} and {@link TIMESTAMPLTZ} driver types, and the LogMiner
     * {@code TO_DATE}, {@code TO_TIMESTAMP}, {@code TO_TIMESTAMP_TZ} and {@code HEXTORAW} literal forms. All other
     * values return {@code false}.
     *
     * @param value the value to inspect, may be {@code null}
     * @return {@code true} if the value represents a zero date, {@code false} otherwise
     */
    public static boolean isZeroDate(Object value) {
        if (value instanceof byte[] bytes) {
            return isZeroDate(bytes);
        }
        else if (isTemporalDatum(value)) {
            return isZeroDate(((Datum) value).getBytes());
        }
        else if (value instanceof String text) {
            return isZeroDateLiteral(text);
        }
        return false;
    }

    /**
     * Returns {@code null} if the supplied {@link DATE}, {@link TIMESTAMP}, {@link TIMESTAMPTZ} or
     * {@link TIMESTAMPLTZ} driver value is a zero date, so that the absent-value rules of custom converters and
     * value converters apply; otherwise returns the value as-is.
     *
     * @param value the value to inspect, may be {@code null}
     * @return {@code null} if the value is a zero date driver value, otherwise the supplied value
     */
    public static Object nullIfZeroDate(Object value) {
        if (isTemporalDatum(value) && isZeroDate(value)) {
            return null;
        }
        return value;
    }

    private static boolean isTemporalDatum(Object value) {
        // Other Datum types, such as NUMBER, can have the same length, so only temporal types are inspected
        return value instanceof DATE || value instanceof TIMESTAMP || value instanceof TIMESTAMPTZ || value instanceof TIMESTAMPLTZ;
    }

    /**
     * Returns whether the LogMiner literal is a zero date.
     *
     * @param value the {@code TO_DATE('...')}, {@code TO_TIMESTAMP('...')}, {@code TO_TIMESTAMP_TZ('...')} or
     *              {@code HEXTORAW('...')} literal
     * @return {@code true} if the literal represents a zero date, {@code false} otherwise
     */
    public static boolean isZeroDateLiteral(String value) {
        if (value == null) {
            return false;
        }
        if (OracleValueConverters.isHexToRawFunctionCall(value)) {
            try {
                return isZeroDate(HexFormat.of().parseHex(OracleValueConverters.getHexToRawHexString(value)));
            }
            catch (IllegalArgumentException e) {
                return false;
            }
        }
        final Matcher matcher = TO_DATE_OR_TIMESTAMP.matcher(value.trim());
        return matcher.matches() && isZeroDateText(matcher.group(1));
    }

    /**
     * Returns whether the {@code yyyy-MM-dd...} date text has a zero month or day.
     *
     * @param text the date text, may be {@code null}
     * @return {@code true} if the month or day is {@code 0}, {@code false} otherwise
     */
    public static boolean isZeroDateText(String text) {
        if (text == null) {
            return false;
        }
        final Matcher matcher = DATE_TEXT.matcher(text);
        return matcher.matches() && (Integer.parseInt(matcher.group(1)) == 0 || Integer.parseInt(matcher.group(2)) == 0);
    }

    private OracleZeroDates() {
    }
}
