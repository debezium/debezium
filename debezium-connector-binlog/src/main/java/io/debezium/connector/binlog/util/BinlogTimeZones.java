/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog.util;

import java.sql.Types;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.debezium.relational.ColumnEditor;
import io.debezium.relational.SystemVariables;

/**
 * Interprets {@code TIMESTAMP} default literals of MySQL and MariaDB in the time zone of the session that runs the DDL
 * statement, as the servers do. The session time zone is tracked as the {@code time_zone} system variable of the DDL
 * parser, and {@code SYSTEM} refers to the time zone of the server, which the connector records as the
 * {@code system_time_zone} system variable once it has resolved it.
 */
public final class BinlogTimeZones {

    public static final String TIME_ZONE = "time_zone";
    public static final String SYSTEM_TIME_ZONE = "system_time_zone";

    private static final String SYSTEM = "SYSTEM";
    private static final Pattern OFFSET = Pattern.compile("([+-])(\\d{1,2}):(\\d{2})");
    private static final Pattern LOCAL_TIMESTAMP = Pattern.compile("(\\d{4})-(\\d{2})-(\\d{2})[ T](\\d{2}):(\\d{2}):(\\d{2})(\\.(\\d{1,6}))?");
    private static final DateTimeFormatter UTC_LITERAL = new DateTimeFormatterBuilder()
            .appendPattern("yyyy-MM-dd HH:mm:ss")
            .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
            .toFormatter();

    private BinlogTimeZones() {
    }

    /**
     * Resolves a time zone as MySQL and MariaDB name it: an offset such as {@code +09:00}, or a named time zone such as
     * {@code Asia/Seoul}.
     *
     * @param value the time zone; may be null
     * @return the time zone, or null if it is not known or cannot be resolved
     */
    public static ZoneId parse(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        final String timeZone = value.trim();
        final Matcher offset = OFFSET.matcher(timeZone);
        try {
            if (offset.matches()) {
                return ZoneOffset.ofHoursMinutes(Integer.parseInt(offset.group(1) + offset.group(2)),
                        Integer.parseInt(offset.group(1) + offset.group(3)));
            }
            return ZoneId.of(timeZone);
        }
        catch (DateTimeException e) {
            return null;
        }
    }

    /**
     * Resolves the time zone of the server, which applies to sessions with the time zone {@code SYSTEM}. As for the
     * conversions of the JDBC driver, a time zone configured for the driver is assumed to be the time zone of the server
     * and takes precedence over the system time zone that the server reports, which may be an abbreviation such as
     * {@code KST} that cannot be resolved. A configured value such as {@code SERVER} or {@code auto}, with which the driver
     * uses the time zone of the server, is not a time zone.
     *
     * @param configuredTimeZone the time zone configured for the JDBC driver; may be null
     * @param systemTimeZone the {@code system_time_zone} of the server; may be null
     * @return the time zone of the server, or null if it cannot be resolved
     */
    public static ZoneId serverTimeZone(String configuredTimeZone, String systemTimeZone) {
        final ZoneId configured = parse(configuredTimeZone);
        return configured != null ? configured : parse(systemTimeZone);
    }

    /**
     * Returns the time zone of the session that runs the DDL statements being parsed.
     *
     * @param variables the system variables of the DDL parser; never null
     * @return the time zone, or null if it is not known
     */
    public static ZoneId sessionTimeZone(SystemVariables variables) {
        final String timeZone = variables.getVariable(TIME_ZONE);
        if (SYSTEM.equalsIgnoreCase(timeZone)) {
            return parse(variables.getVariable(SYSTEM_TIME_ZONE));
        }
        return parse(timeZone);
    }

    /**
     * Converts a {@code TIMESTAMP} default literal to an absolute literal, which carries the offset of UTC, so that the
     * default no longer depends on the time zone in which it is read. Literals of other columns, literals in an unknown
     * time zone, and literals that are not a valid date and time are returned unchanged.
     *
     * @param column the column; never null
     * @param literal the default literal; may be null
     * @param variables the system variables of the DDL parser; never null
     * @return the default literal
     */
    public static String toDefaultValueExpression(ColumnEditor column, String literal, SystemVariables variables) {
        if (literal == null || column.jdbcType() != Types.TIMESTAMP_WITH_TIMEZONE) {
            return literal;
        }
        final ZoneId timeZone = sessionTimeZone(variables);
        final Matcher matcher = LOCAL_TIMESTAMP.matcher(literal.trim());
        if (timeZone == null || !matcher.matches()) {
            return literal;
        }
        try {
            final LocalDateTime local = LocalDateTime.of(Integer.parseInt(matcher.group(1)), Integer.parseInt(matcher.group(2)),
                    Integer.parseInt(matcher.group(3)), Integer.parseInt(matcher.group(4)), Integer.parseInt(matcher.group(5)),
                    Integer.parseInt(matcher.group(6)), nanos(matcher.group(8)));
            final Instant instant = local.atZone(timeZone).toInstant();
            return UTC_LITERAL.format(LocalDateTime.ofInstant(instant, ZoneOffset.UTC)) + "+00:00";
        }
        catch (DateTimeException e) {
            // For example a zero date, which the default value converter handles
            return literal;
        }
    }

    private static int nanos(String fraction) {
        if (fraction == null) {
            return 0;
        }
        return Integer.parseInt((fraction + "000000000").substring(0, 9));
    }
}
