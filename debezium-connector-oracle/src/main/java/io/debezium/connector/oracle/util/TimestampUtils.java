/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.util;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Chris Cranford
 */
public final class TimestampUtils {

    private static final ZoneId GMT_ZONE_ID = ZoneId.of("GMT");

    private static final DateTimeFormatter TIMESTAMP_FORMATTER = new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .appendPattern("yyyy-MM-dd HH:mm:ss")
            .optionalStart()
            .appendPattern(".")
            .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, false)
            .optionalEnd()
            .toFormatter();

    private static final DateTimeFormatter TIMESTAMP_AM_PM_SHORT_FORMATTER = new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .appendPattern("dd-MMM-yy hh.mm.ss")
            .optionalStart()
            .appendPattern(".")
            .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, false)
            .optionalEnd()
            .appendPattern(" a")
            .toFormatter(Locale.ENGLISH);

    private static final Pattern TO_TIMESTAMP = Pattern.compile("TO_TIMESTAMP\\('(.*)'\\)", Pattern.CASE_INSENSITIVE);
    private static final Pattern TO_DATE = Pattern.compile("TO_DATE\\('(.*)',[ ]*'(.*)'\\)", Pattern.CASE_INSENSITIVE);

    /**
     * Convert the supplied timestamp without a timezone.
     *
     * @param value the string-value to be converted
     * @return the returned converted value or {@code null} if the value could not be converted
     */
    public static Instant convertTimestampNoZoneToInstant(String value) {
        final Matcher toTimestampMatcher = TO_TIMESTAMP.matcher(value);
        if (toTimestampMatcher.matches()) {
            final LocalDateTime dateTime;
            String text = toTimestampMatcher.group(1);
            if (text.indexOf(" AM") > 0 || text.indexOf(" PM") > 0) {
                dateTime = LocalDateTime.from(TIMESTAMP_AM_PM_SHORT_FORMATTER.parse(text.trim()));
            }
            else {
                dateTime = LocalDateTime.from(TIMESTAMP_FORMATTER.parse(text.trim()));
            }
            return dateTime.atZone(GMT_ZONE_ID).toInstant();
        }

        final Matcher toDateMatcher = TO_DATE.matcher(value);
        if (toDateMatcher.matches()) {
            return LocalDateTime.from(TIMESTAMP_FORMATTER.parse(toDateMatcher.group(1))).atZone(GMT_ZONE_ID).toInstant();
        }

        // Unable to resolve value
        return null;
    }

    /**
     * Converts the supplied string-value into a SQL compliant {@code TO_TIMESTAMP} string.
     *
     * @param value the string-value to be converted
     * @return the {@code TO_TIMESTAMP} function call
     */
    public static String toSqlCompliantFunctionCall(String value) {
        final Matcher timestampMatcher = TO_TIMESTAMP.matcher(value);
        if (timestampMatcher.matches()) {
            String text = timestampMatcher.group(1);
            if (text.indexOf(" AM") > 0 || text.indexOf(" PM") > 0) {
                return "TO_TIMESTAMP('" + text + "', 'YYYY-MM-DD HH24:MI:SS.FF A')";
            }
            return "TO_TIMESTAMP('" + text + "', 'YYYY-MM-DD HH24:MI:SS.FF')";
        }

        final Matcher dateMatcher = TO_DATE.matcher(value);
        if (dateMatcher.matches()) {
            // TO_DATE is already properly formatted.
            return value;
        }
        return null;
    }

    private TimestampUtils() {
    }
}
