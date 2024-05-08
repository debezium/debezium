/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.olr;

import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

import org.apache.kafka.connect.data.Field;

import io.debezium.DebeziumException;
import io.debezium.config.CommonConnectorConfig.BinaryHandlingMode;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleValueConverters;
import io.debezium.relational.Column;
import io.debezium.util.Strings;

import oracle.sql.INTERVALDS;
import oracle.sql.INTERVALYM;
import oracle.sql.RAW;

/**
 * Provides custom value converter behavior specific to OpenLogReplicator.
 *
 * OpenLogReplicator payloads are JSON-based and several value types are not serialized in the same format
 * as we would normally get them from a JDBC result set or an Oracle JDBC driver equivalent type. So, this
 * converter implementation is meant to pre-transform the values from OpenLogReplicator into a data type
 * that is more appropriate for the column being converted. This avoids polluting the base implementation
 * with any highly-specific OpenLogReplicator details.
 *
 * @author Chris Cranford
 */
public class OpenLogReplicatorValueConverter extends OracleValueConverters {

    private static final String COLUMN_TYPE_DATE = "DATE";
    private static final String COMMA = ",";
    private static final String PRECISION = "%precision%";
    private static final String TIMESTAMP_TIME_ZONE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss." + PRECISION + "xxxxx";
    private static final String TIMESTAMP_LOCAL_TIME_ZONE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss." + PRECISION + "XXXX";

    private final Map<Integer, DateTimeFormatter> timestampWithTimeZoneFormatterCache = new HashMap<>();
    private final Map<Integer, DateTimeFormatter> timestampWithLocalTimeZoneFormatterCache = new HashMap<>();

    public OpenLogReplicatorValueConverter(OracleConnectorConfig connectorConfig, OracleConnection connection) {
        super(connectorConfig, connection);
    }

    @Override
    protected Object convertNumeric(Column column, Field fieldDefn, Object value) {
        return super.convertNumeric(column, fieldDefn, toBigDecimal(column, fieldDefn, value));
    }

    @Override
    protected Object convertTimestampToEpochMillis(Column column, Field fieldDefn, Object value) {
        if (value instanceof Long) {
            value = convertTimestampValue(column, value);
        }
        return super.convertTimestampToEpochMillis(column, fieldDefn, value);
    }

    @Override
    protected Object convertTimestampToEpochMicros(Column column, Field fieldDefn, Object value) {
        if (value instanceof Long) {
            value = convertTimestampValue(column, value);
        }
        return super.convertTimestampToEpochMicros(column, fieldDefn, value);
    }

    @Override
    protected Object convertTimestampToEpochNanos(Column column, Field fieldDefn, Object value) {
        if (value instanceof Long) {
            value = convertTimestampValue(column, value);
        }
        return super.convertTimestampToEpochNanos(column, fieldDefn, value);
    }

    @Override
    protected Object convertTimestampToEpochMillisAsDate(Column column, Field fieldDefn, Object value) {
        if (value instanceof Long) {
            value = convertTimestampValue(column, value);
        }
        return super.convertTimestampToEpochMillisAsDate(column, fieldDefn, value);
    }

    @Override
    protected Object convertTimestampWithZone(Column column, Field fieldDefn, Object value) {
        if (value instanceof String) {
            final String valueStr = (String) value;
            if (!valueStr.contains(COMMA)) {
                throw new DebeziumException("Unexpected timestamp with time zone value: " + value);
            }

            // Split the timestamp with time zone value based on ','.
            // OpenLogReplicator provides the data in '<epoch>,<timezone>' format.
            final String[] valueBits = valueStr.split(",");

            final Instant instant = Instant.ofEpochSecond(0, Long.parseLong(valueBits[0]));
            final ZoneId zoneId = getZoneIdFromTimeZone(valueBits[1]);
            return getTimestampWithTimeZoneFormatter(column).format(OffsetDateTime.ofInstant(instant, zoneId));
        }
        return super.convertTimestampWithZone(column, fieldDefn, value);
    }

    @Override
    protected Object convertTimestampWithLocalZone(Column column, Field fieldDefn, Object value) {
        if (value instanceof Long) {
            final Instant instant = Instant.ofEpochSecond(0, (Long) value);
            return getTimestampWithLocalTimeZoneFormatter(column).format(OffsetDateTime.ofInstant(instant, ZoneOffset.UTC));
        }
        return super.convertTimestampWithLocalZone(column, fieldDefn, value);
    }

    @Override
    protected Object convertBinary(Column column, Field fieldDefn, Object value, BinaryHandlingMode mode) {
        if (value instanceof String) {
            try {
                value = RAW.hexString2Bytes((String) value);
            }
            catch (SQLException e) {
                throw new DebeziumException("Failed to convert HEX string into byte array: " + value, e);
            }
        }
        return super.convertBinary(column, fieldDefn, value, mode);
    }

    @Override
    protected Object convertIntervalYearMonth(Column column, Field fieldDefn, Object value) {
        if (value instanceof String) {
            value = new INTERVALYM((String) value);
        }
        return super.convertIntervalYearMonth(column, fieldDefn, value);
    }

    @Override
    protected Object convertIntervalDaySecond(Column column, Field fieldDefn, Object value) {
        if (value instanceof String) {
            // Values are separated by ",", we need them separated with spaces.
            final String sanitizedValue = ((String) value).replaceAll(",", " ");
            value = new INTERVALDS(sanitizedValue);
        }
        return super.convertIntervalDaySecond(column, fieldDefn, value);
    }

    private Object convertTimestampValue(Column column, Object value) {
        if (column.typeName().equalsIgnoreCase(COLUMN_TYPE_DATE)) {
            // Value is being provided in nanoseconds based on OpenLogReplicator configuration
            // We need to reduce the column's precision to milliseconds
            value = ((Long) value) / 1_000_000L;
        }
        else {
            // TIMESTAMP(n)
            value = Instant.ofEpochSecond(0, (Long) value);
        }
        return value;
    }

    private ZoneId getZoneIdFromTimeZone(String timeZone) {
        // OpenLogReplicator time zone varies, sometimes "HH:MM" and others Java TimeZone.
        if (timeZone.contains(":")) {
            return ZoneOffset.of(timeZone);
        }
        else {
            return TimeZone.getTimeZone(timeZone).toZoneId();
        }
    }

    private DateTimeFormatter getTimestampWithTimeZoneFormatter(Column column) {
        int precision = column.scale().orElse(6); // Oracle defaults to 6
        return timestampWithTimeZoneFormatterCache.computeIfAbsent(precision,
                k -> createPrecisionBasedFormatter(precision, TIMESTAMP_TIME_ZONE_FORMAT));
    }

    private DateTimeFormatter getTimestampWithLocalTimeZoneFormatter(Column column) {
        int precision = column.scale().orElse(6); // Oracle defaults to 6
        return timestampWithLocalTimeZoneFormatterCache.computeIfAbsent(precision,
                k -> createPrecisionBasedFormatter(precision, TIMESTAMP_LOCAL_TIME_ZONE_FORMAT));
    }

    private static DateTimeFormatter createPrecisionBasedFormatter(int precision, String format) {
        final String precisionFormat = Strings.pad("", precision, 'S');
        return DateTimeFormatter.ofPattern(format.replace(PRECISION, precisionFormat));
    }

}
