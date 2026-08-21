/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.type;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.util.TimeZone;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.jdbc.JdbcSinkConnectorConfig;
import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.sink.SinkConnectorConfig;
import io.debezium.time.ZonedTimestamp;

/**
 * An abstract base class for all temporal implementations of {@link JdbcType}.
 *
 * @author Chris Cranford
 */
public abstract class AbstractTemporalType extends AbstractType {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractTemporalType.class);

    private TimeZone databaseTimeZone;
    private boolean clampOutOfRangeValues;
    private boolean timestampBoundsResolved;
    private ZonedDateTime minimumTimestampValue;
    private ZonedDateTime maximumTimestampValue;

    @Override
    public void configure(SinkConnectorConfig config, DatabaseDialect dialect) {
        super.configure(config, dialect);

        if (config instanceof JdbcSinkConnectorConfig jdbcConfig) {
            this.clampOutOfRangeValues = jdbcConfig.isTimestampClampForOutOfRangeValuesEnabled();
        }

        final String databaseTimeZone = config.useTimeZone();
        try {
            this.databaseTimeZone = TimeZone.getTimeZone(ZoneId.of(databaseTimeZone));
        }
        catch (Exception e) {
            LOGGER.error("Failed to resolve time zone '{}', please specify a correct time zone value", databaseTimeZone, e);
            throw e;
        }
    }

    protected TimeZone getDatabaseTimeZone() {
        return databaseTimeZone;
    }

    /**
     * Clamps the supplied value to the dialect's minimum or maximum supported timestamp when the
     * value lies outside that range and {@code timestamp.clamp.out.of.range.values} is enabled;
     * otherwise the value is returned unchanged.
     */
    protected ZonedDateTime clampIfOutOfRange(ZonedDateTime value) {
        if (clampOutOfRangeValues) {
            final ZonedDateTime minimum = getMinimumTimestampValue();
            if (minimum != null && value.toInstant().isBefore(minimum.toInstant())) {
                return minimum;
            }
            final ZonedDateTime maximum = getMaximumTimestampValue();
            if (maximum != null && value.toInstant().isAfter(maximum.toInstant())) {
                return maximum;
            }
        }
        return value;
    }

    protected OffsetDateTime clampIfOutOfRange(OffsetDateTime value) {
        return clampIfOutOfRange(value.toZonedDateTime()).toOffsetDateTime();
    }

    protected LocalDateTime clampIfOutOfRange(LocalDateTime value) {
        if (clampOutOfRangeValues) {
            final ZonedDateTime minimum = getMinimumTimestampValue();
            if (minimum != null && value.isBefore(minimum.toLocalDateTime())) {
                return minimum.toLocalDateTime();
            }
            final ZonedDateTime maximum = getMaximumTimestampValue();
            if (maximum != null && value.isAfter(maximum.toLocalDateTime())) {
                return maximum.toLocalDateTime();
            }
        }
        return value;
    }

    private ZonedDateTime getMinimumTimestampValue() {
        if (!timestampBoundsResolved) {
            resolveTimestampBounds();
        }
        return minimumTimestampValue;
    }

    private ZonedDateTime getMaximumTimestampValue() {
        if (!timestampBoundsResolved) {
            resolveTimestampBounds();
        }
        return maximumTimestampValue;
    }

    private void resolveTimestampBounds() {
        minimumTimestampValue = parseTimestampBound(getDialect().getTimestampNegativeInfinityValue());
        maximumTimestampValue = parseTimestampBound(getDialect().getTimestampPositiveInfinityValue());
        timestampBoundsResolved = true;
    }

    private static ZonedDateTime parseTimestampBound(String value) {
        try {
            return ZonedDateTime.parse(value, ZonedTimestamp.FORMATTER);
        }
        catch (DateTimeParseException e) {
            // Dialects such as PostgreSQL represent infinity with markers the database understands
            // natively rather than a finite timestamp; such dialects have no bound to clamp against.
            return null;
        }
    }

}
