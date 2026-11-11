/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.converters;

import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.HexFormat;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Predicate;

import org.apache.kafka.connect.data.SchemaBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.config.Field;
import io.debezium.connector.oracle.OracleValueConverters;
import io.debezium.connector.oracle.util.OracleZeroDates;
import io.debezium.connector.oracle.util.TimestampUtils;
import io.debezium.function.Predicates;
import io.debezium.metadata.ConfigDescriptor;
import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;
import io.debezium.time.Conversions;
import io.debezium.time.MicroTimestamp;
import io.debezium.time.NanoTimestamp;
import io.debezium.time.Timestamp;
import io.debezium.util.Strings;

import oracle.sql.DATE;
import oracle.sql.TIMESTAMP;

/**
 * Replaces Oracle zero dates ({@code 0000-00-00 00:00:00}), which some OCI clients can store in {@code DATE}
 * and {@code TIMESTAMP} columns, with either {@code null} or a user-chosen literal, per column type and
 * optionally per column.<p></p>
 *
 * Without this converter, the connector emits a zero date as an absent value: {@code null} for an optional
 * column, the column default for a {@code NOT NULL} column with a default, and the epoch otherwise.<p></p>
 *
 * <h2>Supported types</h2>
 * <ul>
 *   <li>{@code DATE} &rarr; {@code io.debezium.time.Timestamp} (INT64 epoch millis)</li>
 *   <li>{@code TIMESTAMP(0-3)} &rarr; {@code io.debezium.time.Timestamp} (INT64 epoch millis)</li>
 *   <li>{@code TIMESTAMP(4-6)} &rarr; {@code io.debezium.time.MicroTimestamp} (INT64 epoch micros)</li>
 *   <li>{@code TIMESTAMP(7-9)} &rarr; {@code io.debezium.time.NanoTimestamp} (INT64 epoch nanos)</li>
 * </ul>
 * The converter does not consult {@code time.precision.mode}.
 *
 * <h2>Policies</h2>
 * <ul>
 *   <li><b>NULL policy</b>: zero dates are emitted as {@code null} and the column schema is made optional.
 *       Active when the fallback is {@code NULL} (case-insensitive) or unset.</li>
 *   <li><b>User-value policy</b>: zero dates, {@code null} values and the schema default are set to the configured
 *       literal; the schema stays required for {@code NOT NULL} columns. Any DDL {@code DEFAULT} on the column is
 *       overridden by this value.</li>
 * </ul>
 * Resolution order: column-level override, then type-level fallback, then {@code NULL}.<p></p>
 *
 * A zero date can reach the converter either in its raw form (LogMiner) or already as {@code null} (snapshots
 * and XStream), so the converter replaces every {@code null} in a selected column with the fallback, in every
 * capture mode. With a non-{@code NULL} fallback, genuine {@code NULL} values in optional columns are therefore
 * replaced as well, and the default selector {@code .*} applies this to every {@code DATE} or {@code TIMESTAMP}
 * column; restrict the converter with the {@code selector} to {@code NOT NULL} columns when this is not wanted.
 * The schema stays required for a non-{@code NULL} fallback only if the column is {@code NOT NULL}.<p></p>
 *
 * As with the binlog connectors' converter, the first call of the conversion function on a column with a
 * default value is assumed to resolve the schema default value at schema-build time.
 *
 * @author Chris Cranford
 */
public class ZeroDateFallbackConverter implements CustomConverter<SchemaBuilder, RelationalColumn>, ConfigDescriptor {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZeroDateFallbackConverter.class);

    private static final DateTimeFormatter FALLBACK_FORMATTER = new DateTimeFormatterBuilder()
            .appendPattern("yyyy-MM-dd")
            .optionalStart()
            .appendPattern(" HH:mm:ss")
            .optionalStart()
            .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
            .optionalEnd()
            .optionalEnd()
            .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
            .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
            .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0)
            .toFormatter(Locale.ENGLISH);

    private static final String NULL_TOKEN = "NULL";
    private static final String TYPE_DATE = "DATE";
    private static final String TYPE_TIMESTAMP = "TIMESTAMP";
    private static final int DEFAULT_TIMESTAMP_PRECISION = 6;

    private boolean applyToDate;
    private boolean applyToTimestamp;
    private LocalDateTime fallbackDate;
    private LocalDateTime fallbackTimestamp;
    private Predicate<RelationalColumn> selector = x -> true;
    private Properties rawProps;

    @Override
    public void configure(Properties props) {
        this.rawProps = props;

        final String targets = props.getProperty(ZeroDateFallbackConverterConfig.TARGET_TYPES.name(),
                ZeroDateFallbackConverterConfig.DEFAULT_TARGET_TYPES).toUpperCase(Locale.ROOT);
        final Set<String> tokens = Strings.setOfTrimmed(targets, ',', s -> s.isEmpty() ? null : s);
        this.applyToDate = tokens.contains(TYPE_DATE);
        this.applyToTimestamp = tokens.contains(TYPE_TIMESTAMP);

        this.fallbackDate = parseFallback(props.getProperty(ZeroDateFallbackConverterConfig.FALLBACK_DATE.name()));
        this.fallbackTimestamp = parseFallback(props.getProperty(ZeroDateFallbackConverterConfig.FALLBACK_TIMESTAMP.name()));

        final String selectorConfig = props.getProperty(ZeroDateFallbackConverterConfig.SELECTOR.name(),
                ZeroDateFallbackConverterConfig.DEFAULT_SELECTOR);
        if (!Strings.isNullOrBlank(selectorConfig) && !ZeroDateFallbackConverterConfig.DEFAULT_SELECTOR.equals(selectorConfig.trim())) {
            this.selector = Predicates.includes(selectorConfig.trim(), x -> x.dataCollection() + "." + x.name());
        }

        LOGGER.info("ZeroDateFallbackConverter configured: targets=[DATE={}, TIMESTAMP={}], fallback=[date={}, timestamp={}], selector='{}'",
                applyToDate, applyToTimestamp, describe(fallbackDate), describe(fallbackTimestamp), selectorConfig);
    }

    @Override
    public void converterFor(RelationalColumn field, ConverterRegistration<SchemaBuilder> registration) {
        if (!selector.test(field)) {
            return;
        }

        final String typeName = field.typeName() != null ? field.typeName().toUpperCase(Locale.ROOT) : "";
        if (TYPE_DATE.equals(typeName) && applyToDate) {
            register(field, registration, fallbackDate, 0);
        }
        else if (field.jdbcType() == Types.TIMESTAMP && typeName.startsWith(TYPE_TIMESTAMP) && !typeName.contains("TIME ZONE") && applyToTimestamp) {
            register(field, registration, fallbackTimestamp, getTimePrecision(field));
        }
    }

    @Override
    public Field.Set getConfigFields() {
        return Field.setOf(
                ZeroDateFallbackConverterConfig.TARGET_TYPES,
                ZeroDateFallbackConverterConfig.FALLBACK_DATE,
                ZeroDateFallbackConverterConfig.FALLBACK_TIMESTAMP,
                ZeroDateFallbackConverterConfig.SELECTOR);
    }

    private void register(RelationalColumn field, ConverterRegistration<SchemaBuilder> registration, LocalDateTime typeFallback, int precision) {
        final String columnRaw = lookupColumnFallback(field);
        final LocalDateTime effective = columnRaw != null ? parseFallback(columnRaw) : typeFallback;
        final boolean hasDefault = field.hasDefaultValue() && field.defaultValue() != null;

        final SchemaBuilder schema;
        final Function<Instant, Long> toEpoch;
        if (precision <= 3) {
            schema = Timestamp.builder();
            toEpoch = Instant::toEpochMilli;
        }
        else if (precision <= 6) {
            schema = MicroTimestamp.builder();
            toEpoch = Conversions::toEpochMicros;
        }
        else {
            schema = NanoTimestamp.builder();
            toEpoch = Conversions::toEpochNanos;
        }

        final Long fallback = effective == null ? null : toEpoch.apply(effective.toInstant(ZoneOffset.UTC));
        if (fallback == null) {
            schema.optional();
        }

        LOGGER.debug("Registered zero-date converter on '{}.{}' source={} hasDefault={} fallback={}",
                field.dataCollection(), field.name(), columnRaw != null ? "column-level" : "type-level", hasDefault, describe(fallback));

        final AtomicBoolean firstCallSeen = new AtomicBoolean(false);
        registration.register(schema, input -> {
            if (!firstCallSeen.getAndSet(true) && hasDefault) {
                // Resolves the schema default value
                return fallback;
            }
            if (input == null || OracleZeroDates.isZeroDate(input)) {
                return fallback;
            }
            final Instant instant = toInstant(input);
            if (instant == null) {
                LOGGER.warn("Unexpected value of type {} on field '{}.{}', returning fallback",
                        input.getClass().getName(), field.dataCollection(), field.name());
                return fallback;
            }
            return toEpoch.apply(instant);
        });
    }

    private static Instant toInstant(Object input) {
        try {
            if (input instanceof LocalDateTime localDateTime) {
                return localDateTime.toInstant(ZoneOffset.UTC);
            }
            if (input instanceof DATE date) {
                return date.toLocalDateTime().toInstant(ZoneOffset.UTC);
            }
            if (input instanceof TIMESTAMP timestamp) {
                return timestamp.toLocalDateTime().toInstant(ZoneOffset.UTC);
            }
            if (input instanceof java.sql.Timestamp timestamp) {
                return timestamp.toLocalDateTime().toInstant(ZoneOffset.UTC);
            }
            if (input instanceof Instant instant) {
                return instant;
            }
            if (input instanceof Long nanos) {
                // OpenLogReplicator provides epoch nanoseconds
                return Instant.ofEpochSecond(0, nanos);
            }
            if (input instanceof String text) {
                if (OracleValueConverters.isHexToRawFunctionCall(text)) {
                    final byte[] bytes = HexFormat.of().parseHex(OracleValueConverters.getHexToRawHexString(text));
                    return new TIMESTAMP(bytes).toLocalDateTime().toInstant(ZoneOffset.UTC);
                }
                return TimestampUtils.convertTimestampNoZoneToInstant(text);
            }
        }
        catch (SQLException e) {
            throw new DebeziumException("Failed to convert value " + input, e);
        }
        return null;
    }

    private static int getTimePrecision(RelationalColumn field) {
        // Oracle stores the fractional-second precision in scale (DDL) or length (JDBC metadata)
        if (field.scale().isPresent()) {
            return field.scale().getAsInt();
        }
        return field.length().orElse(DEFAULT_TIMESTAMP_PRECISION);
    }

    private String lookupColumnFallback(RelationalColumn field) {
        if (rawProps == null) {
            return null;
        }
        return rawProps.getProperty(ZeroDateFallbackConverterConfig.COLUMN_FALLBACK_PREFIX
                + field.dataCollection() + "." + field.name()
                + ZeroDateFallbackConverterConfig.COLUMN_FALLBACK_SUFFIX);
    }

    static LocalDateTime parseFallback(String raw) {
        if (Strings.isNullOrBlank(raw) || NULL_TOKEN.equalsIgnoreCase(raw.trim())) {
            return null;
        }
        return LocalDateTime.parse(raw.trim(), FALLBACK_FORMATTER);
    }

    private static String describe(Object value) {
        return value == null ? NULL_TOKEN : value.toString();
    }
}
