/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog.converters;

import java.sql.Date;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;

import org.apache.kafka.connect.data.SchemaBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Field;
import io.debezium.function.Predicates;
import io.debezium.metadata.ConfigDescriptor;
import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;
import io.debezium.util.Strings;

/**
 * Replaces MySQL/MariaDB zero-date sentinels ({@code 0000-00-00} / {@code 0000-00-00 00:00:00})
 * with either {@code null} or a user-chosen literal, per JDBC type and optionally per column.
 *
 * <h2>Supported types</h2>
 * <ul>
 *   <li>{@code DATE} &rarr; {@code io.debezium.time.Date} (INT32 epoch days)</li>
 *   <li>{@code DATETIME(0-3)} &rarr; {@code io.debezium.time.Timestamp} (INT64 epoch millis)</li>
 *   <li>{@code DATETIME(4-6)} &rarr; {@code io.debezium.time.MicroTimestamp} (INT64 epoch micros)</li>
 *   <li>{@code TIMESTAMP} &rarr; {@code io.debezium.time.ZonedTimestamp} (STRING ISO-8601)</li>
 * </ul>
 *
 * The {@code DATETIME} semantic type follows the column's fractional-second precision
 * ({@link RelationalColumn#length()}): {@code 0-3} maps to {@code Timestamp} (millis) and
 * {@code 4-6} to {@code MicroTimestamp} (micros). The converter does not consult
 * {@code time.precision.mode}.
 *
 * <h2>Two policies, selectable per type</h2>
 * <ul>
 *   <li><b>NULL policy</b> &mdash; the row and the column schema's {@code defaultValue} slot both
 *       become {@code null}. The schema is promoted to nullable. Active when the fallback option
 *       is {@code NULL} (case-insensitive) or unset (default).</li>
 *   <li><b>User-value policy</b> &mdash; the row and the column schema's {@code defaultValue} are
 *       set to the configured literal. Schema stays NOT NULL. Any DDL {@code DEFAULT} on the
 *       column is overridden by this value.</li>
 * </ul>
 *
 * <h2>Resolution order</h2>
 * Column-level override &gt; type-level fallback &gt; NULL.
 *
 * <h2>Configuration</h2>
 * <ul>
 *   <li>{@code <prefix>.target.types} &mdash; comma-separated subset of
 *       {@code DATE,DATETIME,TIMESTAMP}. Default applies to all three.</li>
 *   <li>{@code <prefix>.fallback.date} / {@code .fallback.datetime} / {@code .fallback.timestamp}
 *       &mdash; per-type replacement. {@code NULL} (default) or a type-appropriate literal.</li>
 *   <li>{@code <prefix>.column.<db>.<table>.<column>.fallback} &mdash; column-specific override of
 *       the type-level value. Look-up key is built from
 *       {@code field.dataCollection() + "." + field.name()}.</li>
 *   <li>{@code <prefix>.selector} &mdash; comma-separated regex applied to
 *       {@code <db>.<table>.<column>}. Default {@code .*} (every matching-type column).</li>
 * </ul>
 *
 * <h2>How it interacts with DDL defaults</h2>
 * The implementation relies on {@code TableSchemaBuilder} invoking the registered conversion
 * lambda exactly once at schema-build time (for resolving the column's {@code defaultValue}
 * slot). A per-column {@link AtomicBoolean} guards that first call:
 * <ul>
 *   <li>Under the NULL policy, the lambda returns {@code null} on the first call, which leaves
 *       the schema's {@code defaultValue} slot empty so AvroConverter does not back-fill null rows
 *       with the DDL default.</li>
 *   <li>Under the user-value policy, the lambda returns the configured literal on the first call,
 *       so the schema's {@code defaultValue} adopts the user's sentinel regardless of the DDL
 *       default.</li>
 * </ul>
 * Row-level emission is unaffected: for every row event the lambda inspects {@code input == null}
 * to distinguish zero-date rows (collapsed to {@code null} by the binlog deserializer / JDBC
 * driver) from genuine values, so a real {@code 1970-01-01 00:00:00 UTC} row still emits {@code 0}
 * even when the converter is enabled. This means false positives are not possible at the row
 * level.
 * <p>
 * The first-call behavior depends on Debezium internals rather than a published SPI contract and
 * may need adjustment if {@code TableSchemaBuilder} ever invokes the lambda a different number of
 * times during schema build.
 *
 * @author minleejae
 */
public class ZeroDateFallbackConverter
        implements CustomConverter<SchemaBuilder, RelationalColumn>, ConfigDescriptor {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZeroDateFallbackConverter.class);

    private static final DateTimeFormatter DATETIME_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final String NULL_TOKEN = "NULL";
    private static final String TYPE_DATE = "DATE";
    private static final String TYPE_DATETIME = "DATETIME";
    private static final String TYPE_TIMESTAMP = "TIMESTAMP";
    private static final int SCHEMA_VERSION = 1;

    private boolean applyToDate;
    private boolean applyToDatetime;
    private boolean applyToTimestamp;

    private LocalDate fallbackDate;
    private LocalDateTime fallbackDatetime;
    private OffsetDateTime fallbackTimestamp;

    private Predicate<RelationalColumn> selector = x -> true;
    private Properties rawProps;

    @Override
    public void configure(Properties props) {
        this.rawProps = props;

        final String targets = props.getProperty(
                ZeroDateFallbackConverterConfig.TARGET_TYPES.name(),
                ZeroDateFallbackConverterConfig.DEFAULT_TARGET_TYPES).toUpperCase();
        final Set<String> tokens = parseCsv(targets);
        this.applyToDate = tokens.contains(TYPE_DATE);
        this.applyToDatetime = tokens.contains(TYPE_DATETIME);
        this.applyToTimestamp = tokens.contains(TYPE_TIMESTAMP);

        this.fallbackDate = parseDateFallback(props.getProperty(
                ZeroDateFallbackConverterConfig.FALLBACK_DATE.name()));
        this.fallbackDatetime = parseDatetimeFallback(props.getProperty(
                ZeroDateFallbackConverterConfig.FALLBACK_DATETIME.name()));
        this.fallbackTimestamp = parseTimestampFallback(props.getProperty(
                ZeroDateFallbackConverterConfig.FALLBACK_TIMESTAMP.name()));

        final String selectorConfig = props.getProperty(
                ZeroDateFallbackConverterConfig.SELECTOR.name(),
                ZeroDateFallbackConverterConfig.DEFAULT_SELECTOR);
        if (!Strings.isNullOrBlank(selectorConfig) && !ZeroDateFallbackConverterConfig.DEFAULT_SELECTOR.equals(selectorConfig.trim())) {
            this.selector = Predicates.includes(selectorConfig.trim(), x -> x.dataCollection() + "." + x.name());
        }

        LOGGER.info("ZeroDateFallbackConverter configured: targets=[DATE={}, DATETIME={}, TIMESTAMP={}], "
                + "fallback=[date={}, datetime={}, timestamp={}], selector='{}'",
                applyToDate, applyToDatetime, applyToTimestamp,
                describe(fallbackDate), describe(fallbackDatetime), describe(fallbackTimestamp),
                selectorConfig);
    }

    @Override
    public void converterFor(RelationalColumn field, ConverterRegistration<SchemaBuilder> registration) {
        if (!selector.test(field)) {
            return;
        }

        final int jdbcType = field.jdbcType();
        final String typeName = field.typeName() != null ? field.typeName().toUpperCase() : "";

        if (jdbcType == Types.DATE && applyToDate) {
            registerDate(field, registration);
        }
        else if (jdbcType == Types.TIMESTAMP && applyToDatetime && TYPE_DATETIME.equals(typeName)) {
            registerDatetime(field, registration);
        }
        else if ((jdbcType == Types.TIMESTAMP_WITH_TIMEZONE || TYPE_TIMESTAMP.equals(typeName)) && applyToTimestamp) {
            registerTimestamp(field, registration);
        }
        else {
            LOGGER.debug("Skipping field '{}.{}' jdbcType={} typeName='{}'",
                    field.dataCollection(), field.name(), jdbcType, typeName);
        }
    }

    @Override
    public Field.Set getConfigFields() {
        return Field.setOf(
                ZeroDateFallbackConverterConfig.TARGET_TYPES,
                ZeroDateFallbackConverterConfig.FALLBACK_DATE,
                ZeroDateFallbackConverterConfig.FALLBACK_DATETIME,
                ZeroDateFallbackConverterConfig.FALLBACK_TIMESTAMP,
                ZeroDateFallbackConverterConfig.SELECTOR);
    }

    private void registerDate(RelationalColumn field, ConverterRegistration<SchemaBuilder> registration) {
        final String columnRaw = lookupColumnFallback(field);
        final LocalDate effective = (columnRaw != null) ? parseDateFallback(columnRaw) : fallbackDate;
        final Integer fallback = effective == null ? null : (int) effective.toEpochDay();
        final boolean hasDefault = field.hasDefaultValue();

        SchemaBuilder schema = SchemaBuilder.int32().name(io.debezium.time.Date.SCHEMA_NAME).version(SCHEMA_VERSION);
        if (fallback == null) {
            schema = schema.optional();
        }
        logRegistration(TYPE_DATE, field, columnRaw, hasDefault, fallback);

        final AtomicBoolean firstCallSeen = new AtomicBoolean(false);
        registration.register(schema, input -> {
            if (!firstCallSeen.getAndSet(true) && hasDefault) {
                return fallback;
            }
            if (input == null) {
                return fallback;
            }
            if (input instanceof Date date) {
                return (int) date.toLocalDate().toEpochDay();
            }
            if (input instanceof LocalDate ld) {
                return (int) ld.toEpochDay();
            }
            if (input instanceof Integer i) {
                return i;
            }
            LOGGER.warn("Unexpected input type {} on DATE field '{}.{}', returning fallback",
                    input.getClass().getName(), field.dataCollection(), field.name());
            return fallback;
        });
    }

    private void registerDatetime(RelationalColumn field, ConverterRegistration<SchemaBuilder> registration) {
        final String columnRaw = lookupColumnFallback(field);
        final LocalDateTime effective = (columnRaw != null) ? parseDatetimeFallback(columnRaw) : fallbackDatetime;
        final boolean hasDefault = field.hasDefaultValue();
        final boolean isMicro = field.length().isPresent() && field.length().getAsInt() >= 4;

        final String schemaName = isMicro
                ? io.debezium.time.MicroTimestamp.SCHEMA_NAME
                : io.debezium.time.Timestamp.SCHEMA_NAME;
        final Long fallback = effective == null ? null
                : isMicro
                        ? io.debezium.time.Conversions.toEpochMicros(effective.toInstant(ZoneOffset.UTC))
                        : effective.toInstant(ZoneOffset.UTC).toEpochMilli();

        SchemaBuilder schema = SchemaBuilder.int64().name(schemaName).version(SCHEMA_VERSION);
        if (fallback == null) {
            schema = schema.optional();
        }
        logRegistration(TYPE_DATETIME, field, columnRaw, hasDefault, fallback);

        final AtomicBoolean firstCallSeen = new AtomicBoolean(false);
        registration.register(schema, input -> {
            if (!firstCallSeen.getAndSet(true) && hasDefault) {
                return fallback;
            }
            if (input == null) {
                return fallback;
            }
            if (isMicro) {
                if (input instanceof Timestamp ts) {
                    return io.debezium.time.Conversions.toEpochMicros(ts.toInstant());
                }
                if (input instanceof LocalDateTime ldt) {
                    return io.debezium.time.Conversions.toEpochMicros(ldt.toInstant(ZoneOffset.UTC));
                }
            }
            else {
                if (input instanceof Timestamp ts) {
                    return ts.getTime();
                }
                if (input instanceof LocalDateTime ldt) {
                    return ldt.toInstant(ZoneOffset.UTC).toEpochMilli();
                }
            }
            if (input instanceof Long l) {
                return l;
            }
            LOGGER.warn("Unexpected input type {} on DATETIME field '{}.{}', returning fallback",
                    input.getClass().getName(), field.dataCollection(), field.name());
            return fallback;
        });
    }

    private void registerTimestamp(RelationalColumn field, ConverterRegistration<SchemaBuilder> registration) {
        final String columnRaw = lookupColumnFallback(field);
        final OffsetDateTime effective = (columnRaw != null) ? parseTimestampFallback(columnRaw) : fallbackTimestamp;
        final String fallback = effective == null ? null : effective.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        final boolean hasDefault = field.hasDefaultValue();

        SchemaBuilder schema = SchemaBuilder.string().name(io.debezium.time.ZonedTimestamp.SCHEMA_NAME).version(SCHEMA_VERSION);
        if (fallback == null) {
            schema = schema.optional();
        }
        logRegistration(TYPE_TIMESTAMP, field, columnRaw, hasDefault, fallback);

        final AtomicBoolean firstCallSeen = new AtomicBoolean(false);
        registration.register(schema, input -> {
            if (!firstCallSeen.getAndSet(true) && hasDefault) {
                return fallback;
            }
            if (input == null) {
                return fallback;
            }
            if (input instanceof Timestamp ts) {
                return ts.toInstant().atOffset(ZoneOffset.UTC).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
            }
            if (input instanceof OffsetDateTime odt) {
                return odt.withOffsetSameInstant(ZoneOffset.UTC).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
            }
            if (input instanceof ZonedDateTime zdt) {
                return zdt.withZoneSameInstant(ZoneOffset.UTC).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
            }
            if (input instanceof String s) {
                return s;
            }
            LOGGER.warn("Unexpected input type {} on TIMESTAMP field '{}.{}', returning fallback",
                    input.getClass().getName(), field.dataCollection(), field.name());
            return fallback;
        });
    }

    private String lookupColumnFallback(RelationalColumn field) {
        if (rawProps == null) {
            return null;
        }
        final String key = ZeroDateFallbackConverterConfig.COLUMN_FALLBACK_PREFIX
                + field.dataCollection() + "." + field.name()
                + ZeroDateFallbackConverterConfig.COLUMN_FALLBACK_SUFFIX;
        return rawProps.getProperty(key);
    }

    private static void logRegistration(String typeLabel, RelationalColumn field, String columnRaw,
                                        boolean hasDefault, Object fallback) {
        if (!LOGGER.isDebugEnabled()) {
            return;
        }
        LOGGER.debug("Registered {} converter on '{}.{}' source={} hasDefault={} fallback={}",
                typeLabel, field.dataCollection(), field.name(),
                columnRaw != null ? "column-level" : "type-level",
                hasDefault,
                fallback == null ? NULL_TOKEN : fallback);
    }

    private static Set<String> parseCsv(String raw) {
        return Strings.setOfTrimmed(raw, ',', s -> s.isEmpty() ? null : s);
    }

    static LocalDate parseDateFallback(String raw) {
        if (isNullToken(raw)) {
            return null;
        }
        return LocalDate.parse(raw.trim());
    }

    static LocalDateTime parseDatetimeFallback(String raw) {
        if (isNullToken(raw)) {
            return null;
        }
        return LocalDateTime.parse(raw.trim(), DATETIME_FMT);
    }

    static OffsetDateTime parseTimestampFallback(String raw) {
        if (isNullToken(raw)) {
            return null;
        }
        return OffsetDateTime.parse(raw.trim()).withOffsetSameInstant(ZoneOffset.UTC);
    }

    private static boolean isNullToken(String raw) {
        return Strings.isNullOrBlank(raw) || NULL_TOKEN.equalsIgnoreCase(raw.trim());
    }

    private static String describe(Object value) {
        return value == null ? NULL_TOKEN : value.toString();
    }
}
