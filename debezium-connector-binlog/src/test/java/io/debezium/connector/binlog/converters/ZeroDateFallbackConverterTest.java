/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog.converters;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.Date;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;

/**
 * Unit tests for {@link ZeroDateFallbackConverter}, covering type-level configuration,
 * column-level FQN overrides, the NULL vs user-value policies (including the first-call
 * schema-default trick), input-type dispatch, and target-types / selector filters.
 *
 * @author minleejae
 */
class ZeroDateFallbackConverterTest {

    private static final String DATA_COLLECTION = "appdb.users";

    // -- configure() ---------------------------------------------------------------------------

    @Test
    void shouldDefaultAllFallbacksToNull() {
        final ZeroDateFallbackConverter converter = newConfigured(new Properties());

        final Registration reg = registerOn(converter, dateColumn("d", false));
        assertThat(reg.schema.isOptional()).isTrue();
        assertThat(reg.converter.convert(null)).isNull();
    }

    @Test
    void shouldParseDateFallbackLiteral() {
        final Properties props = new Properties();
        props.setProperty("fallback.date", "2000-01-01");

        final ZeroDateFallbackConverter converter = newConfigured(props);
        final Registration reg = registerOn(converter, dateColumn("d", false));

        assertThat(reg.schema.isOptional()).isFalse();
        assertThat(reg.converter.convert(null)).isEqualTo((int) LocalDate.parse("2000-01-01").toEpochDay());
    }

    @Test
    void shouldTreatNullTokenAsNullPolicy() {
        final Properties props = new Properties();
        props.setProperty("fallback.date", "NULL");
        props.setProperty("fallback.datetime", "null");
        props.setProperty("fallback.timestamp", "  Null  ");

        final ZeroDateFallbackConverter converter = newConfigured(props);

        assertThat(registerOn(converter, dateColumn("d", false)).schema.isOptional()).isTrue();
        assertThat(registerOn(converter, datetimeColumn("dt", false)).schema.isOptional()).isTrue();
        assertThat(registerOn(converter, timestampColumn("ts", false)).schema.isOptional()).isTrue();
    }

    @Test
    void shouldRejectInvalidDateLiteral() {
        final Properties props = new Properties();
        props.setProperty("fallback.date", "not-a-date");

        assertThatThrownBy(() -> newConfigured(props))
                .isInstanceOf(java.time.format.DateTimeParseException.class);
    }

    @Test
    void shouldRejectInvalidDatetimeLiteral() {
        final Properties props = new Properties();
        props.setProperty("fallback.datetime", "2000-01-01"); // missing time portion

        assertThatThrownBy(() -> newConfigured(props))
                .isInstanceOf(java.time.format.DateTimeParseException.class);
    }

    @Test
    void shouldRejectInvalidTimestampLiteral() {
        final Properties props = new Properties();
        props.setProperty("fallback.timestamp", "2000-01-01 00:00:00"); // missing offset

        assertThatThrownBy(() -> newConfigured(props))
                .isInstanceOf(java.time.format.DateTimeParseException.class);
    }

    // -- target.types --------------------------------------------------------------------------

    @Test
    void shouldSkipTypesNotInTargetSet() {
        final Properties props = new Properties();
        props.setProperty("target.types", "DATETIME");

        final ZeroDateFallbackConverter converter = newConfigured(props);

        assertThat(tryRegister(converter, dateColumn("d", false))).isNull();
        assertThat(tryRegister(converter, datetimeColumn("dt", false))).isNotNull();
        assertThat(tryRegister(converter, timestampColumn("ts", false))).isNull();
    }

    @Test
    void shouldDefaultTargetTypesToAllThree() {
        final ZeroDateFallbackConverter converter = newConfigured(new Properties());

        assertThat(tryRegister(converter, dateColumn("d", false))).isNotNull();
        assertThat(tryRegister(converter, datetimeColumn("dt", false))).isNotNull();
        assertThat(tryRegister(converter, timestampColumn("ts", false))).isNotNull();
    }

    // -- DATE branch ---------------------------------------------------------------------------

    @Test
    void dateLambda_inputNull_returnsFallback() {
        final Registration reg = registerOn(newConfigured(propsWith("fallback.date", "2000-01-01")),
                dateColumn("d", false));
        assertThat(reg.converter.convert(null)).isEqualTo((int) LocalDate.parse("2000-01-01").toEpochDay());
    }

    @Test
    void dateLambda_javaSqlDate_returnsEpochDays() {
        final Registration reg = registerOn(newConfigured(new Properties()), dateColumn("d", false));
        assertThat(reg.converter.convert(Date.valueOf("2024-06-15")))
                .isEqualTo((int) LocalDate.parse("2024-06-15").toEpochDay());
    }

    @Test
    void dateLambda_localDate_returnsEpochDays() {
        final Registration reg = registerOn(newConfigured(new Properties()), dateColumn("d", false));
        assertThat(reg.converter.convert(LocalDate.parse("2024-06-15")))
                .isEqualTo((int) LocalDate.parse("2024-06-15").toEpochDay());
    }

    @Test
    void dateLambda_integerInput_passesThrough() {
        final Registration reg = registerOn(newConfigured(new Properties()), dateColumn("d", false));
        assertThat(reg.converter.convert(19889)).isEqualTo(19889);
    }

    @Test
    void dateLambda_unexpectedInput_returnsFallback() {
        final Registration reg = registerOn(newConfigured(propsWith("fallback.date", "2000-01-01")),
                dateColumn("d", false));
        assertThat(reg.converter.convert("garbage"))
                .isEqualTo((int) LocalDate.parse("2000-01-01").toEpochDay());
    }

    // -- DATETIME branch -----------------------------------------------------------------------

    @Test
    void datetimeLambda_inputNull_returnsFallback() {
        final Registration reg = registerOn(newConfigured(propsWith("fallback.datetime", "2000-01-01 00:00:00")),
                datetimeColumn("dt", false));
        final long expected = LocalDateTime.parse("2000-01-01T00:00:00").toInstant(ZoneOffset.UTC).toEpochMilli();
        assertThat(reg.converter.convert(null)).isEqualTo(expected);
    }

    @Test
    void datetimeLambda_javaSqlTimestamp_returnsEpochMillis() {
        final Registration reg = registerOn(newConfigured(new Properties()), datetimeColumn("dt", false));
        final Timestamp ts = Timestamp.valueOf("2024-06-15 12:34:56");
        assertThat(reg.converter.convert(ts)).isEqualTo(ts.getTime());
    }

    @Test
    void datetimeLambda_localDateTime_returnsEpochMillisInUtc() {
        final Registration reg = registerOn(newConfigured(new Properties()), datetimeColumn("dt", false));
        final LocalDateTime ldt = LocalDateTime.parse("2024-06-15T12:34:56");
        assertThat(reg.converter.convert(ldt))
                .isEqualTo(ldt.toInstant(ZoneOffset.UTC).toEpochMilli());
    }

    @Test
    void datetimeLambda_longInput_passesThrough() {
        final Registration reg = registerOn(newConfigured(new Properties()), datetimeColumn("dt", false));
        assertThat(reg.converter.convert(1_718_454_896_000L)).isEqualTo(1_718_454_896_000L);
    }

    @Test
    void datetimeLambda_unexpectedInput_returnsFallback() {
        final Registration reg = registerOn(newConfigured(propsWith("fallback.datetime", "2000-01-01 00:00:00")),
                datetimeColumn("dt", false));
        final long expected = LocalDateTime.parse("2000-01-01T00:00:00").toInstant(ZoneOffset.UTC).toEpochMilli();
        assertThat(reg.converter.convert("garbage")).isEqualTo(expected);
    }

    // -- DATETIME(4-6) branch: MicroTimestamp precision ----------------------------------------

    @Test
    void datetimeLambda_precision4_schemaIsMicroTimestamp() {
        final Registration reg = registerOn(newConfigured(new Properties()), datetimeColumn("dt", false, 4));
        assertThat(reg.schema.name()).isEqualTo(io.debezium.time.MicroTimestamp.SCHEMA_NAME);
    }

    @Test
    void datetimeLambda_precision6_schemaIsMicroTimestamp() {
        final Registration reg = registerOn(newConfigured(new Properties()), datetimeColumn("dt", false, 6));
        assertThat(reg.schema.name()).isEqualTo(io.debezium.time.MicroTimestamp.SCHEMA_NAME);
    }

    @Test
    void datetimeLambda_precision3_schemaIsTimestamp() {
        final Registration reg = registerOn(newConfigured(new Properties()), datetimeColumn("dt", false, 3));
        assertThat(reg.schema.name()).isEqualTo(io.debezium.time.Timestamp.SCHEMA_NAME);
    }

    @Test
    void datetimeLambda_noPrecision_schemaIsTimestamp() {
        final Registration reg = registerOn(newConfigured(new Properties()), datetimeColumn("dt", false));
        assertThat(reg.schema.name()).isEqualTo(io.debezium.time.Timestamp.SCHEMA_NAME);
    }

    @Test
    void datetimeLambda_precision5_inputNull_returnsFallbackInMicros() {
        final Registration reg = registerOn(
                newConfigured(propsWith("fallback.datetime", "2000-01-01 00:00:00")),
                datetimeColumn("dt", false, 5));
        final long expected = io.debezium.time.Conversions
                .toEpochMicros(LocalDateTime.parse("2000-01-01T00:00:00").toInstant(ZoneOffset.UTC));
        assertThat(reg.converter.convert(null)).isEqualTo(expected);
    }

    @Test
    void datetimeLambda_precision6_javaSqlTimestamp_returnsEpochMicros() {
        final Registration reg = registerOn(newConfigured(new Properties()), datetimeColumn("dt", false, 6));
        final Timestamp ts = Timestamp.valueOf("2024-06-15 12:34:56.123456");
        final long expected = io.debezium.time.Conversions.toEpochMicros(ts.toInstant());
        assertThat(reg.converter.convert(ts)).isEqualTo(expected);
    }

    @Test
    void datetimeLambda_precision4_localDateTime_returnsEpochMicros() {
        final Registration reg = registerOn(newConfigured(new Properties()), datetimeColumn("dt", false, 4));
        final LocalDateTime ldt = LocalDateTime.parse("2024-06-15T12:34:56.1234");
        final long expected = io.debezium.time.Conversions.toEpochMicros(ldt.toInstant(ZoneOffset.UTC));
        assertThat(reg.converter.convert(ldt)).isEqualTo(expected);
    }

    // -- TIMESTAMP branch ----------------------------------------------------------------------

    @Test
    void timestampLambda_inputNull_returnsFallback() {
        final Registration reg = registerOn(newConfigured(propsWith("fallback.timestamp", "1970-01-01T00:00:00Z")),
                timestampColumn("ts", false));
        assertThat(reg.converter.convert(null)).isEqualTo("1970-01-01T00:00:00Z");
    }

    @Test
    void timestampLambda_javaSqlTimestamp_returnsIso8601Utc() {
        final Registration reg = registerOn(newConfigured(new Properties()), timestampColumn("ts", false));
        final Timestamp ts = Timestamp.from(OffsetDateTime.parse("2024-06-15T12:34:56Z").toInstant());
        assertThat(reg.converter.convert(ts)).isEqualTo("2024-06-15T12:34:56Z");
    }

    @Test
    void timestampLambda_offsetDateTime_normalizedToUtc() {
        final Registration reg = registerOn(newConfigured(new Properties()), timestampColumn("ts", false));
        final OffsetDateTime odt = OffsetDateTime.parse("2024-06-15T21:34:56+09:00");
        assertThat(reg.converter.convert(odt)).isEqualTo("2024-06-15T12:34:56Z");
    }

    @Test
    void timestampLambda_zonedDateTime_normalizedToUtc() {
        final Registration reg = registerOn(newConfigured(new Properties()), timestampColumn("ts", false));
        final ZonedDateTime zdt = ZonedDateTime.parse("2024-06-15T12:34:56Z");
        assertThat(reg.converter.convert(zdt)).isEqualTo("2024-06-15T12:34:56Z");
    }

    @Test
    void timestampLambda_stringInput_passesThrough() {
        final Registration reg = registerOn(newConfigured(new Properties()), timestampColumn("ts", false));
        assertThat(reg.converter.convert("2024-06-15T12:34:56Z")).isEqualTo("2024-06-15T12:34:56Z");
    }

    // -- First-call schema-default trick -------------------------------------------------------

    @Test
    void firstCall_columnWithDdlDefault_returnsFallback_underNullPolicy() {
        final Registration reg = registerOn(newConfigured(new Properties()), dateColumn("d", true));

        // Schema must be optional under NULL policy
        assertThat(reg.schema.isOptional()).isTrue();
        // First call (schema-default conversion) ignores input and returns null
        assertThat(reg.converter.convert(LocalDate.parse("2024-06-15"))).isNull();
        // Subsequent row-level calls dispatch normally
        assertThat(reg.converter.convert(LocalDate.parse("2024-06-15")))
                .isEqualTo((int) LocalDate.parse("2024-06-15").toEpochDay());
    }

    @Test
    void firstCall_columnWithDdlDefault_returnsFallback_underUserValuePolicy() {
        final Registration reg = registerOn(newConfigured(propsWith("fallback.datetime", "2000-01-01 00:00:00")),
                datetimeColumn("dt", true));
        final long expected = LocalDateTime.parse("2000-01-01T00:00:00").toInstant(ZoneOffset.UTC).toEpochMilli();

        // Schema retains NOT NULL semantics (no .optional())
        assertThat(reg.schema.isOptional()).isFalse();
        // First call returns the user-configured fallback regardless of the DDL default's resolved value
        assertThat(reg.converter.convert(LocalDate.parse("1970-01-01"))).isEqualTo(expected);
        // Row-level: real-1970 still emits 0L because it's a genuine LocalDateTime (non-null)
        assertThat(reg.converter.convert(LocalDateTime.parse("1970-01-01T00:00:00"))).isEqualTo(0L);
    }

    @Test
    void firstCall_columnWithoutDdlDefault_dispatchesNormally() {
        final Registration reg = registerOn(newConfigured(new Properties()), dateColumn("d", false));
        // No first-call guard fires because hasDefaultValue=false; row-level dispatch from the very first call
        assertThat(reg.converter.convert(LocalDate.parse("2024-06-15")))
                .isEqualTo((int) LocalDate.parse("2024-06-15").toEpochDay());
    }

    // -- Column-level override ----------------------------------------------------------------

    @Test
    void columnLevelOverride_takesPrecedenceOverTypeLevel() {
        final Properties props = new Properties();
        props.setProperty("fallback.datetime", "2000-01-01 00:00:00");
        props.setProperty("column.appdb.users.deleted_at.fallback", "NULL");
        props.setProperty("column.appdb.users.shipped_at.fallback", "2099-12-31 23:59:59");

        final ZeroDateFallbackConverter converter = newConfigured(props);

        final Registration nullCol = registerOn(converter, datetimeColumn("deleted_at", false));
        final Registration otherCol = registerOn(converter, datetimeColumn("shipped_at", false));
        final Registration typeCol = registerOn(converter, datetimeColumn("created_at", false));

        // Column-level NULL overrides type-level value
        assertThat(nullCol.schema.isOptional()).isTrue();
        assertThat(nullCol.converter.convert(null)).isNull();
        // Column-level value overrides type-level value
        assertThat(otherCol.schema.isOptional()).isFalse();
        assertThat(otherCol.converter.convert(null))
                .isEqualTo(LocalDateTime.parse("2099-12-31T23:59:59").toInstant(ZoneOffset.UTC).toEpochMilli());
        // No column-level key → falls back to type-level
        assertThat(typeCol.schema.isOptional()).isFalse();
        assertThat(typeCol.converter.convert(null))
                .isEqualTo(LocalDateTime.parse("2000-01-01T00:00:00").toInstant(ZoneOffset.UTC).toEpochMilli());
    }

    // -- selector ------------------------------------------------------------------------------

    @Test
    void selector_filtersOutNonMatchingColumns() {
        final Properties props = new Properties();
        props.setProperty("selector", "appdb\\.users\\.deleted_at");

        final ZeroDateFallbackConverter converter = newConfigured(props);

        assertThat(tryRegister(converter, datetimeColumn("deleted_at", false))).isNotNull();
        assertThat(tryRegister(converter, datetimeColumn("created_at", false))).isNull();
    }

    @Test
    void selector_defaultWildcardMatchesAll() {
        final ZeroDateFallbackConverter converter = newConfigured(new Properties());
        assertThat(tryRegister(converter, datetimeColumn("deleted_at", false))).isNotNull();
        assertThat(tryRegister(converter, datetimeColumn("any_other_column", false))).isNotNull();
    }

    // -- TIMESTAMP_WITH_TIMEZONE variant ------------------------------------------------------

    @Test
    void shouldAcceptTimestampWithTimezoneJdbcType() {
        final RelationalColumn col = Mockito.mock(RelationalColumn.class);
        Mockito.when(col.name()).thenReturn("ts");
        Mockito.when(col.dataCollection()).thenReturn(DATA_COLLECTION);
        Mockito.when(col.jdbcType()).thenReturn(Types.TIMESTAMP_WITH_TIMEZONE);
        Mockito.when(col.typeName()).thenReturn("TIMESTAMP_WITH_TIMEZONE");
        Mockito.when(col.hasDefaultValue()).thenReturn(false);

        final ZeroDateFallbackConverter converter = newConfigured(new Properties());
        assertThat(tryRegister(converter, col)).isNotNull();
    }

    // -- Config descriptor --------------------------------------------------------------------

    @Test
    void configDescriptorExposesAllFields() {
        final ZeroDateFallbackConverter converter = new ZeroDateFallbackConverter();
        assertThat(converter.getConfigFields().asArray())
                .extracting("name")
                .containsExactlyInAnyOrder(
                        "target.types", "fallback.date", "fallback.datetime", "fallback.timestamp", "selector");
    }

    // -- helpers ------------------------------------------------------------------------------

    private static ZeroDateFallbackConverter newConfigured(Properties props) {
        final ZeroDateFallbackConverter converter = new ZeroDateFallbackConverter();
        converter.configure(props);
        return converter;
    }

    private static Properties propsWith(String key, String value) {
        final Properties p = new Properties();
        p.setProperty(key, value);
        return p;
    }

    private static RelationalColumn dateColumn(String name, boolean hasDefault) {
        return mockColumn(name, Types.DATE, "DATE", hasDefault, OptionalInt.empty());
    }

    private static RelationalColumn datetimeColumn(String name, boolean hasDefault) {
        return mockColumn(name, Types.TIMESTAMP, "DATETIME", hasDefault, OptionalInt.empty());
    }

    private static RelationalColumn datetimeColumn(String name, boolean hasDefault, int precision) {
        return mockColumn(name, Types.TIMESTAMP, "DATETIME", hasDefault, OptionalInt.of(precision));
    }

    private static RelationalColumn timestampColumn(String name, boolean hasDefault) {
        return mockColumn(name, Types.TIMESTAMP_WITH_TIMEZONE, "TIMESTAMP", hasDefault, OptionalInt.empty());
    }

    private static RelationalColumn mockColumn(String name, int jdbcType, String typeName, boolean hasDefault, OptionalInt length) {
        final RelationalColumn col = Mockito.mock(RelationalColumn.class);
        Mockito.when(col.name()).thenReturn(name);
        Mockito.when(col.dataCollection()).thenReturn(DATA_COLLECTION);
        Mockito.when(col.jdbcType()).thenReturn(jdbcType);
        Mockito.when(col.typeName()).thenReturn(typeName);
        Mockito.when(col.hasDefaultValue()).thenReturn(hasDefault);
        Mockito.when(col.length()).thenReturn(length);
        return col;
    }

    private static Registration registerOn(ZeroDateFallbackConverter converter, RelationalColumn column) {
        final Registration reg = tryRegister(converter, column);
        assertThat(reg).as("converter did not register a schema for column %s", column.name()).isNotNull();
        return reg;
    }

    private static Registration tryRegister(ZeroDateFallbackConverter converter, RelationalColumn column) {
        final AtomicReference<SchemaBuilder> schemaRef = new AtomicReference<>();
        final AtomicReference<CustomConverter.Converter> convRef = new AtomicReference<>();
        converter.converterFor(column, (schema, conv) -> {
            schemaRef.set(schema);
            convRef.set(conv);
        });
        if (schemaRef.get() == null) {
            return null;
        }
        return new Registration(schemaRef.get().build(), convRef.get());
    }

    private record Registration(Schema schema, CustomConverter.Converter converter) {
    }
}
