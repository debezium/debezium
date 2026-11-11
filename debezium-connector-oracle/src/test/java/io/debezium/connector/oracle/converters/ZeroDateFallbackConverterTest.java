/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.converters;

import static io.debezium.connector.oracle.util.OracleZeroDates.ZERO_DATE_BYTES;
import static io.debezium.connector.oracle.util.OracleZeroDates.ZERO_TIMESTAMP_BYTES;
import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Types;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.OptionalInt;
import java.util.Properties;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.Test;

import io.debezium.doc.FixFor;
import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;
import io.debezium.time.MicroTimestamp;
import io.debezium.time.NanoTimestamp;
import io.debezium.time.Timestamp;

import oracle.sql.DATE;
import oracle.sql.TIMESTAMP;

/**
 * Unit tests for the Oracle {@link ZeroDateFallbackConverter}.
 *
 * @author Chris Cranford
 */
public class ZeroDateFallbackConverterTest {

    private static final String DATA_COLLECTION = "ORCLPDB1.DEBEZIUM.ZERO_DATES";

    @Test
    @FixFor("debezium/dbz#3055")
    public void shouldDefaultToNullPolicy() throws Exception {
        final Registration reg = register(new Properties(), dateColumn("D", false, null));

        assertThat(reg.schema.name()).isEqualTo(Timestamp.SCHEMA_NAME);
        assertThat(reg.schema.isOptional()).isTrue();
        assertThat(reg.converter.convert(null)).isNull();
        assertThat(reg.converter.convert(new DATE(ZERO_DATE_BYTES))).isNull();
        assertThat(reg.converter.convert("TO_DATE('0000-00-00 00:00:00', 'YYYY-MM-DD HH24:MI:SS')")).isNull();
        assertThat(reg.converter.convert("HEXTORAW('64640000010101')")).isNull();

        final LocalDateTime value = LocalDateTime.of(2020, 5, 17, 10, 11, 12);
        assertThat(reg.converter.convert(value)).isEqualTo(toMillis(value));
        assertThat(reg.converter.convert(new DATE(value))).isEqualTo(toMillis(value));
        assertThat(reg.converter.convert("TO_DATE('2020-05-17 10:11:12', 'YYYY-MM-DD HH24:MI:SS')")).isEqualTo(toMillis(value));
    }

    @Test
    @FixFor("debezium/dbz#3055")
    public void shouldReplaceZeroDateWithTypeLevelLiteral() throws Exception {
        final Properties props = new Properties();
        props.setProperty("fallback.date", "1900-01-01");
        props.setProperty("fallback.timestamp", "1900-01-01 00:00:00.000001");

        final Registration date = register(props, dateColumn("D", false, null));
        final long expectedDate = toMillis(LocalDateTime.of(1900, 1, 1, 0, 0));
        assertThat(date.schema.isOptional()).isFalse();
        assertThat(date.converter.convert(null)).isEqualTo(expectedDate);
        assertThat(date.converter.convert(new DATE(ZERO_DATE_BYTES))).isEqualTo(expectedDate);

        final Registration timestamp = register(props, timestampColumn("TS", 6));
        assertThat(timestamp.schema.name()).isEqualTo(MicroTimestamp.SCHEMA_NAME);
        assertThat(timestamp.schema.isOptional()).isFalse();
        final long expectedTimestamp = toMillis(LocalDateTime.of(1900, 1, 1, 0, 0)) * 1_000 + 1;
        assertThat(timestamp.converter.convert(new TIMESTAMP(ZERO_TIMESTAMP_BYTES))).isEqualTo(expectedTimestamp);
        assertThat(timestamp.converter.convert("TO_TIMESTAMP('0000-00-00 00:00:00.000000')")).isEqualTo(expectedTimestamp);
    }

    @Test
    @FixFor("debezium/dbz#3055")
    public void shouldPreferColumnLevelOverride() {
        final Properties props = new Properties();
        props.setProperty("fallback.date", "1900-01-01");
        props.setProperty("column." + DATA_COLLECTION + ".D2.fallback", "NULL");
        props.setProperty("column." + DATA_COLLECTION + ".D3.fallback", "2000-01-01 12:00:00");

        assertThat(register(props, dateColumn("D1", false, null)).converter.convert(null))
                .isEqualTo(toMillis(LocalDateTime.of(1900, 1, 1, 0, 0)));

        final Registration nullOverride = register(props, dateColumn("D2", false, null));
        assertThat(nullOverride.schema.isOptional()).isTrue();
        assertThat(nullOverride.converter.convert(null)).isNull();

        assertThat(register(props, dateColumn("D3", false, null)).converter.convert(null))
                .isEqualTo(toMillis(LocalDateTime.of(2000, 1, 1, 12, 0)));
    }

    @Test
    @FixFor("debezium/dbz#3055")
    public void shouldOverrideColumnDefaultValue() {
        final Properties props = new Properties();
        props.setProperty("fallback.date", "1900-01-01");

        final Registration reg = register(props, dateColumn("D", false, "SYSDATE"));
        final long expected = toMillis(LocalDateTime.of(1900, 1, 1, 0, 0));

        // The first call resolves the schema default value
        assertThat(reg.converter.convert(LocalDateTime.of(2020, 1, 1, 0, 0))).isEqualTo(expected);
        assertThat(reg.converter.convert(new DATE(ZERO_DATE_BYTES))).isEqualTo(expected);
    }

    @Test
    @FixFor("debezium/dbz#3055")
    public void shouldSelectSchemaByTimestampPrecision() {
        assertThat(register(new Properties(), timestampColumn("TS", 3)).schema.name()).isEqualTo(Timestamp.SCHEMA_NAME);
        assertThat(register(new Properties(), timestampColumn("TS", 6)).schema.name()).isEqualTo(MicroTimestamp.SCHEMA_NAME);
        assertThat(register(new Properties(), timestampColumn("TS", 9)).schema.name()).isEqualTo(NanoTimestamp.SCHEMA_NAME);

        // Precision held in the length rather than the scale
        assertThat(register(new Properties(), timestampColumnWithLength("TS", 3)).schema.name()).isEqualTo(Timestamp.SCHEMA_NAME);
        assertThat(register(new Properties(), timestampColumnWithLength("TS", 6)).schema.name()).isEqualTo(MicroTimestamp.SCHEMA_NAME);
        assertThat(register(new Properties(), timestampColumnWithLength("TS", 9)).schema.name()).isEqualTo(NanoTimestamp.SCHEMA_NAME);

        final Properties props = new Properties();
        props.setProperty("fallback.timestamp", "1900-01-01 00:00:00.000000001");
        final Registration nanos = register(props, timestampColumnWithLength("TS", 9));
        assertThat(nanos.converter.convert(new TIMESTAMP(ZERO_TIMESTAMP_BYTES)))
                .isEqualTo(toMillis(LocalDateTime.of(1900, 1, 1, 0, 0)) * 1_000_000 + 1);
    }

    @Test
    @FixFor("debezium/dbz#3055")
    public void shouldHonorSelectorAndTargetTypes() {
        final Properties props = new Properties();
        props.setProperty("selector", ".*\\.D1");
        assertThat(tryRegister(props, dateColumn("D1", true, null))).isNotNull();
        assertThat(tryRegister(props, dateColumn("D2", true, null))).isNull();

        final Properties dateOnly = new Properties();
        dateOnly.setProperty("target.types", "DATE");
        assertThat(tryRegister(dateOnly, dateColumn("D", true, null))).isNotNull();
        assertThat(tryRegister(dateOnly, timestampColumn("TS", 6))).isNull();

        // Columns with a time zone are not affected
        assertThat(tryRegister(new Properties(), new Column("TSTZ", Types.TIMESTAMP, "TIMESTAMP(6) WITH TIME ZONE", OptionalInt.empty(), OptionalInt.of(6), true, null)))
                .isNull();
    }

    private static long toMillis(LocalDateTime value) {
        return value.toInstant(ZoneOffset.UTC).toEpochMilli();
    }

    private static Column dateColumn(String name, boolean optional, Object defaultValue) {
        return new Column(name, Types.TIMESTAMP, "DATE", OptionalInt.empty(), OptionalInt.of(0), optional, defaultValue);
    }

    private static Column timestampColumn(String name, int precision) {
        return new Column(name, Types.TIMESTAMP, "TIMESTAMP(" + precision + ")", OptionalInt.empty(), OptionalInt.of(precision), false, null);
    }

    /**
     * A column read via JDBC metadata, where {@code OracleConnection#overrideColumn} moves the {@code TIMESTAMP}
     * precision from the scale into the length.
     */
    private static Column timestampColumnWithLength(String name, int precision) {
        return new Column(name, Types.TIMESTAMP, "TIMESTAMP(" + precision + ")", OptionalInt.of(precision), OptionalInt.empty(), false, null);
    }

    private static Registration register(Properties props, RelationalColumn column) {
        final Registration registration = tryRegister(props, column);
        assertThat(registration).isNotNull();
        return registration;
    }

    private static Registration tryRegister(Properties props, RelationalColumn column) {
        final ZeroDateFallbackConverter converter = new ZeroDateFallbackConverter();
        converter.configure(props);
        final Registration registration = new Registration();
        converter.converterFor(column, registration);
        return registration.converter == null ? null : registration;
    }

    private static class Registration implements CustomConverter.ConverterRegistration<SchemaBuilder> {
        private Schema schema;
        private CustomConverter.Converter converter;

        @Override
        public void register(SchemaBuilder fieldSchema, CustomConverter.Converter converter) {
            this.schema = fieldSchema.build();
            this.converter = converter;
        }
    }

    private record Column(String name, int jdbcType, String typeName, OptionalInt length, OptionalInt scale, boolean isOptional, Object defaultValue)
            implements
                RelationalColumn {

        @Override
        public String dataCollection() {
            return DATA_COLLECTION;
        }

        @Override
        public int nativeType() {
            return jdbcType;
        }

        @Override
        public String typeExpression() {
            return typeName;
        }

        @Override
        public boolean hasDefaultValue() {
            return defaultValue != null;
        }
    }
}
