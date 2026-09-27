/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.Test;
import org.postgresql.PGStatement;
import org.postgresql.core.Oid;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.doc.FixFor;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.Column;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.ValueConverter;
import io.debezium.time.Conversions;

class PostgresValueConverterTest {

    private static final int NUMERIC_ARRAY_OID = 2003;

    private static final Column NUMERIC_ARRAY_COLUMN = Column.editor()
            .name("numeric_array")
            .type("_numeric")
            .jdbcType(Types.ARRAY)
            .nativeType(NUMERIC_ARRAY_OID)
            .length(9)
            .scale(3)
            .optional(false)
            .create();

    private static final Field NUMERIC_ARRAY_FIELD = new Field("numeric_array", 0,
            SchemaBuilder.array(Decimal.builder(3).optional().build()).build());

    private static final List<BigDecimal> VALUES = List.of(new BigDecimal("1.100"), new BigDecimal("2.200"));

    private final PostgresValueConverter converter = PostgresValueConverter.of(
            new PostgresConnectorConfig(Configuration.create()
                    .with(CommonConnectorConfig.TOPIC_PREFIX, "test")
                    .build()),
            StandardCharsets.UTF_8,
            null);

    private final ValueConverter elementConverter = value -> value;

    @Test
    public void shouldConvertArrayFromJdbcArrayThatIsNotAPgArray() {
        Object converted = converter.convertArray(NUMERIC_ARRAY_COLUMN, NUMERIC_ARRAY_FIELD, PostgresType.UNKNOWN,
                elementConverter, jdbcArrayOf(VALUES.toArray()));

        assertThat(converted).isEqualTo(VALUES);
    }

    @Test
    public void shouldConvertArrayFromList() {
        Object converted = converter.convertArray(NUMERIC_ARRAY_COLUMN, NUMERIC_ARRAY_FIELD, PostgresType.UNKNOWN,
                elementConverter, VALUES);

        assertThat(converted).isEqualTo(VALUES);
    }

    @Test
    public void shouldRejectValueThatIsNeitherAnArrayNorAList() {
        assertThatThrownBy(() -> converter.convertArray(NUMERIC_ARRAY_COLUMN, NUMERIC_ARRAY_FIELD,
                PostgresType.UNKNOWN, elementConverter, "{1.100,2.200}"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unexpected value for JDBC type " + Types.ARRAY);
    }

    private static Array jdbcArrayOf(Object[] values) {
        return jdbcArray(values, null);
    }

    @FixFor("debezium/dbz#2524")
    @Test
    public void shouldMatchPositiveAndNegativeInfinityInstantAndOffsetDateTime() {
        final Instant expectedPositiveInstant = Conversions.toInstantFromMillis(PGStatement.DATE_POSITIVE_INFINITY);
        final Instant expectedNegativeInstant = Conversions.toInstantFromMillis(PGStatement.DATE_NEGATIVE_INFINITY);

        assertThat(PostgresValueConverter.POSITIVE_INFINITY_INSTANT).isEqualTo(expectedPositiveInstant);
        assertThat(PostgresValueConverter.NEGATIVE_INFINITY_INSTANT).isEqualTo(expectedNegativeInstant);
        assertThat(PostgresValueConverter.POSITIVE_INFINITY_OFFSET_DATE_TIME.toInstant())
                .isEqualTo(PostgresValueConverter.POSITIVE_INFINITY_INSTANT);
        assertThat(PostgresValueConverter.NEGATIVE_INFINITY_OFFSET_DATE_TIME.toInstant())
                .isEqualTo(PostgresValueConverter.NEGATIVE_INFINITY_INSTANT);
    }

    @FixFor("debezium/dbz#2524")
    @Test
    public void shouldMatchPositiveAndNegativeInfinityLocalDate() {
        assertThat(PostgresValueConverter.POSITIVE_INFINITY_LOCAL_DATE.getYear()).isPositive();
        assertThat(PostgresValueConverter.POSITIVE_INFINITY_LOCAL_DATE).isEqualTo(LocalDate.parse("+5877611-06-21"));
        assertThat(PostgresValueConverter.NEGATIVE_INFINITY_LOCAL_DATE.getYear()).isNegative();
        assertThat(PostgresValueConverter.NEGATIVE_INFINITY_LOCAL_DATE).isEqualTo(LocalDate.parse("-5877611-06-22"));
    }

    /**
     * A time[] element must be read as text. java.sql.Time keeps only milliseconds and can throw when the
     * JVM zone makes its epoch millis negative. See debezium/dbz#2559.
     */
    @FixFor("debezium/dbz#2559")
    @Test
    public void shouldPreserveTimeArrayMicrosecondsIndependentOfJvmZone() throws SQLException {
        TimeZone original = TimeZone.getDefault();
        TimeZone.setDefault(TimeZone.getTimeZone("GMT+3"));
        try {
            assertThat(convertTimeArray(TemporalPrecisionMode.ISOSTRING, "02:59:59.999999"))
                    .isEqualTo(List.of("02:59:59.999999Z", "02:59:59.999999Z"));
            assertThat(convertTimeArray(TemporalPrecisionMode.ISOSTRING, "03:00:00.123456"))
                    .isEqualTo(List.of("03:00:00.123456Z", "03:00:00.123456Z"));

            assertThat(convertTimeArray(TemporalPrecisionMode.ADAPTIVE, "02:59:59.999999"))
                    .isEqualTo(List.of(10_799_999_999L, 10_799_999_999L));
            assertThat(convertTimeArray(TemporalPrecisionMode.ADAPTIVE, "03:00:00.123456"))
                    .isEqualTo(List.of(10_800_123_456L, 10_800_123_456L));

            // connect keeps millisecond precision, but must not throw when the value is earlier than the zone offset.
            Date early = new Date(LocalTime.parse("02:59:59.999999").toNanoOfDay() / 1_000_000L);
            Date onOffset = new Date(LocalTime.parse("03:00:00.123456").toNanoOfDay() / 1_000_000L);
            assertThat(convertTimeArray(TemporalPrecisionMode.CONNECT, "02:59:59.999999")).isEqualTo(List.of(early, early));
            assertThat(convertTimeArray(TemporalPrecisionMode.CONNECT, "03:00:00.123456")).isEqualTo(List.of(onOffset, onOffset));
        }
        finally {
            TimeZone.setDefault(original);
        }
    }

    private static PostgresValueConverter timeConverter(TemporalPrecisionMode mode) {
        return PostgresValueConverter.of(
                new PostgresConnectorConfig(Configuration.create()
                        .with(CommonConnectorConfig.TOPIC_PREFIX, "test")
                        .with(RelationalDatabaseConnectorConfig.TIME_PRECISION_MODE, mode)
                        .build()),
                StandardCharsets.UTF_8,
                null);
    }

    private Object convertTimeArray(TemporalPrecisionMode mode, String value) throws SQLException {
        PostgresValueConverter converter = timeConverter(mode);
        Column column = Column.editor()
                .name("time_6_array")
                .type("_time")
                .jdbcType(Types.ARRAY)
                .nativeType(Oid.TIME_ARRAY)
                .scale(6)
                .optional(true)
                .create();
        Column elementColumn = Column.editor()
                .name("time_6_array")
                .type("time")
                .jdbcType(Types.TIME)
                .nativeType(Oid.TIME)
                .scale(6)
                .optional(true)
                .create();
        Field elementField = new Field("time_6_array", 0, mode.getTimeBuilder(6).optional().build());
        Field field = new Field("time_6_array", 0, SchemaBuilder.array(elementField.schema()).optional().build());
        PostgresType timeType = new PostgresType.Builder(null, "time", Oid.TIME, Types.TIME, TypeRegistry.NO_TYPE_MODIFIER, null).build();
        return converter.convertArray(column, field, timeType, converter.converter(elementColumn, elementField),
                jdbcArray(jdbcTimes(value), new String[]{ value, value }));
    }

    /**
     * pgjdbc returns {@link Time} from {@code Array.getArray()}. That type keeps only milliseconds and interprets
     * the value in the JVM zone, which is the failure this test has to be able to reproduce.
     */
    private static Time[] jdbcTimes(String value) {
        LocalTime time = LocalTime.parse(value);
        long millis = time.toNanoOfDay() / 1_000_000L;
        ZoneId zone = ZoneId.systemDefault();
        Time jdbcTime = new Time(millis - zone.getRules().getOffset(Instant.EPOCH).getTotalSeconds() * 1000L);
        return new Time[]{ jdbcTime, jdbcTime };
    }

    private static Array jdbcArray(Object[] values, String[] textValues) {
        return (Array) Proxy.newProxyInstance(
                PostgresValueConverterTest.class.getClassLoader(),
                new Class<?>[]{ Array.class },
                (proxy, method, args) -> {
                    switch (method.getName()) {
                        case "getArray":
                            return values;
                        case "getResultSet":
                            return stringResultSet(textValues);
                        case "getBaseType":
                            return Types.NUMERIC;
                        case "toString":
                            return "java.sql.Array" + List.of(values == null ? textValues : values);
                        case "hashCode":
                            return System.identityHashCode(proxy);
                        case "equals":
                            return proxy == args[0];
                        default:
                            throw new UnsupportedOperationException(method.getName());
                    }
                });
    }

    private static ResultSet stringResultSet(String[] values) {
        final int[] index = { -1 };
        return (ResultSet) Proxy.newProxyInstance(
                PostgresValueConverterTest.class.getClassLoader(),
                new Class<?>[]{ ResultSet.class },
                (proxy, method, args) -> {
                    switch (method.getName()) {
                        case "next":
                            return ++index[0] < values.length;
                        case "getString":
                            // PgArray column 1 is the element index and column 2 is the value.
                            return (int) args[0] == 2 ? values[index[0]] : String.valueOf(index[0] + 1);
                        case "close":
                            return null;
                        default:
                            throw new UnsupportedOperationException(method.getName());
                    }
                });
    }

    @FixFor("debezium/dbz#2524")
    @Test
    public void shouldConvertInfinityTimestampsToEpochNanos() {
        assertThat(converter.convertTimestampToEpochNanos(null, null, PostgresValueConverter.POSITIVE_INFINITY_INSTANT))
                .isEqualTo(Long.MAX_VALUE);
        assertThat(converter.convertTimestampToEpochNanos(null, null, PostgresValueConverter.NEGATIVE_INFINITY_INSTANT))
                .isEqualTo(Long.MIN_VALUE);
        assertThat(converter.convertTimestampToEpochNanos(null, null, PostgresValueConverter.POSITIVE_INFINITY_LOCAL_DATE_TIME))
                .isEqualTo(Long.MAX_VALUE);
        assertThat(converter.convertTimestampToEpochNanos(null, null, PostgresValueConverter.NEGATIVE_INFINITY_LOCAL_DATE_TIME))
                .isEqualTo(Long.MIN_VALUE);
    }
}
