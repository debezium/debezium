/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Types;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.doc.FixFor;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.Column;
import io.debezium.time.StructuredDuration;
import io.debezium.time.StructuredTemporal;
import io.debezium.time.StructuredTimestamp;
import io.debezium.time.StructuredZonedTimestamp;

import oracle.jdbc.OracleTypes;
import oracle.sql.CharacterSet;
import oracle.sql.DATE;
import oracle.sql.INTERVALDS;
import oracle.sql.INTERVALYM;
import oracle.sql.TIMESTAMP;
import oracle.sql.TIMESTAMPTZ;

/**
 * Unit tests for {@link OracleValueConverters}, specifically verifying that
 * HEXTORAW string decoding respects the database character set.
 *
 * @author Bjorn Aangbaeck
 */
public class OracleValueConvertersTest {

    private OracleValueConverters convertersUtf8;
    private OracleValueConverters convertersLatin1;
    private OracleValueConverters convertersStructured;

    @BeforeEach
    void setUp() {
        final Configuration configuration = TestHelper.defaultConfig().build();
        final OracleConnectorConfig connectorConfig = new OracleConnectorConfig(configuration);

        // Converter with AL32UTF8 database charset (common case)
        final OracleConnection utf8Connection = Mockito.mock(OracleConnection.class);
        Mockito.when(utf8Connection.getNationalCharacterSet()).thenReturn(CharacterSet.make(CharacterSet.AL16UTF16_CHARSET));
        Mockito.when(utf8Connection.getDatabaseCharacterSet()).thenReturn(CharacterSet.make(CharacterSet.AL32UTF8_CHARSET));
        convertersUtf8 = connectorConfig.getAdapter().getValueConverter(connectorConfig, utf8Connection);

        // Converter with WE8ISO8859P1 (Latin-1) database charset
        final OracleConnection latin1Connection = Mockito.mock(OracleConnection.class);
        Mockito.when(latin1Connection.getNationalCharacterSet()).thenReturn(CharacterSet.make(CharacterSet.AL16UTF16_CHARSET));
        Mockito.when(latin1Connection.getDatabaseCharacterSet()).thenReturn(CharacterSet.make(CharacterSet.WE8ISO8859P1_CHARSET));
        convertersLatin1 = connectorConfig.getAdapter().getValueConverter(connectorConfig, latin1Connection);

        final Configuration structuredConfiguration = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.TIME_PRECISION_MODE, TemporalPrecisionMode.STRUCTURED)
                .build();
        final OracleConnectorConfig structuredConnectorConfig = new OracleConnectorConfig(structuredConfiguration);
        final OracleConnection structuredConnection = Mockito.mock(OracleConnection.class);
        Mockito.when(structuredConnection.getNationalCharacterSet()).thenReturn(CharacterSet.make(CharacterSet.AL16UTF16_CHARSET));
        Mockito.when(structuredConnection.getDatabaseCharacterSet()).thenReturn(CharacterSet.make(CharacterSet.AL32UTF8_CHARSET));
        convertersStructured = structuredConnectorConfig.getAdapter().getValueConverter(structuredConnectorConfig, structuredConnection);
    }

    @Test
    public void shouldDecodeHexToRawWithLatin1CharacterSet() {
        // "KESKUKSEN LISÄTARVIKE" in Latin-1 bytes
        // K=4b E=45 S=53 K=4b U=55 K=4b S=53 E=45 N=4e ' '=20 L=4c I=49 S=53 Ä=c4 T=54 A=41 R=52 V=56 I=49 K=4b E=45
        final String hexToRaw = "HEXTORAW('4b45534b554b53454e204c4953c454415256494b45')";

        final Column column = Column.editor()
                .name("VENDPARTDESCR1")
                .type("VARCHAR2")
                .jdbcType(OracleTypes.VARCHAR)
                .create();

        final Field field = new Field("VENDPARTDESCR1", 0, SchemaBuilder.string().optional().build());

        final Object result = convertersLatin1.convertString(column, field, hexToRaw);
        assertThat(result).isEqualTo("KESKUKSEN LIS\u00C4TARVIKE");
    }

    @Test
    public void shouldDecodeHexToRawWithLatin1NordicCharacters() {
        // Test all Nordic characters: Å=c5 Ä=c4 Ö=d6 å=e5 ä=e4 ö=f6
        final String hexToRaw = "HEXTORAW('c5c4d6e5e4f6')";

        final Column column = Column.editor()
                .name("DATA")
                .type("VARCHAR2")
                .jdbcType(OracleTypes.VARCHAR)
                .create();

        final Field field = new Field("DATA", 0, SchemaBuilder.string().optional().build());

        final Object result = convertersLatin1.convertString(column, field, hexToRaw);
        assertThat(result).isEqualTo("\u00C5\u00C4\u00D6\u00E5\u00E4\u00F6");
    }

    @Test
    public void shouldDecodeHexToRawWithUtf8CharacterSet() {
        // "ÅÄÖ" in UTF-8 bytes: Å=c385 Ä=c384 Ö=c396
        final String hexToRaw = "HEXTORAW('c385c384c396')";

        final Column column = Column.editor()
                .name("DATA")
                .type("VARCHAR2")
                .jdbcType(OracleTypes.VARCHAR)
                .create();

        final Field field = new Field("DATA", 0, SchemaBuilder.string().optional().build());

        final Object result = convertersUtf8.convertString(column, field, hexToRaw);
        assertThat(result).isEqualTo("\u00C5\u00C4\u00D6");
    }

    @Test
    public void shouldDecodeHexToRawAsciiIdenticallyForBothCharsets() {
        // Pure ASCII is identical in both UTF-8 and Latin-1
        final String hexToRaw = "HEXTORAW('48454c4c4f')"; // "HELLO"

        final Column column = Column.editor()
                .name("DATA")
                .type("VARCHAR2")
                .jdbcType(OracleTypes.VARCHAR)
                .create();

        final Field field = new Field("DATA", 0, SchemaBuilder.string().optional().build());

        assertThat(convertersUtf8.convertString(column, field, hexToRaw)).isEqualTo("HELLO");
        assertThat(convertersLatin1.convertString(column, field, hexToRaw)).isEqualTo("HELLO");
    }

    @Test
    public void shouldNotCorruptLatin1BytesWhenDecodedAsLatin1() {
        // This is the exact bug scenario: byte 0xC4 (Ä in Latin-1) is NOT a valid
        // single-byte UTF-8 sequence. When decoded as UTF-8, it produces U+FFFD
        // (replacement character). When decoded as Latin-1, it correctly produces Ä.
        final String hexToRaw = "HEXTORAW('c4')";

        final Column column = Column.editor()
                .name("DATA")
                .type("VARCHAR2")
                .jdbcType(OracleTypes.VARCHAR)
                .create();

        final Field field = new Field("DATA", 0, SchemaBuilder.string().optional().build());

        final Object result = convertersLatin1.convertString(column, field, hexToRaw);

        // Must be Ä (U+00C4), NOT U+FFFD (replacement character)
        assertThat(result).isEqualTo("\u00C4");
        assertThat(result).isNotEqualTo("\uFFFD");
    }

    @Test
    @FixFor("debezium/dbz#1286")
    public void shouldConvertBcTimestampPreservingEra() {
        final Column column = Column.editor()
                .name("TS")
                .type("TIMESTAMP")
                .jdbcType(Types.TIMESTAMP)
                .scale(6)
                .optional(true)
                .create();
        final Field field = adaptiveFieldFor(column);

        // Oracle year -2018 (2018 BC) is ISO proleptic year -2017
        final LocalDateTime bc = LocalDateTime.of(-2017, 3, 27, 12, 34, 56, 7_890_000);
        final Object result = convertersUtf8.converter(column, field).convert(new TIMESTAMP(bc));
        assertThat(result).isEqualTo(toEpochMicros(bc));
    }

    @Test
    @FixFor("debezium/dbz#1286")
    public void shouldConvertBcDatePreservingEra() throws Exception {
        final Column column = dateColumn();
        final Field field = adaptiveFieldFor(column);

        final LocalDateTime bc = LocalDateTime.of(-2017, 3, 27, 0, 0);
        assertThat(convertersUtf8.converter(column, field).convert(new DATE(bc)))
                .isEqualTo(bc.toInstant(ZoneOffset.UTC).toEpochMilli());

        // 1 BC (Oracle year -1) is ISO proleptic year 0
        final LocalDateTime oneBc = LocalDateTime.of(0, 12, 31, 23, 59, 59);
        assertThat(convertersUtf8.converter(column, field).convert(new DATE(oneBc)))
                .isEqualTo(oneBc.toInstant(ZoneOffset.UTC).toEpochMilli());

        // Oracle's minimum date, 4712 BC
        final LocalDateTime minimum = LocalDateTime.of(-4711, 1, 1, 0, 0);
        assertThat(convertersUtf8.converter(column, field).convert(new DATE(minimum)))
                .isEqualTo(minimum.toInstant(ZoneOffset.UTC).toEpochMilli());
    }

    @Test
    @FixFor("debezium/dbz#1286")
    public void shouldConvertBcTimestampWithTimeZonePreservingEra() throws Exception {
        final Column column = Column.editor()
                .name("TSTZ")
                .type("TIMESTAMP WITH TIME ZONE")
                .jdbcType(OracleTypes.TIMESTAMPTZ)
                .scale(6)
                .optional(true)
                .create();
        final Field field = adaptiveFieldFor(column);

        final OffsetDateTime bc = OffsetDateTime.of(-2017, 3, 27, 1, 34, 56, 7_890_000, ZoneOffset.ofHours(-11));
        final Object result = convertersUtf8.converter(column, field).convert(new TIMESTAMPTZ(bc));
        assertThat(result).isEqualTo("-2017-03-27T01:34:56.007890-11:00");

        // The era-suffixed streaming representation, as mined with NLS_TIMESTAMP_TZ_FORMAT
        final Object streamed = convertersUtf8.converter(column, field)
                .convert("TO_TIMESTAMP_TZ('2018-03-27 01:34:56.007890000 -11:00 BC')");
        assertThat(streamed).isEqualTo("-2017-03-27T01:34:56.007890-11:00");
    }

    @Test
    @FixFor("debezium/dbz#1286")
    public void shouldConvertPreGregorianCutoverDateWithoutDayShift() throws Exception {
        final Column column = dateColumn();
        final Field field = adaptiveFieldFor(column);

        // java.sql.Timestamp interprets this date in the Julian calendar, shifting it by 10 days
        final LocalDateTime preCutover = LocalDateTime.of(1500, 3, 1, 0, 0);
        assertThat(convertersUtf8.converter(column, field).convert(new DATE(preCutover)))
                .isEqualTo(preCutover.toInstant(ZoneOffset.UTC).toEpochMilli());
    }

    @Test
    public void shouldPreserveStructuredTimestampBeyondNanoTimestampRange() {
        final Column column = Column.editor()
                .name("TS")
                .type("TIMESTAMP")
                .jdbcType(Types.TIMESTAMP)
                .scale(9)
                .optional(true)
                .create();
        final Field field = fieldFor(column);

        final Struct result = (Struct) convertersStructured.converter(column, field)
                .convert(new TIMESTAMP(LocalDateTime.of(9999, 12, 31, 23, 59, 59, 999_999_999)));

        assertThat(field.schema().name()).isEqualTo(StructuredTimestamp.SCHEMA_NAME);
        assertThat(field.schema().parameters()).isNullOrEmpty();
        assertThat(result.getInt32(StructuredTemporal.PRECISION_FIELD)).isEqualTo(9);
        assertThat(result.getString(StructuredTemporal.SPECIAL_VALUE_FIELD)).isNull();
        assertThat(result.getInt32(StructuredTemporal.YEAR_FIELD)).isEqualTo(9999);
        assertThat(result.getInt8(StructuredTemporal.MONTH_FIELD)).isEqualTo((byte) 12);
        assertThat(result.getInt8(StructuredTemporal.DAY_FIELD)).isEqualTo((byte) 31);
        assertThat(result.getInt8(StructuredTemporal.HOUR_FIELD)).isEqualTo((byte) 23);
        assertThat(result.getInt8(StructuredTemporal.MINUTE_FIELD)).isEqualTo((byte) 59);
        assertThat(result.getInt8(StructuredTemporal.SECOND_FIELD)).isEqualTo((byte) 59);
        assertThat(result.getInt32(StructuredTemporal.NANOS_FIELD)).isEqualTo(999_999_999);
    }

    @Test
    public void shouldPreserveStructuredTimestampWithTimeZoneBeyondNanoTimestampRange() throws Exception {
        final Column column = Column.editor()
                .name("TSTZ")
                .type("TIMESTAMP WITH TIME ZONE")
                .jdbcType(OracleTypes.TIMESTAMPTZ)
                .scale(9)
                .optional(true)
                .create();
        final Field field = fieldFor(column);

        final Struct result = (Struct) convertersStructured.converter(column, field)
                .convert(new TIMESTAMPTZ(OffsetDateTime.of(9999, 12, 31, 23, 59, 59, 999_999_999, ZoneOffset.ofHours(14))));

        assertThat(field.schema().name()).isEqualTo(StructuredZonedTimestamp.SCHEMA_NAME);
        assertThat(field.schema().parameters()).isNullOrEmpty();
        assertThat(result.getInt32(StructuredTemporal.PRECISION_FIELD)).isEqualTo(9);
        assertThat(result.getString(StructuredTemporal.SPECIAL_VALUE_FIELD)).isNull();
        assertThat(result.getInt32(StructuredTemporal.YEAR_FIELD)).isEqualTo(9999);
        assertThat(result.getInt8(StructuredTemporal.MONTH_FIELD)).isEqualTo((byte) 12);
        assertThat(result.getInt8(StructuredTemporal.DAY_FIELD)).isEqualTo((byte) 31);
        assertThat(result.getInt8(StructuredTemporal.HOUR_FIELD)).isEqualTo((byte) 23);
        assertThat(result.getInt8(StructuredTemporal.MINUTE_FIELD)).isEqualTo((byte) 59);
        assertThat(result.getInt8(StructuredTemporal.SECOND_FIELD)).isEqualTo((byte) 59);
        assertThat(result.getInt32(StructuredTemporal.NANOS_FIELD)).isEqualTo(999_999_999);
        assertThat(result.getInt32(StructuredTemporal.OFFSET_SECONDS_FIELD)).isEqualTo(50_400);
    }

    @Test
    public void shouldPreserveStructuredIntervalComponents() {
        final Column yearMonthColumn = Column.editor()
                .name("YTM")
                .type("INTERVAL YEAR TO MONTH")
                .jdbcType(OracleTypes.INTERVALYM)
                .optional(true)
                .create();
        final Field yearMonthField = fieldFor(yearMonthColumn);

        final Struct yearMonth = (Struct) convertersStructured.converter(yearMonthColumn, yearMonthField)
                .convert(new INTERVALYM("-123-11"));

        assertThat(yearMonthField.schema().name()).isEqualTo(StructuredDuration.SCHEMA_NAME);
        assertThat(yearMonthField.schema().parameters()).isNullOrEmpty();
        assertThat(yearMonth.getInt32(StructuredTemporal.YEARS_FIELD)).isEqualTo(-123);
        assertThat(yearMonth.getInt32(StructuredTemporal.MONTHS_FIELD)).isEqualTo(-11);
        assertThat(yearMonth.getInt32(StructuredTemporal.DAYS_FIELD)).isZero();

        final Column daySecondColumn = Column.editor()
                .name("DTS")
                .type("INTERVAL DAY TO SECOND")
                .jdbcType(OracleTypes.INTERVALDS)
                .scale(9)
                .optional(true)
                .create();
        final Field daySecondField = fieldFor(daySecondColumn);

        final Struct daySecond = (Struct) convertersStructured.converter(daySecondColumn, daySecondField)
                .convert(new INTERVALDS("-999 23:59:59.999999999"));

        assertThat(daySecondField.schema().name()).isEqualTo(StructuredDuration.SCHEMA_NAME);
        assertThat(daySecondField.schema().parameters()).isNullOrEmpty();
        assertThat(daySecond.getInt32(StructuredTemporal.PRECISION_FIELD)).isEqualTo(9);
        assertThat(daySecond.getInt32(StructuredTemporal.DAYS_FIELD)).isEqualTo(-999);
        assertThat(daySecond.getInt32(StructuredTemporal.HOURS_FIELD)).isEqualTo(-23);
        assertThat(daySecond.getInt32(StructuredTemporal.MINUTES_FIELD)).isEqualTo(-59);
        assertThat(daySecond.getInt64(StructuredTemporal.SECONDS_FIELD)).isEqualTo(-59L);
        assertThat(daySecond.getInt32(StructuredTemporal.NANOS_FIELD)).isEqualTo(-999_999_999);
    }

    private Field fieldFor(Column column) {
        final Schema schema = convertersStructured.schemaBuilder(column).optional().build();
        return new Field(column.name(), 0, schema);
    }

    private Field adaptiveFieldFor(Column column) {
        final Schema schema = convertersUtf8.schemaBuilder(column).optional().build();
        return new Field(column.name(), 0, schema);
    }

    private static Column dateColumn() {
        return Column.editor()
                .name("VAL_DATE")
                .type("DATE")
                .jdbcType(Types.TIMESTAMP)
                .scale(0)
                .optional(true)
                .create();
    }

    private static long toEpochMicros(LocalDateTime dateTime) {
        final Instant instant = dateTime.toInstant(ZoneOffset.UTC);
        return instant.getEpochSecond() * 1_000_000 + instant.getNano() / 1_000;
    }
}
