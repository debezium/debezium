/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import static io.debezium.connector.oracle.util.OracleZeroDates.ZERO_DATE_BYTES;
import static io.debezium.connector.oracle.util.OracleZeroDates.ZERO_TIMESTAMP_BYTES;
import static io.debezium.connector.oracle.util.OracleZeroDates.ZERO_TIMESTAMP_TZ_BYTES;
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
import io.debezium.connector.oracle.jdbc.OracleDatabaseSettings;
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
import oracle.sql.TIMESTAMPLTZ;
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
    private OracleValueConverters convertersIntervalString;

    @BeforeEach
    void setUp() {
        final Configuration configuration = TestHelper.defaultConfig().build();
        final OracleConnectorConfig connectorConfig = new OracleConnectorConfig(configuration);

        // The value converters only use the connection to convert timestamps
        final OracleConnection connection = Mockito.mock(OracleConnection.class);

        // Converter with AL32UTF8 database charset (common case)
        final OracleDatabaseSettings utf8Settings = settings(CharacterSet.AL32UTF8_CHARSET);
        convertersUtf8 = connectorConfig.getAdapter().getValueConverter(connectorConfig, connection, utf8Settings);

        // Converter with WE8ISO8859P1 (Latin-1) database charset
        convertersLatin1 = connectorConfig.getAdapter().getValueConverter(connectorConfig, connection, settings(CharacterSet.WE8ISO8859P1_CHARSET));

        final Configuration structuredConfiguration = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.TIME_PRECISION_MODE, TemporalPrecisionMode.STRUCTURED)
                .build();
        final OracleConnectorConfig structuredConnectorConfig = new OracleConnectorConfig(structuredConfiguration);
        convertersStructured = structuredConnectorConfig.getAdapter().getValueConverter(structuredConnectorConfig, connection, utf8Settings);

        final Configuration intervalStringConfiguration = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.INTERVAL_HANDLING_MODE, OracleConnectorConfig.IntervalHandlingMode.STRING.getValue())
                .build();
        final OracleConnectorConfig intervalStringConnectorConfig = new OracleConnectorConfig(intervalStringConfiguration);
        convertersIntervalString = intervalStringConnectorConfig.getAdapter().getValueConverter(intervalStringConnectorConfig, connection, utf8Settings);
    }

    private static OracleDatabaseSettings settings(int databaseCharacterSet) {
        return new OracleDatabaseSettings(CharacterSet.make(databaseCharacterSet), CharacterSet.make(CharacterSet.AL16UTF16_CHARSET), false, false);
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
    @FixFor("debezium/dbz#3055")
    public void shouldConvertZeroDateToNullForOptionalColumn() {
        final Column column = dateColumn();
        final Field field = adaptiveFieldFor(column);

        assertThat(convertersUtf8.converter(column, field).convert(new DATE(ZERO_DATE_BYTES))).isNull();
        assertThat(convertersUtf8.converter(column, field).convert("HEXTORAW('64640000010101')")).isNull();
        assertThat(convertersUtf8.converter(column, field).convert("TO_DATE('0000-00-00 00:00:00', 'YYYY-MM-DD HH24:MI:SS')")).isNull();

        final Column tsColumn = timestampColumn(true);
        final Field tsField = adaptiveFieldFor(tsColumn);
        assertThat(convertersUtf8.converter(tsColumn, tsField).convert(new TIMESTAMP(ZERO_TIMESTAMP_BYTES))).isNull();
        assertThat(convertersUtf8.converter(tsColumn, tsField).convert("HEXTORAW('6464000001010100000000')")).isNull();
        assertThat(convertersUtf8.converter(tsColumn, tsField).convert("TO_TIMESTAMP('0000-00-00 00:00:00.000000')")).isNull();
    }

    @Test
    @FixFor("debezium/dbz#3055")
    public void shouldConvertZeroDateToDefaultForRequiredColumnWithDefault() {
        final Column column = requiredDateColumn();
        final long defaultValue = LocalDateTime.of(2000, 1, 1, 0, 0).toInstant(ZoneOffset.UTC).toEpochMilli();
        final Field field = new Field(column.name(), 0, convertersUtf8.schemaBuilder(column).defaultValue(defaultValue).build());

        assertThat(convertersUtf8.converter(column, field).convert(new DATE(ZERO_DATE_BYTES))).isEqualTo(defaultValue);
        assertThat(convertersUtf8.converter(column, field).convert("TO_DATE('0000-00-00 00:00:00', 'YYYY-MM-DD HH24:MI:SS')"))
                .isEqualTo(defaultValue);
    }

    @Test
    @FixFor("debezium/dbz#3055")
    public void shouldConvertZeroDateToEpochForRequiredColumnWithoutDefault() {
        final Column column = requiredDateColumn();
        final Field field = new Field(column.name(), 0, convertersUtf8.schemaBuilder(column).build());

        assertThat(convertersUtf8.converter(column, field).convert(new DATE(ZERO_DATE_BYTES))).isEqualTo(0L);
        assertThat(convertersUtf8.converter(column, field).convert("HEXTORAW('64640000010101')")).isEqualTo(0L);
        assertThat(convertersUtf8.converter(column, field).convert("TO_DATE('0000-00-00 00:00:00', 'YYYY-MM-DD HH24:MI:SS')")).isEqualTo(0L);

        final Column tsColumn = timestampColumn(false);
        final Field tsField = new Field(tsColumn.name(), 0, convertersUtf8.schemaBuilder(tsColumn).build());
        assertThat(convertersUtf8.converter(tsColumn, tsField).convert(new TIMESTAMP(ZERO_TIMESTAMP_BYTES))).isEqualTo(0L);
    }

    @Test
    @FixFor("debezium/dbz#3055")
    public void shouldConvertZeroDateWithTimeZoneToNullForOptionalColumn() {
        final Column tstzColumn = timestampWithTimeZoneColumn(OracleTypes.TIMESTAMPTZ, "TIMESTAMP(6) WITH TIME ZONE", true);
        final Field tstzField = adaptiveFieldFor(tstzColumn);
        assertThat(convertersUtf8.converter(tstzColumn, tstzField).convert(new TIMESTAMPTZ(ZERO_TIMESTAMP_TZ_BYTES))).isNull();
        assertThat(convertersUtf8.converter(tstzColumn, tstzField).convert("HEXTORAW('6464000001010100000000143c')")).isNull();
        assertThat(convertersUtf8.converter(tstzColumn, tstzField).convert("TO_TIMESTAMP_TZ('0000-00-00 00:00:00.000000000 +00:00 AD')")).isNull();

        final Column tsltzColumn = timestampWithTimeZoneColumn(OracleTypes.TIMESTAMPLTZ, "TIMESTAMP(6) WITH LOCAL TIME ZONE", true);
        final Field tsltzField = adaptiveFieldFor(tsltzColumn);
        assertThat(convertersUtf8.converter(tsltzColumn, tsltzField).convert(new TIMESTAMPLTZ(ZERO_TIMESTAMP_BYTES))).isNull();
        assertThat(convertersUtf8.converter(tsltzColumn, tsltzField).convert("TO_TIMESTAMP_TZ('0000-00-00 00:00:00.000000000 +00:00 AD')")).isNull();
    }

    @Test
    @FixFor("debezium/dbz#3055")
    public void shouldConvertZeroDateWithTimeZoneToEpochForRequiredColumnWithoutDefault() {
        final Column tstzColumn = timestampWithTimeZoneColumn(OracleTypes.TIMESTAMPTZ, "TIMESTAMP(6) WITH TIME ZONE", false);
        final Field tstzField = new Field(tstzColumn.name(), 0, convertersUtf8.schemaBuilder(tstzColumn).build());
        assertThat(convertersUtf8.converter(tstzColumn, tstzField).convert(new TIMESTAMPTZ(ZERO_TIMESTAMP_TZ_BYTES))).isEqualTo("1970-01-01T00:00:00Z");
        assertThat(convertersUtf8.converter(tstzColumn, tstzField).convert("TO_TIMESTAMP_TZ('0000-00-00 00:00:00.000000000 +00:00 AD')"))
                .isEqualTo("1970-01-01T00:00:00Z");

        final Column tsltzColumn = timestampWithTimeZoneColumn(OracleTypes.TIMESTAMPLTZ, "TIMESTAMP(6) WITH LOCAL TIME ZONE", false);
        final Field tsltzField = new Field(tsltzColumn.name(), 0, convertersUtf8.schemaBuilder(tsltzColumn).build());
        assertThat(convertersUtf8.converter(tsltzColumn, tsltzField).convert(new TIMESTAMPLTZ(ZERO_TIMESTAMP_BYTES))).isEqualTo("1970-01-01T00:00:00Z");

        // A zero year with a valid month and day is not a zero date
        assertThat(convertersUtf8.converter(tstzColumn, tstzField).convert("TO_TIMESTAMP_TZ('0001-01-02 00:00:00.000000000 +00:00 BC')"))
                .isEqualTo("0000-01-02T00:00:00.000000Z");
    }

    @Test
    @FixFor("debezium/dbz#3055")
    public void shouldStillConvertZeroYearWithValidMonthAndDay() throws Exception {
        final Column column = dateColumn();
        final Field field = adaptiveFieldFor(column);

        // Oracle year 1 BC is ISO proleptic year 0; only a zero month or day denotes a zero date
        final LocalDateTime oneBc = LocalDateTime.of(0, 1, 2, 0, 0);
        assertThat(convertersUtf8.converter(column, field).convert(new DATE(oneBc)))
                .isEqualTo(oneBc.toInstant(ZoneOffset.UTC).toEpochMilli());
        assertThat(convertersUtf8.converter(column, field).convert("TO_DATE('0000-01-02 00:00:00', 'YYYY-MM-DD HH24:MI:SS')"))
                .isEqualTo(oneBc.toInstant(ZoneOffset.UTC).toEpochMilli());
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

    @Test
    @FixFor("debezium/dbz#2945")
    public void shouldScaleIntervalDaySecondFractionAsMicrosecondsInNumericMode() {
        final Column column = intervalDaySecondColumn(9);
        final Field field = adaptiveFieldFor(column);

        assertThat(convertersUtf8.converter(column, field).convert(new INTERVALDS("0 00:00:00.123456789"))).isEqualTo(123_456L);
        assertThat(convertersUtf8.converter(column, field).convert(new INTERVALDS("0 00:00:00.123456"))).isEqualTo(123_456L);
        assertThat(convertersUtf8.converter(column, field).convert(new INTERVALDS("0 00:00:00.000000999"))).isEqualTo(0L);
        assertThat(convertersUtf8.converter(column, field).convert(new INTERVALDS("111 10:09:08.555444333"))).isEqualTo(9_626_948_555_444L);
        assertThat(convertersUtf8.converter(column, field).convert(new INTERVALDS("-7 05:12:10.123456789"))).isEqualTo(-623_530_123_456L);
    }

    @Test
    @FixFor("debezium/dbz#2945")
    public void shouldPreserveIntervalDaySecondFractionInStringMode() {
        final Column column = intervalDaySecondColumn(9);
        final Field field = new Field(column.name(), 0, convertersIntervalString.schemaBuilder(column).optional().build());

        assertThat(convertersIntervalString.converter(column, field).convert(new INTERVALDS("0 00:00:00.123456789")))
                .isEqualTo("P0Y0M0DT0H0M0.123456789S");
        assertThat(convertersIntervalString.converter(column, field).convert(new INTERVALDS("111 10:09:08.555444333")))
                .isEqualTo("P0Y0M111DT10H9M8.555444333S");
        assertThat(convertersIntervalString.converter(column, field).convert(new INTERVALDS("-7 05:12:10.0123")))
                .isEqualTo("P0Y0M-7DT-5H-12M-10.0123S");
        assertThat(convertersIntervalString.converter(column, field).convert(new INTERVALDS("0 00:00:00.0")))
                .isEqualTo("P0Y0M0DT0H0M0S");
    }

    private Field fieldFor(Column column) {
        final Schema schema = convertersStructured.schemaBuilder(column).optional().build();
        return new Field(column.name(), 0, schema);
    }

    private Field adaptiveFieldFor(Column column) {
        final Schema schema = convertersUtf8.schemaBuilder(column).optional().build();
        return new Field(column.name(), 0, schema);
    }

    private static Column intervalDaySecondColumn(int scale) {
        return Column.editor()
                .name("DTS")
                .type("INTERVAL DAY TO SECOND")
                .jdbcType(OracleTypes.INTERVALDS)
                .scale(scale)
                .optional(true)
                .create();
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

    private static Column requiredDateColumn() {
        return Column.editor()
                .name("VAL_DATE")
                .type("DATE")
                .jdbcType(Types.TIMESTAMP)
                .scale(0)
                .optional(false)
                .create();
    }

    private static Column timestampWithTimeZoneColumn(int jdbcType, String typeName, boolean optional) {
        return Column.editor()
                .name("TSTZ")
                .type(typeName)
                .jdbcType(jdbcType)
                .scale(6)
                .optional(optional)
                .create();
    }

    private static Column timestampColumn(boolean optional) {
        return Column.editor()
                .name("TS")
                .type("TIMESTAMP")
                .jdbcType(Types.TIMESTAMP)
                .scale(6)
                .optional(optional)
                .create();
    }

    private static long toEpochMicros(LocalDateTime dateTime) {
        final Instant instant = dateTime.toInstant(ZoneOffset.UTC);
        return instant.getEpochSecond() * 1_000_000 + instant.getNano() / 1_000;
    }
}
