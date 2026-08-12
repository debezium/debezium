/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import static io.debezium.util.NumberConversions.BYTE_FALSE;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import io.debezium.DebeziumException;
import io.debezium.config.CommonConnectorConfig.BinaryHandlingMode;
import io.debezium.connector.oracle.logminer.UnistrHelper;
import io.debezium.connector.oracle.util.TimestampUtils;
import io.debezium.data.Json;
import io.debezium.data.SpecialValueDecimal;
import io.debezium.data.VariableScaleDecimal;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.jdbc.ResultReceiver;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.Column;
import io.debezium.relational.ValueConverter;
import io.debezium.time.Date;
import io.debezium.time.Interval;
import io.debezium.time.MicroDuration;
import io.debezium.time.StructuredDuration;
import io.debezium.time.StructuredZonedTimestamp;
import io.debezium.time.ZonedTimestamp;
import io.debezium.util.NumberConversions;
import io.debezium.util.Strings;

import oracle.jdbc.OracleTypes;
import oracle.sql.BINARY_DOUBLE;
import oracle.sql.BINARY_FLOAT;
import oracle.sql.CHAR;
import oracle.sql.CharacterSet;
import oracle.sql.DATE;
import oracle.sql.INTERVALDS;
import oracle.sql.INTERVALYM;
import oracle.sql.NUMBER;
import oracle.sql.RAW;
import oracle.sql.TIMESTAMP;
import oracle.sql.TIMESTAMPLTZ;
import oracle.sql.TIMESTAMPTZ;
import oracle.sql.json.OracleJsonFactory;

public class OracleValueConverters extends JdbcValueConverters {

    /**
     * Marker value indicating an unavilable column value.
     */
    public static final Object UNAVAILABLE_VALUE = new Object();
    public static final String EMPTY_BLOB_FUNCTION = "EMPTY_BLOB()";
    public static final String EMPTY_CLOB_FUNCTION = "EMPTY_CLOB()";
    public static final String EMPTY_EXTENDED_STRING = "LM_EMPTY_STRING";
    public static final String HEXTORAW_FUNCTION_START = "HEXTORAW('";
    public static final String HEXTORAW_FUNCTION_END = "')";

    private static final Pattern INTERVAL_DAY_SECOND_PATTERN = Pattern.compile("([+\\-])?(\\d+) (\\d+):(\\d+):(\\d+).(\\d+)");

    private static final DateTimeFormatter TIMESTAMP_TZ_FORMATTER = new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .appendPattern("yyyy-MM-dd HH:mm:ss")
            .optionalStart()
            .appendPattern(".")
            .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, false)
            .optionalEnd()
            .optionalStart()
            .appendPattern(" ")
            .optionalEnd()
            .appendOffset("+HH:MM", "")
            .toFormatter();

    private static final Pattern TO_TIMESTAMP_TZ = Pattern.compile("TO_TIMESTAMP_TZ\\('(.*)'\\)", Pattern.CASE_INSENSITIVE);
    private static final BigDecimal MICROSECONDS_PER_SECOND = new BigDecimal(1_000_000);

    private final OracleJsonFactory jsonFactory = new OracleJsonFactory();

    private final OracleConnection connection;
    private final boolean legacyDecimalModeStrategy;
    private final OracleConnectorConfig.IntervalHandlingMode intervalHandlingMode;
    private final byte[] unavailableValuePlaceholderBinary;
    private final String unavailableValuePlaceholderString;
    private final CharacterSet nationalCharacterSet;
    private final CharacterSet databaseCharacterSet;

    public OracleValueConverters(OracleConnectorConfig config, OracleConnection connection) {
        super(config.getDecimalMode(), config.getTemporalPrecisionMode(), ZoneOffset.UTC, null, null, config.binaryHandlingMode());
        this.connection = connection;
        this.legacyDecimalModeStrategy = config.isUsingLegacyDecimalHandlingStrategy();
        this.intervalHandlingMode = config.getIntervalHandlingMode();
        this.unavailableValuePlaceholderBinary = config.getUnavailableValuePlaceholder();
        this.unavailableValuePlaceholderString = new String(config.getUnavailableValuePlaceholder());
        this.nationalCharacterSet = connection.getNationalCharacterSet();
        this.databaseCharacterSet = connection.getDatabaseCharacterSet();
    }

    public byte[] getUnavailableValuePlaceholderBinary() {
        return unavailableValuePlaceholderBinary;
    }

    public String getUnavailableValuePlaceholderString() {
        return unavailableValuePlaceholderString;
    }

    @Override
    public SchemaBuilder schemaBuilder(Column column) {
        logger.debug("Building schema for column {} of type {} named {} with constraints ({},{})",
                column.name(),
                column.jdbcType(),
                column.typeName(),
                column.length(),
                column.scale());

        return switch (column.jdbcType()) {
            case Types.FLOAT ->
                // Oracle's float is not float as in Java but a NUMERIC without scale
                variableScaleSchema(column);
            case Types.NUMERIC -> getNumericSchema(column);
            case OracleTypes.BINARY_FLOAT -> SchemaBuilder.float32();
            case OracleTypes.BINARY_DOUBLE -> SchemaBuilder.float64();
            case OracleTypes.TIMESTAMPTZ, OracleTypes.TIMESTAMPLTZ -> {
                if (temporalPrecisionMode == TemporalPrecisionMode.STRUCTURED) {
                    yield StructuredZonedTimestamp.builder();
                }
                yield ZonedTimestamp.builder();
            }
            case OracleTypes.INTERVALDS -> {
                if (temporalPrecisionMode == TemporalPrecisionMode.STRUCTURED) {
                    yield StructuredDuration.builder();
                }
                yield intervalHandlingMode == OracleConnectorConfig.IntervalHandlingMode.STRING ? Interval.builder() : MicroDuration.builder();
            }
            case OracleTypes.INTERVALYM -> {
                if (temporalPrecisionMode == TemporalPrecisionMode.STRUCTURED) {
                    yield StructuredDuration.builder();
                }
                yield intervalHandlingMode == OracleConnectorConfig.IntervalHandlingMode.STRING ? Interval.builder() : MicroDuration.builder();
            }
            case Types.STRUCT -> SchemaBuilder.string();
            case OracleTypes.ROWID -> SchemaBuilder.string();
            default -> {
                if ("JSON".equals(column.typeName())) {
                    yield Json.builder();
                }

                final SchemaBuilder builder = super.schemaBuilder(column);
                logger.debug("JdbcValueConverters returned '{}' for column '{}'", builder != null ? builder.getClass().getName() : null, column.name());
                yield builder;
            }
        };
    }

    @Override
    protected int getTimePrecision(Column column) {
        // INTERVALDS stores its fractional-second precision in scale; TIMESTAMP precision is normalized to length
        // in OracleConnection#overrideColumn.
        return column.scale().orElse(column.length());
    }

    private SchemaBuilder getNumericSchema(Column column) {
        if (column.scale().isPresent()) {
            // return sufficiently sized int schema for non-floating point types
            Integer scale = column.scale().get();

            if (!legacyDecimalModeStrategy && scale == 0 && decimalMode != DecimalMode.PRECISE) {
                return SpecialValueDecimal.builder(decimalMode, column.length(), 0);
            }

            // a negative scale means rounding, e.g. NUMBER(10, -2) would be rounded to hundreds
            if (scale <= 0) {
                int width = column.length() - scale;
                if (width < 3) {
                    return SchemaBuilder.int8();
                }
                else if (width < 5) {
                    return SchemaBuilder.int16();
                }
                else if (width < 10) {
                    return SchemaBuilder.int32();
                }
                else if (width < 19) {
                    return SchemaBuilder.int64();
                }
            }

            // larger non-floating point types and floating point types use Decimal
            return super.schemaBuilder(column);
        }
        else if (!legacyDecimalModeStrategy && column.length() == 0) {
            // Defined as NUMBER without specifying a length and scale, treat as NUMBER(38,0)
            if (decimalMode != DecimalMode.PRECISE) {
                return SpecialValueDecimal.builder(decimalMode, 38, 0);
            }
        }

        return variableScaleSchema(column);
    }

    private SchemaBuilder variableScaleSchema(Column column) {
        if (decimalMode == DecimalMode.PRECISE) {
            return VariableScaleDecimal.builder();
        }
        return SpecialValueDecimal.builder(decimalMode, column.length(), column.scale().orElse(-1));
    }

    @Override
    public ValueConverter converter(Column column, Field fieldDefn) {
        return switch (column.jdbcType()) {
            case Types.CHAR, Types.VARCHAR, Types.NCHAR, Types.NVARCHAR, Types.STRUCT, Types.CLOB, OracleTypes.ROWID ->
                data -> convertString(column, fieldDefn, data);
            case Types.BLOB -> data -> convertBinary(column, fieldDefn, data, binaryMode);
            case OracleTypes.BINARY_FLOAT -> data -> convertFloat(column, fieldDefn, data);
            case OracleTypes.BINARY_DOUBLE -> data -> convertDouble(column, fieldDefn, data);
            case Types.NUMERIC -> getNumericConverter(column, fieldDefn);
            case Types.FLOAT -> data -> convertVariableScale(column, fieldDefn, data);
            case OracleTypes.TIMESTAMPTZ -> (data) -> convertTimestampWithZone(column, fieldDefn, data);
            case OracleTypes.TIMESTAMPLTZ -> (data) -> convertTimestampWithLocalZone(column, fieldDefn, data);
            case OracleTypes.INTERVALYM -> (data) -> convertIntervalYearMonth(column, fieldDefn, data);
            case OracleTypes.INTERVALDS -> (data) -> convertIntervalDaySecond(column, fieldDefn, data);
            case OracleTypes.RAW -> (data) -> convertBinary(column, fieldDefn, data, binaryMode);
            default -> {
                if ("JSON".equals(column.typeName())) {
                    yield (data) -> convertJson(column, fieldDefn, data);
                }
                yield super.converter(column, fieldDefn);
            }
        };
    }

    private ValueConverter getNumericConverter(Column column, Field fieldDefn) {
        if (column.scale().isPresent()) {
            Integer scale = column.scale().get();

            if (!legacyDecimalModeStrategy && scale == 0 && decimalMode != DecimalMode.PRECISE) {
                return data -> convertVariableScale(column, fieldDefn, data);
            }

            if (scale <= 0) {
                int width = column.length() - scale;
                if (width < 3) {
                    return data -> convertNumericAsTinyInt(column, fieldDefn, data);
                }
                else if (width < 5) {
                    return data -> convertNumericAsSmallInt(column, fieldDefn, data);
                }
                else if (width < 10) {
                    return data -> convertNumericAsInteger(column, fieldDefn, data);
                }
                else if (width < 19) {
                    return data -> convertNumericAsBigInteger(column, fieldDefn, data);
                }
            }

            // larger non-floating point types and floating point types use Decimal
            return data -> convertNumeric(column, fieldDefn, data);
        }

        return data -> convertVariableScale(column, fieldDefn, data);
    }

    @Override
    protected BigDecimal withScaleAdjustedIfNeeded(Column column, BigDecimal data) {
        // deal with Oracle negative scales
        if (column.scale().isPresent() && column.scale().get() < data.scale()) {
            data = data.setScale(column.scale().get());
        }
        return super.withScaleAdjustedIfNeeded(column, data);
    }

    @Override
    protected Object convertString(Column column, Field fieldDefn, Object data) {
        if (data instanceof CHAR charData) {
            return charData.stringValue();
        }
        if (data instanceof Clob clob) {
            try {
                // use buffered read for large CLOB values
                if (clob.length() >= Integer.MAX_VALUE) {
                    try (
                            BufferedReader reader = new BufferedReader(clob.getCharacterStream());
                            StringWriter writer = new StringWriter()) {
                        reader.transferTo(writer);
                        return writer.toString();
                    }
                }
                else {
                    // use non-buffered read for smaller values
                    // Note that java.sql.Clob specifies that the first character starts at 1
                    // and that length must be greater-than or equal to 0. So for an empty
                    // clob field, a call to getSubString(1, 0) is perfectly valid.
                    return clob.getSubString(1, (int) clob.length());
                }
            }
            catch (SQLException | IOException e) {
                throw new DebeziumException("Couldn't read binary data for column " + column.name(), e);
            }
        }
        if (data instanceof String s) {
            if (EMPTY_CLOB_FUNCTION.equals(s) || EMPTY_EXTENDED_STRING.equals(s)) {
                return column.isOptional() ? null : "";
            }
            else if (UnistrHelper.isUnistrFunction(s)) {
                return UnistrHelper.convert(s);
            }
            else if (isHexToRawFunctionCall(s)) {
                data = convertHexToRawFunctionToString(column, s);
            }
        }

        if (data == UNAVAILABLE_VALUE) {
            return unavailableValuePlaceholderString;
        }

        return super.convertString(column, fieldDefn, data);
    }

    @Override
    protected Object convertBinary(Column column, Field fieldDefn, Object data, BinaryHandlingMode mode) {
        try {
            if (data instanceof String str) {
                if (EMPTY_BLOB_FUNCTION.equals(str)) {
                    if (column.isOptional()) {
                        return null;
                    }
                    data = "";
                }
                else if (isHexToRawFunctionCall(str)) {
                    data = RAW.hexString2Bytes(getHexToRawHexString(str));
                }
            }
            else if (data instanceof Blob blob) {
                if (blob.length() >= Integer.MAX_VALUE) {
                    // use buffered read to support large BLOB values
                    try (
                            BufferedInputStream inputStream = new BufferedInputStream(blob.getBinaryStream());
                            ByteArrayOutputStream writer = new ByteArrayOutputStream()) {
                        inputStream.transferTo(writer);
                        data = writer.toByteArray();
                    }
                    catch (SQLException | IOException e) {
                        throw new DebeziumException("Couldn't read binary data for column " + column.name(), e);
                    }
                }
                else {
                    // use non-buffered read for smaller BLOB values
                    data = blob.getBytes(1, (int) blob.length());
                }
            }
            else if (data instanceof RAW rawData) {
                data = rawData.getBytes();
            }

            if (data == UNAVAILABLE_VALUE) {
                data = unavailableValuePlaceholderBinary;
            }

            return super.convertBinary(column, fieldDefn, data, mode);
        }
        catch (SQLException e) {
            throw new DebeziumException("Couldn't convert value for column " + column.name(), e);
        }
    }

    protected Object convertJson(Column column, Field fieldDefn, Object data) {
        try {
            if (data instanceof String stringData) {
                if (stringData.startsWith("/* JSON */ ")) {
                    stringData = stringData.substring(11);
                }

                if (EMPTY_CLOB_FUNCTION.equals(stringData) || EMPTY_BLOB_FUNCTION.equals(stringData)) {
                    return column.isOptional() ? null : "";
                }
                else if (UnistrHelper.isUnistrFunction(stringData)) {
                    return UnistrHelper.convert(stringData);
                }
                else if (isHexToRawFunctionCall(stringData)) {
                    final byte[] jsonData = RAW.hexString2Bytes(getHexToRawHexString(stringData));
                    return jsonFactory.createJsonBinaryValue(ByteBuffer.wrap(jsonData)).asJsonObject().toString();
                }

                data = stringData;
            }
        }
        catch (SQLException e) {
            throw new DebeziumException("Couldn't convert value for json column " + column.name(), e);
        }

        if (data == UNAVAILABLE_VALUE) {
            return unavailableValuePlaceholderString;
        }

        return super.convertString(column, fieldDefn, data);
    }

    @Override
    protected Object convertInteger(Column column, Field fieldDefn, Object data) {
        if (data instanceof NUMBER numberData) {
            try {
                data = numberData.intValue();
            }
            catch (SQLException e) {
                throw new DebeziumException("Couldn't convert value for column " + column.name(), e);
            }
        }

        return super.convertInteger(column, fieldDefn, data);
    }

    @Override
    protected Object convertFloat(Column column, Field fieldDefn, Object data) {
        if (data instanceof Float) {
            return data;
        }
        else if (data instanceof NUMBER numberData) {
            return numberData.floatValue();
        }
        else if (data instanceof BINARY_FLOAT binaryFloat) {
            try {
                return binaryFloat.floatValue();
            }
            catch (SQLException e) {
                throw new DebeziumException("Couldn't convert value for column " + column.name(), e);
            }
        }
        else if (data instanceof Double doubleData) {
            return doubleData.floatValue();
        }
        else if (data instanceof String strData) {
            return Float.parseFloat(toStringFromNumericHexToRawIfApplicable(column, strData));
        }

        return super.convertFloat(column, fieldDefn, data);
    }

    @Override
    protected Object convertDouble(Column column, Field fieldDefn, Object data) {
        if (data instanceof BINARY_DOUBLE binaryDouble) {
            try {
                return binaryDouble.doubleValue();
            }
            catch (SQLException e) {
                throw new DebeziumException("Couldn't convert value for column " + column.name(), e);
            }
        }
        else if (data instanceof String strData) {
            return Double.parseDouble(toStringFromNumericHexToRawIfApplicable(column, strData));
        }

        return super.convertDouble(column, fieldDefn, data);
    }

    @Override
    protected Object convertDecimal(Column column, Field fieldDefn, Object data) {
        if (data instanceof NUMBER numberData) {
            try {
                data = numberData.bigDecimalValue();
            }
            catch (SQLException e) {
                throw new DebeziumException("Couldn't convert value for column " + column.name(), e);
            }
        }
        else if (data instanceof BigInteger bigIntData) {
            // OpenLogReplicator
            data = toBigDecimal(column, fieldDefn, bigIntData.toString());
        }
        else if (data instanceof String strData) {
            // LogMiner
            data = toBigDecimal(column, fieldDefn, toNumberFromNumericHexToRawIfApplicable(column, strData));
        }

        // adjust scale to column's scale if the column's scale is larger than the one from
        // the value (e.g. 4.4444 -> 4.444400)
        if (data instanceof BigDecimal bigDecData) {
            data = withScaleAdjustedIfNeeded(column, bigDecData);
        }

        return super.convertDecimal(column, fieldDefn, data);
    }

    @Override
    protected Object convertNumeric(Column column, Field fieldDefn, Object data) {
        return convertDecimal(column, fieldDefn, data);
    }

    protected Object convertNumericAsTinyInt(Column column, Field fieldDefn, Object data) {
        if (data instanceof NUMBER numberData) {
            try {
                data = numberData.byteValue();
            }
            catch (SQLException e) {
                throw new DebeziumException("Couldn't convert value for column " + column.name(), e);
            }
        }
        else if (data instanceof String strData) {
            data = toNumberFromNumericHexToRawIfApplicable(column, strData);
        }

        return convertTinyInt(column, fieldDefn, data);
    }

    protected Object convertNumericAsSmallInt(Column column, Field fieldDefn, Object data) {
        if (data instanceof NUMBER numberData) {
            try {
                data = numberData.shortValue();
            }
            catch (SQLException e) {
                throw new DebeziumException("Couldn't convert value for column " + column.name(), e);
            }
        }
        else if (data instanceof String strData) {
            data = toNumberFromNumericHexToRawIfApplicable(column, strData);
        }

        return super.convertSmallInt(column, fieldDefn, data);
    }

    protected Object convertNumericAsInteger(Column column, Field fieldDefn, Object data) {
        if (data instanceof NUMBER numberData) {
            try {
                data = numberData.intValue();
            }
            catch (SQLException e) {
                throw new DebeziumException("Couldn't convert value for column " + column.name(), e);
            }
        }
        else if (data instanceof String strData) {
            data = toNumberFromNumericHexToRawIfApplicable(column, strData);
        }

        return super.convertInteger(column, fieldDefn, data);
    }

    protected Object convertNumericAsBigInteger(Column column, Field fieldDefn, Object data) {
        if (data instanceof NUMBER numberData) {
            try {
                data = numberData.longValue();
            }
            catch (SQLException e) {
                throw new DebeziumException("Couldn't convert value for column " + column.name(), e);
            }
        }
        else if (data instanceof String strData) {
            data = toNumberFromNumericHexToRawIfApplicable(column, strData);
        }

        return super.convertBigInt(column, fieldDefn, data);
    }

    /**
     * Converts a value object for an expected JDBC type of {@link Types#BOOLEAN}.
     *
     * @param column    the column definition describing the {@code data} value; never null
     * @param fieldDefn the field definition; never null
     * @param data      the data object to be converted into a {@link Date Kafka Connect date} type; never null
     * @return the converted value, or null if the conversion could not be made and the column allows nulls
     * @throws IllegalArgumentException if the value could not be converted but the column does not allow nulls
     */
    @Override
    protected Object convertBoolean(Column column, Field fieldDefn, Object data) {
        if (data instanceof BigDecimal bigDecData) {
            return bigDecData.byteValue() == 0 ? Boolean.FALSE : Boolean.TRUE;
        }
        if (data instanceof String strData) {
            final var convertedData = toStringFromStringHexToRawIfApplicable(column, strData);
            return Byte.parseByte((String) convertedData) == 0 ? Boolean.FALSE : Boolean.TRUE;
        }
        if (data instanceof NUMBER numberData) {
            try {
                return numberData.intValue() == 0 ? Boolean.FALSE : Boolean.TRUE;
            }
            catch (SQLException e) {
                throw new DebeziumException("Couldn't convert value for column " + column.name(), e);
            }
        }
        return super.convertBoolean(column, fieldDefn, data);
    }

    @Override
    protected Object convertTinyInt(Column column, Field fieldDefn, Object data) {
        return convertValue(column, fieldDefn, data, BYTE_FALSE, (r) -> {
            if (data instanceof Byte) {
                r.deliver(data);
            }
            else if (data instanceof Number value) {
                r.deliver(value.byteValue());
            }
            else if (data instanceof Boolean boolData) {
                r.deliver(NumberConversions.getByte(boolData));
            }
            else if (data instanceof String strData) {
                r.deliver(Byte.parseByte(toStringFromNumericHexToRawIfApplicable(column, strData)));
            }
        });
    }

    protected Object convertVariableScale(Column column, Field fieldDefn, Object data) {
        data = convertNumeric(column, fieldDefn, data); // provides default value

        if (data == null) {
            return null;
        }
        // TODO Need to handle special values, it is not supported in variable scale decimal
        if (decimalMode == DecimalMode.PRECISE) {
            if (data instanceof SpecialValueDecimal specialDecData) {
                return VariableScaleDecimal.fromLogical(fieldDefn.schema(), specialDecData);
            }
            else if (data instanceof BigDecimal bigDecData) {
                return VariableScaleDecimal.fromLogical(fieldDefn.schema(), new SpecialValueDecimal(bigDecData));
            }
        }
        else {
            return data;
        }
        return handleUnknownData(column, fieldDefn, data);
    }

    protected Object fromOracleTimeClasses(Column column, Object data) {
        try {
            if (data instanceof TIMESTAMP timestampData) {
                data = timestampData.timestampValue();
            }
            else if (data instanceof DATE dateData) {
                data = dateData.timestampValue();
            }
            else if (data instanceof TIMESTAMPTZ ts) {
                data = ts.toZonedDateTime();
            }
            else if (data instanceof TIMESTAMPLTZ ts) {
                data = ZonedDateTime.ofInstant(ts.timestampValue(connection.connection()).toInstant(), ZoneId.systemDefault()).withZoneSameInstant(ZoneOffset.UTC);
            }
        }
        catch (SQLException e) {
            throw new DebeziumException("Couldn't convert value for column " + column.name(), e);
        }
        return data;
    }

    @Override
    protected Object convertDateToEpochDays(Column column, Field fieldDefn, Object data) {
        if (data instanceof String strData) {
            data = toStringFromStringHexToRawIfApplicable(column, strData);
        }
        return super.convertDateToEpochDays(column, fieldDefn, data);
    }

    @Override
    protected Object convertDateToEpochDaysAsDate(Column column, Field fieldDefn, Object data) {
        if (data instanceof String strData) {
            data = toStringFromStringHexToRawIfApplicable(column, strData);
        }
        return super.convertDateToEpochDaysAsDate(column, fieldDefn, data);
    }

    @Override
    protected Object convertTimestampToEpochMillisAsDate(Column column, Field fieldDefn, Object data) {
        if (data instanceof String strData) {
            data = resolveTimestampStringAsInstant(strData);
        }
        return super.convertTimestampToEpochMillisAsDate(column, fieldDefn, fromOracleTimeClasses(column, data));
    }

    @Override
    protected Object convertTimestampToEpochMicros(Column column, Field fieldDefn, Object data) {
        if (data instanceof Long) {
            return data;
        }
        if (data instanceof String strData) {
            data = resolveTimestampStringAsInstant(strData);
        }
        return super.convertTimestampToEpochMicros(column, fieldDefn, fromOracleTimeClasses(column, data));
    }

    @Override
    protected Object convertTimestampToEpochMillis(Column column, Field fieldDefn, Object data) {
        if (data instanceof String strData) {
            data = resolveTimestampStringAsInstant(strData);
        }
        return super.convertTimestampToEpochMillis(column, fieldDefn, fromOracleTimeClasses(column, data));
    }

    @Override
    protected Object convertTimestampToEpochNanos(Column column, Field fieldDefn, Object data) {
        if (data instanceof String strData) {
            data = resolveTimestampStringAsInstant(strData);
        }
        else if (data instanceof Long longData) {
            // todo: should we do this in OpenLogReplicator?
            data = Instant.ofEpochSecond(0, longData);
        }
        return super.convertTimestampToEpochNanos(column, fieldDefn, fromOracleTimeClasses(column, data));
    }

    private Instant resolveTimestampStringAsInstant(String data) {
        if (isHexToRawFunctionCall(data)) {
            return convertHexToRawFunctionToTimestamp(data).toInstant();
        }
        return TimestampUtils.convertTimestampNoZoneToInstant(data);
    }

    @Override
    protected Object convertTimestampToUtcIsoString(Column column, Field fieldDefn, Object data) {
        if (data instanceof String strData) {
            data = resolveTimestampStringAsInstant(strData);
        }
        else if (data instanceof Long longData) {
            data = Instant.ofEpochSecond(0, longData);
        }
        return super.convertTimestampToUtcIsoString(column, fieldDefn, fromOracleTimeClasses(column, data));
    }

    @Override
    protected Object convertTimestampToStructured(Column column, Field fieldDefn, Object data) {
        if (data instanceof String strData) {
            data = resolveTimestampStringAsInstant(strData);
        }
        else if (data instanceof Long longData) {
            data = Instant.ofEpochSecond(0, longData);
        }
        return super.convertTimestampToStructured(column, fieldDefn, fromOracleTimeClasses(column, data));
    }

    @Override
    protected Object convertTimestampWithZone(Column column, Field fieldDefn, Object data) {
        if (data instanceof String s) {
            if (isHexToRawFunctionCall(s)) {
                data = convertHexToRawFunctionToTimestamp(s);
            }
            else {
                final Matcher toTimestampTzMatcher = TO_TIMESTAMP_TZ.matcher(s);
                if (toTimestampTzMatcher.matches()) {
                    final var dateText = toTimestampTzMatcher.group(1);
                    data = ZonedDateTime.from(TIMESTAMP_TZ_FORMATTER.parse(dateText.trim()));
                }
            }
        }
        final var javaData = fromOracleTimeClasses(column, data);
        if (temporalPrecisionMode == TemporalPrecisionMode.STRUCTURED) {
            return super.convertTimestampWithZone(column, fieldDefn, javaData);
        }
        return convertValue(column, fieldDefn, javaData, fallbackTimestampWithTimeZone, (r) -> {
            try {
                // Fractional width for zoned timestamp is set in scale if schema obtained via snapshot
                // if obtained via streaming then it is in length
                final Integer fraction = column.scale().orElse(column.length());
                r.deliver(ZonedTimestamp.toIsoString(javaData, defaultOffset, adjuster, fraction));
            }
            catch (IllegalArgumentException e) {
            }
        });
    }

    protected Object convertTimestampWithLocalZone(Column column, Field fieldDefn, Object data) {
        return convertTimestampWithZone(column, fieldDefn, data);
    }

    protected Object convertIntervalYearMonth(Column column, Field fieldDefn, Object data) {
        if (temporalPrecisionMode == TemporalPrecisionMode.STRUCTURED) {
            return convertValue(column, fieldDefn, data, StructuredDuration.from(fieldDefn.schema(), 0, 0, 0, 0, 0, 0, 0), (r) -> {
                if (data instanceof Number numData) {
                    convertMicrosToStructuredDuration(numData.longValue(), fieldDefn.schema(), r);
                }
                else if (data instanceof INTERVALYM intervalData) {
                    convertOracleIntervalYearMonthToStructured(intervalData, fieldDefn.schema(), r);
                }
                else if (data instanceof String value) {
                    final INTERVALYM interval;
                    if (isHexToRawFunctionCall(value)) {
                        interval = new INTERVALYM(convertHexToRawFunctionToByteArray(value));
                    }
                    else {
                        interval = new INTERVALYM(value.substring(15, value.length() - 2));
                    }
                    convertOracleIntervalYearMonthToStructured(interval, fieldDefn.schema(), r);
                }
            });
        }
        return convertValue(column, fieldDefn, data, NumberConversions.LONG_FALSE, (r) -> {
            if (data instanceof Number numData) {
                // we expect to get back from the plugin a double value
                final long micros = numData.longValue();
                if (intervalHandlingMode == OracleConnectorConfig.IntervalHandlingMode.STRING) {
                    r.deliver(Interval.toIsoString(0, 0, 0, 0, 0, new BigDecimal(micros).divide(MICROSECONDS_PER_SECOND)));
                }
                else {
                    r.deliver(micros);
                }
            }
            else if (data instanceof INTERVALYM intervalData) {
                convertOracleIntervalYearMonth(intervalData, r);
            }
            else if (data instanceof String value) {
                final INTERVALYM interval;
                if (isHexToRawFunctionCall(value)) {
                    interval = new INTERVALYM(convertHexToRawFunctionToByteArray(value));
                }
                else {
                    // Example: TO_YMINTERVAL('-03-06')
                    interval = new INTERVALYM(value.substring(15, value.length() - 2));
                }
                convertOracleIntervalYearMonth(interval, r);
            }
        });
    }

    private void convertOracleIntervalYearMonth(Object data, ResultReceiver r) {
        final String interval = ((INTERVALYM) data).stringValue();
        int sign = 1;
        int start = 0;
        if (interval.charAt(0) == '-') {
            sign = -1;
            start = 1;
        }
        for (int i = 1; i < interval.length(); i++) {
            if (interval.charAt(i) == '-') {
                final int year = sign * Integer.parseInt(interval.substring(start, i));
                final int month = sign * Integer.parseInt(interval.substring(i + 1, interval.length()));
                if (intervalHandlingMode == OracleConnectorConfig.IntervalHandlingMode.STRING) {
                    r.deliver(Interval.toIsoString(year, month, 0, 0, 0, BigDecimal.ZERO));
                }
                else {
                    r.deliver(MicroDuration.durationMicros(year, month, 0, 0,
                            0, 0, MicroDuration.DAYS_PER_MONTH_AVG));
                }
            }
        }
    }

    protected Object convertIntervalDaySecond(Column column, Field fieldDefn, Object data) {
        if (temporalPrecisionMode == TemporalPrecisionMode.STRUCTURED) {
            final int precision = getTimePrecision(column);
            return convertValue(column, fieldDefn, data, StructuredDuration.from(fieldDefn.schema(), 0, 0, 0, 0, 0, 0, 0, precision), (r) -> {
                if (data instanceof Number numData) {
                    convertMicrosToStructuredDuration(numData.longValue(), fieldDefn.schema(), precision, r);
                }
                else if (data instanceof INTERVALDS intervalData) {
                    convertOracleIntervalDaySecondToStructured(intervalData, fieldDefn.schema(), precision, r);
                }
                else if (data instanceof String value) {
                    final INTERVALDS interval;
                    if (isHexToRawFunctionCall(value)) {
                        interval = new INTERVALDS(convertHexToRawFunctionToByteArray(value));
                    }
                    else {
                        interval = new INTERVALDS(value.substring(15, value.length() - 2));
                    }
                    convertOracleIntervalDaySecondToStructured(interval, fieldDefn.schema(), precision, r);
                }
            });
        }
        return convertValue(column, fieldDefn, data, NumberConversions.LONG_FALSE, (r) -> {
            if (data instanceof Number numData) {
                // we expect to get back from the plugin a double value
                final long micros = numData.longValue();
                if (intervalHandlingMode == OracleConnectorConfig.IntervalHandlingMode.STRING) {
                    r.deliver(Interval.toIsoString(0, 0, 0, 0, 0, new BigDecimal(micros).divide(MICROSECONDS_PER_SECOND)));
                }
                else {
                    r.deliver(micros);
                }
            }
            else if (data instanceof INTERVALDS intervalData) {
                convertOracleIntervalDaySecond(intervalData, r);
            }
            else if (data instanceof String value) {
                final INTERVALDS interval;
                if (isHexToRawFunctionCall(value)) {
                    interval = new INTERVALDS(convertHexToRawFunctionToByteArray(value));
                }
                else {
                    // Example: TO_DSINTERVAL('-001 02:03:04.56')
                    interval = new INTERVALDS(value.substring(15, value.length() - 2));
                }
                convertOracleIntervalDaySecond(interval, r);
            }
        });
    }

    private void convertOracleIntervalDaySecond(Object data, ResultReceiver r) {
        final String interval = ((INTERVALDS) data).stringValue();
        final Matcher m = INTERVAL_DAY_SECOND_PATTERN.matcher(interval);
        if (m.matches()) {
            final int sign = "-".equals(m.group(1)) ? -1 : 1;
            if (intervalHandlingMode == OracleConnectorConfig.IntervalHandlingMode.STRING) {
                double seconds = sign * ((double) (Integer.parseInt(m.group(5)))
                        + (double) Integer.parseInt(Strings.pad(m.group(6), 6, '0')) / 1_000_000D);
                r.deliver(Interval.toIsoString(
                        0,
                        0,
                        sign * Integer.valueOf(m.group(2)),
                        sign * Integer.valueOf(m.group(3)),
                        sign * Integer.valueOf(m.group(4)),
                        BigDecimal.valueOf(seconds)));
            }
            else {
                r.deliver(MicroDuration.durationMicros(
                        0,
                        0,
                        sign * Integer.valueOf(m.group(2)),
                        sign * Integer.valueOf(m.group(3)),
                        sign * Integer.valueOf(m.group(4)),
                        sign * Integer.valueOf(m.group(5)),
                        sign * Integer.valueOf(Strings.pad(m.group(6), 6, '0')),
                        MicroDuration.DAYS_PER_MONTH_AVG));
            }
        }
    }

    private void convertMicrosToStructuredDuration(long micros, Schema schema, ResultReceiver r) {
        convertMicrosToStructuredDuration(micros, schema, -1, r);
    }

    private void convertMicrosToStructuredDuration(long micros, Schema schema, int precision, ResultReceiver r) {
        final long seconds = micros / 1_000_000;
        final int nanos = (int) (micros % 1_000_000) * 1_000;
        r.deliver(StructuredDuration.from(schema, 0, 0, 0, 0, 0, seconds, nanos, precision));
    }

    private void convertOracleIntervalYearMonthToStructured(Object data, Schema schema, ResultReceiver r) {
        final String interval = ((INTERVALYM) data).stringValue();
        int sign = 1;
        int start = 0;
        if (interval.charAt(0) == '-') {
            sign = -1;
            start = 1;
        }
        for (int i = 1; i < interval.length(); i++) {
            if (interval.charAt(i) == '-') {
                final int year = sign * Integer.parseInt(interval.substring(start, i));
                final int month = sign * Integer.parseInt(interval.substring(i + 1, interval.length()));
                r.deliver(StructuredDuration.from(schema, year, month, 0, 0, 0, 0, 0));
            }
        }
    }

    private void convertOracleIntervalDaySecondToStructured(Object data, Schema schema, int precision, ResultReceiver r) {
        final String interval = ((INTERVALDS) data).stringValue();
        final Matcher m = INTERVAL_DAY_SECOND_PATTERN.matcher(interval);
        if (m.matches()) {
            final int sign = "-".equals(m.group(1)) ? -1 : 1;
            r.deliver(StructuredDuration.from(
                    schema,
                    0,
                    0,
                    sign * Integer.valueOf(m.group(2)),
                    sign * Integer.valueOf(m.group(3)),
                    sign * Integer.valueOf(m.group(4)),
                    sign * Long.parseLong(m.group(5)),
                    sign * Integer.parseInt(Strings.pad(m.group(6), 9, '0')),
                    precision));
        }
    }

    /**
     * Get the {@code HEXTORAW} function argument, removing the function call prefix/suffix if present.
     *
     * @param hexToRawValue the hex-to-raw string, optionally wrapped by the function call, never {@code null}
     * @return the hex-to-raw argument, never {@code null}.
     */
    public static String getHexToRawHexString(String hexToRawValue) {
        if (isHexToRawFunctionCall(hexToRawValue)) {
            return hexToRawValue.substring(10, hexToRawValue.length() - 2);
        }
        return hexToRawValue;
    }

    /**
     * Returns whether the provided value is a {@code HEXTORAW} function, format {@code HEXTORAW('<hex>')}.
     *
     * @param value the value to inspect and validate, may be {@code null}
     * @return true if the value is a {@code HEXTORAW} function call; false otherwise.
     */
    public static boolean isHexToRawFunctionCall(String value) {
        return value != null && value.startsWith(HEXTORAW_FUNCTION_START) && value.endsWith(HEXTORAW_FUNCTION_END);
    }

    /**
     * Takes the {@code HEXTORAW} function call and argument and returns a byte array.
     *
     * @param value the {@code HEXTORAW} function with argument, should not be {@code null}
     * @return a byte array of the hex-to-raw function argument
     */
    private byte[] convertHexToRawFunctionToByteArray(String value) {
        final String rawValue = getHexToRawHexString(value);
        int len = rawValue.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(rawValue.charAt(i), 16) << 4)
                    + Character.digit(rawValue.charAt(i + 1), 16));
        }
        return data;
    }

    /**
     * Converts the supplied {@code HEXTORAW} function and argument to a string literal. This method supports
     * all character data types, including the nationalized character set data types.
     *
     * @param column the column
     * @param function the {@code HEXTORAW} function and argument
     * @return the converted string value
     */
    private String convertHexToRawFunctionToString(Column column, String function) {
        try {
            switch (column.jdbcType()) {
                case OracleTypes.NVARCHAR:
                case OracleTypes.NCHAR:
                    return new CHAR(convertHexToRawFunctionToByteArray(function), nationalCharacterSet).toString();
                default:
                    return new CHAR(convertHexToRawFunctionToByteArray(function), databaseCharacterSet).toString();
            }
        }
        catch (Exception e) {
            throw new DebeziumException("Couldn't convert value for column " + column.name(), e);
        }
    }

    /**
     * Converts the supplied {@code HEXTORAW} function and argument to a specific Oracle numeric data type.
     *
     * @param column the column
     * @param data the {@code HEXTORAW} function and argument
     * @return the converted numeric data type
     */
    private Object convertHexToRawFunctionToNumber(Column column, String data) {
        try {
            switch (column.jdbcType()) {
                case OracleTypes.BINARY_FLOAT:
                    return new BINARY_FLOAT(convertHexToRawFunctionToByteArray(data)).stringValue();
                case OracleTypes.BINARY_DOUBLE:
                    return new BINARY_DOUBLE(convertHexToRawFunctionToByteArray(data)).stringValue();
                default:
                    return new NUMBER(convertHexToRawFunctionToByteArray(data)).stringValue();
            }
        }
        catch (Exception e) {
            throw new DebeziumException("Couldn't convert value for column " + column.name(), e);
        }
    }

    private String toStringFromNumericHexToRawIfApplicable(Column column, String data) {
        if (isHexToRawFunctionCall(data)) {
            return String.valueOf(convertHexToRawFunctionToNumber(column, data));
        }
        return data;
    }

    private Object toNumberFromNumericHexToRawIfApplicable(Column column, String data) {
        if (isHexToRawFunctionCall(data)) {
            return convertHexToRawFunctionToNumber(column, data);
        }
        return data;
    }

    private Object toStringFromStringHexToRawIfApplicable(Column column, String data) {
        if (isHexToRawFunctionCall(data)) {
            return convertHexToRawFunctionToString(column, data);
        }
        return data;
    }

    /**
     * Convert the {@code HEXTORAW} timestamp function to a {@link ZonedDateTime}.
     *
     * @param value the hex-to-raw function and argument
     * @return a zoned date time
     * @throws DebeziumException if the conversion failed
     */
    private ZonedDateTime convertHexToRawFunctionToTimestamp(String value) {
        try {
            // Convert the HEXTORAW function into a byte array
            final byte[] data = convertHexToRawFunctionToByteArray(value);

            // Calculate the raw value and its length
            if (data.length == 7 || data.length == 11) {
                // 7 bytes (14 character) values represent dates, with optional hours/minute/second values
                // 11 bytes (22 characters) values represent timestamps, no explicit timezone data
                return new TIMESTAMP(data).toLocalDateTime().atOffset(ZoneOffset.UTC).toZonedDateTime();
            }
            else if (data.length == 13) {
                // 13 bytes (26 characters) values represent timestamps with timezone information
                // data[11] - offset hours, offset by 20, i.e. subtract 20
                // data[12] - offset minutes, offset by 60, i.e. subtract 60
                final ZoneOffset offset = ZoneOffset.ofHoursMinutes(data[11] - 20, data[12] - 60);
                return new TIMESTAMPTZ(data).toLocalDateTime().atOffset(offset).toZonedDateTime();
            }
            else {
                throw new DebeziumException("The HEXTORAW value '" + value + "' cannot be converted.");
            }
        }
        catch (SQLException e) {
            throw new DebeziumException("Failed to convert HEXTORAW value '" + value + "' to timestamp.");
        }
    }

}
