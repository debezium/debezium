/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import static io.debezium.util.NumberConversions.BYTE_FALSE;

import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.SchemaBuilder;

import io.debezium.DebeziumException;
import io.debezium.config.CommonConnectorConfig.BinaryHandlingMode;
import io.debezium.connector.oracle.logminer.UnistrHelper;
import io.debezium.data.SpecialValueDecimal;
import io.debezium.data.VariableScaleDecimal;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.jdbc.ResultReceiver;
import io.debezium.relational.Column;
import io.debezium.relational.ValueConverter;
import io.debezium.time.Date;
import io.debezium.time.Interval;
import io.debezium.time.MicroDuration;
import io.debezium.time.ZonedTimestamp;
import io.debezium.util.NumberConversions;
import io.debezium.util.Strings;

import oracle.jdbc.OracleTypes;
import oracle.sql.BINARY_DOUBLE;
import oracle.sql.BINARY_FLOAT;
import oracle.sql.CHAR;
import oracle.sql.DATE;
import oracle.sql.INTERVALDS;
import oracle.sql.INTERVALYM;
import oracle.sql.NUMBER;
import oracle.sql.RAW;
import oracle.sql.TIMESTAMP;
import oracle.sql.TIMESTAMPLTZ;
import oracle.sql.TIMESTAMPTZ;

public class OracleValueConverters extends JdbcValueConverters {

    /**
     * Marker value indicating an unavilable column value.
     */
    public static final Object UNAVAILABLE_VALUE = new Object();
    public static final String EMPTY_BLOB_FUNCTION = "EMPTY_BLOB()";
    public static final String EMPTY_CLOB_FUNCTION = "EMPTY_CLOB()";
    public static final String HEXTORAW_FUNCTION_START = "HEXTORAW('";
    public static final String HEXTORAW_FUNCTION_END = "')";

    private static final Pattern INTERVAL_DAY_SECOND_PATTERN = Pattern.compile("([+\\-])?(\\d+) (\\d+):(\\d+):(\\d+).(\\d+)");

    private static final ZoneId GMT_ZONE_ID = ZoneId.of("GMT");

    private static final DateTimeFormatter TIMESTAMP_FORMATTER = new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .appendPattern("yyyy-MM-dd HH:mm:ss")
            .optionalStart()
            .appendPattern(".")
            .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, false)
            .optionalEnd()
            .toFormatter();

    private static final DateTimeFormatter TIMESTAMP_AM_PM_SHORT_FORMATTER = new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .appendPattern("dd-MMM-yy hh.mm.ss")
            .optionalStart()
            .appendPattern(".")
            .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, false)
            .optionalEnd()
            .appendPattern(" a")
            .toFormatter(Locale.ENGLISH);

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

    private static final Pattern TO_TIMESTAMP = Pattern.compile("TO_TIMESTAMP\\('(.*)'\\)", Pattern.CASE_INSENSITIVE);
    private static final Pattern TO_TIMESTAMP_TZ = Pattern.compile("TO_TIMESTAMP_TZ\\('(.*)'\\)", Pattern.CASE_INSENSITIVE);
    private static final Pattern TO_DATE = Pattern.compile("TO_DATE\\('(.*)',[ ]*'(.*)'\\)", Pattern.CASE_INSENSITIVE);
    private static final BigDecimal MICROSECONDS_PER_SECOND = new BigDecimal(1_000_000);

    private final OracleConnection connection;
    private final boolean lobEnabled;
    private final OracleConnectorConfig.IntervalHandlingMode intervalHandlingMode;
    private final byte[] unavailableValuePlaceholderBinary;
    private final String unavailableValuePlaceholderString;

    public OracleValueConverters(OracleConnectorConfig config, OracleConnection connection) {
        super(config.getDecimalMode(), config.getTemporalPrecisionMode(), ZoneOffset.UTC, null, null, config.binaryHandlingMode());
        this.connection = connection;
        this.lobEnabled = config.isLobEnabled();
        this.intervalHandlingMode = config.getIntervalHandlingMode();
        this.unavailableValuePlaceholderBinary = config.getUnavailableValuePlaceholder();
        this.unavailableValuePlaceholderString = new String(config.getUnavailableValuePlaceholder());
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

        switch (column.jdbcType()) {
            // Oracle's float is not float as in Java but a NUMERIC without scale
            case Types.FLOAT:
                return variableScaleSchema(column);
            case Types.NUMERIC:
                return getNumericSchema(column);
            case OracleTypes.BINARY_FLOAT:
                return SchemaBuilder.float32();
            case OracleTypes.BINARY_DOUBLE:
                return SchemaBuilder.float64();
            case OracleTypes.TIMESTAMPTZ:
            case OracleTypes.TIMESTAMPLTZ:
                return ZonedTimestamp.builder();
            case OracleTypes.INTERVALYM:
            case OracleTypes.INTERVALDS:
                return intervalHandlingMode == OracleConnectorConfig.IntervalHandlingMode.STRING ? Interval.builder() : MicroDuration.builder();
            case Types.STRUCT:
                return SchemaBuilder.string();
            case OracleTypes.ROWID:
                return SchemaBuilder.string();
            default: {
                SchemaBuilder builder = super.schemaBuilder(column);
                logger.debug("JdbcValueConverters returned '{}' for column '{}'", builder != null ? builder.getClass().getName() : null, column.name());
                return builder;
            }
        }
    }

    private SchemaBuilder getNumericSchema(Column column) {
        if (column.scale().isPresent()) {
            // return sufficiently sized int schema for non-floating point types
            Integer scale = column.scale().get();

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
        else {
            return variableScaleSchema(column);
        }
    }

    private SchemaBuilder variableScaleSchema(Column column) {
        if (decimalMode == DecimalMode.PRECISE) {
            return VariableScaleDecimal.builder();
        }
        return SpecialValueDecimal.builder(decimalMode, column.length(), column.scale().orElse(-1));
    }

    @Override
    public ValueConverter converter(Column column, Field fieldDefn) {
        switch (column.jdbcType()) {
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.NCHAR:
            case Types.NVARCHAR:
            case Types.STRUCT:
            case Types.CLOB:
            case OracleTypes.ROWID:
                return data -> convertString(column, fieldDefn, data);
            case Types.BLOB:
                return data -> convertBinary(column, fieldDefn, data, binaryMode);
            case OracleTypes.BINARY_FLOAT:
                return data -> convertFloat(column, fieldDefn, data);
            case OracleTypes.BINARY_DOUBLE:
                return data -> convertDouble(column, fieldDefn, data);
            case Types.NUMERIC:
                return getNumericConverter(column, fieldDefn);
            case Types.FLOAT:
                return data -> convertVariableScale(column, fieldDefn, data);
            case OracleTypes.TIMESTAMPTZ:
            case OracleTypes.TIMESTAMPLTZ:
                return (data) -> convertTimestampWithZone(column, fieldDefn, data);
            case OracleTypes.INTERVALYM:
                return (data) -> convertIntervalYearMonth(column, fieldDefn, data);
            case OracleTypes.INTERVALDS:
                return (data) -> convertIntervalDaySecond(column, fieldDefn, data);
            case OracleTypes.RAW:
                // Raw data types are not supported
                return null;
        }

        return super.converter(column, fieldDefn);
    }

    private ValueConverter getNumericConverter(Column column, Field fieldDefn) {
        if (column.scale().isPresent()) {
            Integer scale = column.scale().get();

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
        else {
            return data -> convertVariableScale(column, fieldDefn, data);
        }
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
        if (data instanceof CHAR) {
            return ((CHAR) data).stringValue();
        }
        if (data instanceof Clob) {
            if (!lobEnabled) {
                if (column.isOptional()) {
                    return null;
                }
                return "";
            }
            try {
                Clob clob = (Clob) data;
                // Note that java.sql.Clob specifies that the first character starts at 1
                // and that length must be greater-than or equal to 0. So for an empty
                // clob field, a call to getSubString(1, 0) is perfectly valid.
                return clob.getSubString(1, (int) clob.length());
            }
            catch (SQLException e) {
                throw new DebeziumException("Couldn't convert value for column " + column.name(), e);
            }
        }
        if (data instanceof String) {
            String s = (String) data;
            if (EMPTY_CLOB_FUNCTION.equals(s)) {
                return column.isOptional() ? null : "";
            }
            else if (UnistrHelper.isUnistrFunction(s)) {
                return UnistrHelper.convert(s);
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
            if (data instanceof String) {
                String str = (String) data;
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
            else if (data instanceof Blob) {
                if (!lobEnabled) {
                    if (column.isOptional()) {
                        return null;
                    }
                    else {
                        data = NumberConversions.BYTE_ZERO;
                    }
                }
                else {
                    Blob blob = (Blob) data;
                    data = blob.getBytes(1, Long.valueOf(blob.length()).intValue());
                }
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

    @Override
    protected Object convertInteger(Column column, Field fieldDefn, Object data) {
        if (data instanceof NUMBER) {
            try {
                data = ((NUMBER) data).intValue();
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
        else if (data instanceof NUMBER) {
            return ((NUMBER) data).floatValue();
        }
        else if (data instanceof BINARY_FLOAT) {
            try {
                return ((BINARY_FLOAT) data).floatValue();
            }
            catch (SQLException e) {
                throw new DebeziumException("Couldn't convert value for column " + column.name(), e);
            }
        }
        else if (data instanceof String) {
            return Float.parseFloat((String) data);
        }

        return super.convertFloat(column, fieldDefn, data);
    }

    @Override
    protected Object convertDouble(Column column, Field fieldDefn, Object data) {
        if (data instanceof BINARY_DOUBLE) {
            try {
                return ((BINARY_DOUBLE) data).doubleValue();
            }
            catch (SQLException e) {
                throw new DebeziumException("Couldn't convert value for column " + column.name(), e);
            }
        }
        else if (data instanceof String) {
            return Double.parseDouble((String) data);
        }

        return super.convertDouble(column, fieldDefn, data);
    }

    @Override
    protected Object convertDecimal(Column column, Field fieldDefn, Object data) {
        if (data instanceof NUMBER) {
            try {
                data = ((NUMBER) data).bigDecimalValue();
            }
            catch (SQLException e) {
                throw new DebeziumException("Couldn't convert value for column " + column.name(), e);
            }
        }

        if (data instanceof String) {
            // In the case when the value is of String, convert it to a BigDecimal so that we can then
            // aptly apply the scale adjustment below.
            data = toBigDecimal(column, fieldDefn, data);
        }

        // adjust scale to column's scale if the column's scale is larger than the one from
        // the value (e.g. 4.4444 -> 4.444400)
        if (data instanceof BigDecimal) {
            data = withScaleAdjustedIfNeeded(column, (BigDecimal) data);
        }

        return super.convertDecimal(column, fieldDefn, data);
    }

    @Override
    protected Object convertNumeric(Column column, Field fieldDefn, Object data) {
        return convertDecimal(column, fieldDefn, data);
    }

    protected Object convertNumericAsTinyInt(Column column, Field fieldDefn, Object data) {
        if (data instanceof NUMBER) {
            try {
                data = ((NUMBER) data).byteValue();
            }
            catch (SQLException e) {
                throw new DebeziumException("Couldn't convert value for column " + column.name(), e);
            }
        }

        return convertTinyInt(column, fieldDefn, data);
    }

    protected Object convertNumericAsSmallInt(Column column, Field fieldDefn, Object data) {
        if (data instanceof NUMBER) {
            try {
                data = ((NUMBER) data).shortValue();
            }
            catch (SQLException e) {
                throw new DebeziumException("Couldn't convert value for column " + column.name(), e);
            }
        }

        return super.convertSmallInt(column, fieldDefn, data);
    }

    protected Object convertNumericAsInteger(Column column, Field fieldDefn, Object data) {
        if (data instanceof NUMBER) {
            try {
                data = ((NUMBER) data).intValue();
            }
            catch (SQLException e) {
                throw new DebeziumException("Couldn't convert value for column " + column.name(), e);
            }
        }

        return super.convertInteger(column, fieldDefn, data);
    }

    protected Object convertNumericAsBigInteger(Column column, Field fieldDefn, Object data) {
        if (data instanceof NUMBER) {
            try {
                data = ((NUMBER) data).longValue();
            }
            catch (SQLException e) {
                throw new DebeziumException("Couldn't convert value for column " + column.name(), e);
            }
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
        if (data instanceof BigDecimal) {
            return ((BigDecimal) data).byteValue() == 0 ? Boolean.FALSE : Boolean.TRUE;
        }
        if (data instanceof String) {
            return Byte.parseByte((String) data) == 0 ? Boolean.FALSE : Boolean.TRUE;
        }
        if (data instanceof NUMBER) {
            try {
                return ((NUMBER) data).intValue() == 0 ? Boolean.FALSE : Boolean.TRUE;
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
            else if (data instanceof Number) {
                Number value = (Number) data;
                r.deliver(value.byteValue());
            }
            else if (data instanceof Boolean) {
                r.deliver(NumberConversions.getByte((boolean) data));
            }
            else if (data instanceof String) {
                r.deliver(Byte.parseByte((String) data));
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
            if (data instanceof SpecialValueDecimal) {
                return VariableScaleDecimal.fromLogical(fieldDefn.schema(), (SpecialValueDecimal) data);
            }
            else if (data instanceof BigDecimal) {
                return VariableScaleDecimal.fromLogical(fieldDefn.schema(), new SpecialValueDecimal((BigDecimal) data));
            }
        }
        else {
            return data;
        }
        return handleUnknownData(column, fieldDefn, data);
    }

    protected Object fromOracleTimeClasses(Column column, Object data) {
        try {
            if (data instanceof TIMESTAMP) {
                data = ((TIMESTAMP) data).timestampValue();
            }
            else if (data instanceof DATE) {
                data = ((DATE) data).timestampValue();
            }
            else if (data instanceof TIMESTAMPTZ) {
                final TIMESTAMPTZ ts = (TIMESTAMPTZ) data;
                data = ts.toZonedDateTime();
            }
            else if (data instanceof TIMESTAMPLTZ) {
                final TIMESTAMPLTZ ts = (TIMESTAMPLTZ) data;
                data = ZonedDateTime.ofInstant(ts.timestampValue(connection.connection()).toInstant(), ZoneId.systemDefault()).withZoneSameInstant(ZoneOffset.UTC);
            }
        }
        catch (SQLException e) {
            throw new DebeziumException("Couldn't convert value for column " + column.name(), e);
        }
        return data;
    }

    @Override
    protected Object convertTimestampToEpochMillisAsDate(Column column, Field fieldDefn, Object data) {
        if (data instanceof String) {
            data = resolveTimestampStringAsInstant((String) data);
        }
        return super.convertTimestampToEpochMillisAsDate(column, fieldDefn, fromOracleTimeClasses(column, data));
    }

    @Override
    protected Object convertTimestampToEpochMicros(Column column, Field fieldDefn, Object data) {
        if (data instanceof Long) {
            return data;
        }
        if (data instanceof String) {
            data = resolveTimestampStringAsInstant((String) data);
        }
        return super.convertTimestampToEpochMicros(column, fieldDefn, fromOracleTimeClasses(column, data));
    }

    @Override
    protected Object convertTimestampToEpochMillis(Column column, Field fieldDefn, Object data) {
        if (data instanceof String) {
            data = resolveTimestampStringAsInstant((String) data);
        }
        return super.convertTimestampToEpochMillis(column, fieldDefn, fromOracleTimeClasses(column, data));
    }

    @Override
    protected Object convertTimestampToEpochNanos(Column column, Field fieldDefn, Object data) {
        if (data instanceof String) {
            data = resolveTimestampStringAsInstant((String) data);
        }
        return super.convertTimestampToEpochNanos(column, fieldDefn, fromOracleTimeClasses(column, data));
    }

    private Instant resolveTimestampStringAsInstant(String data) {
        LocalDateTime dateTime;

        final Matcher toTimestampMatcher = TO_TIMESTAMP.matcher(data);
        if (toTimestampMatcher.matches()) {
            String dateText = toTimestampMatcher.group(1);
            if (dateText.indexOf(" AM") > 0 || dateText.indexOf(" PM") > 0) {
                dateTime = LocalDateTime.from(TIMESTAMP_AM_PM_SHORT_FORMATTER.parse(dateText.trim()));
            }
            else {
                dateTime = LocalDateTime.from(TIMESTAMP_FORMATTER.parse(dateText.trim()));
            }
            return dateTime.atZone(GMT_ZONE_ID).toInstant();
        }

        final Matcher toDateMatcher = TO_DATE.matcher(data);
        if (toDateMatcher.matches()) {
            dateTime = LocalDateTime.from(TIMESTAMP_FORMATTER.parse(toDateMatcher.group(1)));
            return dateTime.atZone(GMT_ZONE_ID).toInstant();
        }

        // Unable to resolve
        return null;
    }

    @Override
    protected Object convertTimestampWithZone(Column column, Field fieldDefn, Object data) {
        if (data instanceof String) {
            final Matcher toTimestampTzMatcher = TO_TIMESTAMP_TZ.matcher((String) data);
            if (toTimestampTzMatcher.matches()) {
                String dateText = toTimestampTzMatcher.group(1);
                data = ZonedDateTime.from(TIMESTAMP_TZ_FORMATTER.parse(dateText.trim()));
            }
        }
        final Object javaData = fromOracleTimeClasses(column, data);
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

    protected Object convertIntervalYearMonth(Column column, Field fieldDefn, Object data) {
        return convertValue(column, fieldDefn, data, NumberConversions.LONG_FALSE, (r) -> {
            if (data instanceof Number) {
                // we expect to get back from the plugin a double value
                final long micros = ((Number) data).longValue();
                if (intervalHandlingMode == OracleConnectorConfig.IntervalHandlingMode.STRING) {
                    r.deliver(Interval.toIsoString(0, 0, 0, 0, 0, new BigDecimal(micros).divide(MICROSECONDS_PER_SECOND)));
                }
                else {
                    r.deliver(micros);
                }
            }
            else if (data instanceof INTERVALYM) {
                convertOracleIntervalYearMonth(data, r);
            }
            else if (data instanceof String) {
                String value = (String) data;
                // Example: TO_YMINTERVAL('-03-06')
                INTERVALYM interval = new INTERVALYM(value.substring(15, value.length() - 2));
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
        return convertValue(column, fieldDefn, data, NumberConversions.LONG_FALSE, (r) -> {
            if (data instanceof Number) {
                // we expect to get back from the plugin a double value
                final long micros = ((Number) data).longValue();
                if (intervalHandlingMode == OracleConnectorConfig.IntervalHandlingMode.STRING) {
                    r.deliver(Interval.toIsoString(0, 0, 0, 0, 0, new BigDecimal(micros).divide(MICROSECONDS_PER_SECOND)));
                }
                else {
                    r.deliver(micros);
                }
            }
            else if (data instanceof INTERVALDS) {
                convertOracleIntervalDaySecond(data, r);
            }
            else if (data instanceof String) {
                String value = (String) data;
                // Exmaple: TO_DSINTERVAL('-001 02:03:04.56')
                INTERVALDS interval = new INTERVALDS(value.substring(15, value.length() - 2));
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

    /**
     * Get the {@code HEXTORAW} function argument, removing the function call prefix/suffix if present.
     *
     * @param hexToRawValue the hex-to-raw string, optionally wrapped by the function call, never {@code null}
     * @return the hex-to-raw argument, never {@code null}.
     */
    private String getHexToRawHexString(String hexToRawValue) {
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
    private boolean isHexToRawFunctionCall(String value) {
        return value != null && value.startsWith(HEXTORAW_FUNCTION_START) && value.endsWith(HEXTORAW_FUNCTION_END);
    }
}
