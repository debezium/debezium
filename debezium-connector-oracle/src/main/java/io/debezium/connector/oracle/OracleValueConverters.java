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
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import io.debezium.config.CommonConnectorConfig.BinaryHandlingMode;
import io.debezium.data.SpecialValueDecimal;
import io.debezium.data.VariableScaleDecimal;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.jdbc.ResultReceiver;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.Column;
import io.debezium.relational.ValueConverter;
import io.debezium.time.Conversions;
import io.debezium.time.Date;
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
import oracle.sql.TIMESTAMP;
import oracle.sql.TIMESTAMPLTZ;
import oracle.sql.TIMESTAMPTZ;

public class OracleValueConverters extends JdbcValueConverters {

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
            .toFormatter();

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

    private final OracleConnection connection;

    public OracleValueConverters(OracleConnectorConfig config, OracleConnection connection) {
        super(config.getDecimalMode(), TemporalPrecisionMode.ADAPTIVE, ZoneOffset.UTC, null, null, null);
        this.connection = connection;
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
                return MicroDuration.builder();
            case Types.STRUCT:
                return SchemaBuilder.string();
            default: {
                SchemaBuilder builder = super.schemaBuilder(column);
                logger.debug("JdbcValueConverters returned '{}' for column '{}'", builder.getClass().getName(), column.name());
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
    protected Object convertString(Column column, Field fieldDefn, Object data) {
        if (data instanceof CHAR) {
            return ((CHAR) data).stringValue();
        }
        if (data instanceof Clob) {
            return ((Clob) data).toString();
        }
        if (data instanceof String) {
            String s = (String) data;
            if (s.startsWith("UNISTR('") && s.endsWith("')")) {
                return convertOracleUnistr(column, fieldDefn, s.substring(8, s.length() - 2));
            }
        }

        return super.convertString(column, fieldDefn, data);
    }

    private String convertOracleUnistr(Column column, Field fieldDefn, String data) {
        if (data != null && data.length() > 0) {
            StringBuilder result = new StringBuilder();
            for (int i = 0; i < data.length(); ++i) {
                char c = data.charAt(i);
                if (c == '\\') {
                    // handle special legacy parser use case where '\' is actually '\\', necessitated by JSqlParser
                    // can safely be removed when SimpleDmlParser is retired
                    if (data.charAt(i + 1) == '\\') {
                        // advance by 1
                        i += 1;
                    }
                    if (data.length() >= (i + 4)) {
                        // Read next 4 character hex and convert to character.
                        result.append(Character.toChars(Integer.parseInt(data.substring(i + 1, i + 5), 16)));
                        i += 4;
                        continue;
                    }
                }
                result.append(c);
            }
            return result.toString();
        }
        return data;
    }

    @Override
    protected Object convertBinary(Column column, Field fieldDefn, Object data, BinaryHandlingMode mode) {
        if (data instanceof Blob) {
            try {
                Blob blob = (Blob) data;
                return blob.getBytes(0, Long.valueOf(blob.length()).intValue());
            }
            catch (SQLException e) {
                throw new RuntimeException("Couldn't convert value for column " + column.name(), e);
            }
        }
        return super.convertBinary(column, fieldDefn, data, mode);
    }

    @Override
    protected Object convertInteger(Column column, Field fieldDefn, Object data) {
        if (data instanceof NUMBER) {
            try {
                data = ((NUMBER) data).intValue();
            }
            catch (SQLException e) {
                throw new RuntimeException("Couldn't convert value for column " + column.name(), e);
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
                throw new RuntimeException("Couldn't convert value for column " + column.name(), e);
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
                throw new RuntimeException("Couldn't convert value for column " + column.name(), e);
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
                throw new RuntimeException("Couldn't convert value for column " + column.name(), e);
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

        // When SimpleDmlParser is removed, the following block can be removed.
        // This is necessitated by the fact SimpleDmlParser invokes the converters internally and
        // won't be needed when that parser is no longer part of the source.
        if (data instanceof Struct) {
            SpecialValueDecimal value = VariableScaleDecimal.toLogical((Struct) data);
            return value.getDecimalValue().orElse(null);
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
                throw new RuntimeException("Couldn't convert value for column " + column.name(), e);
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
                throw new RuntimeException("Couldn't convert value for column " + column.name(), e);
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
                throw new RuntimeException("Couldn't convert value for column " + column.name(), e);
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
                throw new RuntimeException("Couldn't convert value for column " + column.name(), e);
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
                throw new RuntimeException("Couldn't convert value for column " + column.name(), e);
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
                data = ZonedDateTime.ofInstant(ts.timestampValue(connection.connection()).toInstant(), ts.getTimeZone().toZoneId());
            }
            else if (data instanceof TIMESTAMPLTZ) {
                // JDBC driver throws an exception
                // final TIMESTAMPLTZ ts = (TIMESTAMPLTZ)data;
                // data = ts.offsetDateTimeValue(connection.connection());
                return null;
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Couldn't convert value for column " + column.name(), e);
        }
        return data;
    }

    @Override
    protected Object convertTimestampToEpochMicros(Column column, Field fieldDefn, Object data) {
        if (data instanceof Long) {
            return data;
        }
        if (data instanceof String) {
            return resolveTimestampString(column, fieldDefn, (String) data);
        }
        return super.convertTimestampToEpochMicros(column, fieldDefn, fromOracleTimeClasses(column, data));
    }

    @Override
    protected Object convertTimestampToEpochMillis(Column column, Field fieldDefn, Object data) {
        if (data instanceof String) {
            return resolveTimestampString(column, fieldDefn, (String) data);
        }
        return super.convertTimestampToEpochMillis(column, fieldDefn, fromOracleTimeClasses(column, data));
    }

    @Override
    protected Object convertTimestampToEpochNanos(Column column, Field fieldDefn, Object data) {
        if (data instanceof String) {
            return resolveTimestampString(column, fieldDefn, (String) data);
        }
        return super.convertTimestampToEpochNanos(column, fieldDefn, fromOracleTimeClasses(column, data));
    }

    private Object resolveTimestampString(Column column, Field fieldDefn, String data) {
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
            return getDateTimeWithPrecision(column, dateTime);
        }

        final Matcher toDateMatcher = TO_DATE.matcher(data);
        if (toDateMatcher.matches()) {
            dateTime = LocalDateTime.from(TIMESTAMP_FORMATTER.parse(toDateMatcher.group(1)));
            return getDateTimeWithPrecision(column, dateTime);
        }

        // Unable to resolve
        return null;
    }

    private Object getDateTimeWithPrecision(Column column, LocalDateTime dateTime) {
        if (adaptiveTimePrecisionMode || adaptiveTimeMicrosecondsPrecisionMode) {
            if (getTimePrecision(column) <= 3) {
                return dateTime.atZone(GMT_ZONE_ID).toInstant().toEpochMilli();
            }
            if (getTimePrecision(column) <= 6) {
                return Conversions.toEpochMicros(dateTime.atZone(GMT_ZONE_ID).toInstant());
            }
            return dateTime.atZone(GMT_ZONE_ID).toInstant().toEpochMilli() * 1_000_000;
        }
        return dateTime.atZone(GMT_ZONE_ID).toInstant().toEpochMilli();
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
        return super.convertTimestampWithZone(column, fieldDefn, fromOracleTimeClasses(column, data));
    }

    protected Object convertIntervalYearMonth(Column column, Field fieldDefn, Object data) {
        return convertValue(column, fieldDefn, data, NumberConversions.LONG_FALSE, (r) -> {
            if (data instanceof Number) {
                // we expect to get back from the plugin a double value
                r.deliver(((Number) data).longValue());
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
                r.deliver(MicroDuration.durationMicros(year, month, 0, 0,
                        0, 0, MicroDuration.DAYS_PER_MONTH_AVG));
            }
        }
    }

    protected Object convertIntervalDaySecond(Column column, Field fieldDefn, Object data) {
        return convertValue(column, fieldDefn, data, NumberConversions.LONG_FALSE, (r) -> {
            if (data instanceof Number) {
                // we expect to get back from the plugin a double value
                r.deliver(((Number) data).longValue());
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
