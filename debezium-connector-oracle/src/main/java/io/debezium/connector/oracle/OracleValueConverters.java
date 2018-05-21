/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.sql.Types;
import java.time.ZonedDateTime;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.SchemaBuilder;

import io.debezium.data.SpecialValueDecimal;
import io.debezium.data.VariableScaleDecimal;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.relational.Column;
import io.debezium.relational.ValueConverter;
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
    private static int NUMBER_VARIABLE_SCALE_LENGTH = 0;
    private static final Pattern INTERVAL_DAY_SECOND_PATTERN = Pattern.compile("([+\\-])?(\\d+) (\\d+):(\\d+):(\\d+).(\\d+)");

    private final OracleConnection connection;

    public OracleValueConverters(OracleConnection connection) {
        this.connection = connection;
    }

    @Override
    public SchemaBuilder schemaBuilder(Column column) {
        logger.debug("Building schema for column {} of type {} named {} with constraints ({},{})",
                column.name(),
                column.jdbcType(),
                column.typeName(),
                column.length(),
                column.scale()
        );

        switch (column.jdbcType()) {
            // Oracle's float is not float as in Java but a NUMERIC without scale
            case Types.FLOAT:
                return VariableScaleDecimal.builder();
            case Types.NUMERIC:
                return column.length() == NUMBER_VARIABLE_SCALE_LENGTH ? 
                        VariableScaleDecimal.builder() :
                        super.schemaBuilder(column);
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
            default:
                return super.schemaBuilder(column);
        }
    }

    @Override
    public ValueConverter converter(Column column, Field fieldDefn) {
        switch(column.jdbcType()) {
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.NCHAR:
            case Types.NVARCHAR:
                return data -> convertString(column, fieldDefn, data);
            case OracleTypes.BINARY_FLOAT:
                return data -> convertFloat(column, fieldDefn, data);
            case OracleTypes.BINARY_DOUBLE:
                return data -> convertDouble(column, fieldDefn, data);
            case Types.NUMERIC:
                    return column.length() == NUMBER_VARIABLE_SCALE_LENGTH ? 
                            data -> convertVariableScale(column, fieldDefn, data) :
                            data -> convertNumeric(column, fieldDefn, data);
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

    @Override
    protected Object convertString(Column column, Field fieldDefn, Object data) {
        if (data instanceof CHAR) {
            return ((CHAR)data).stringValue();
        }

        return super.convertString(column, fieldDefn, data);
    }

    @Override
    protected Object convertInteger(Column column, Field fieldDefn, Object data) {
        if (data instanceof NUMBER) {
            try {
                data = ((NUMBER)data).intValue();
            }
            catch (SQLException e) {
                throw new RuntimeException("Couldn't convert value for column " + column.name(), e);
            }
        }

        return super.convertInteger(column, fieldDefn, data);
    }

    @Override
    protected Object convertFloat(Column column, Field fieldDefn, Object data) {
        if (data instanceof NUMBER) {
            return ((NUMBER)data).floatValue();
        }
        else if (data instanceof BINARY_FLOAT) {
            try {
                return ((BINARY_FLOAT)data).floatValue();
            }
            catch (SQLException e) {
                throw new RuntimeException("Couldn't convert value for column " + column.name(), e);
            }
        }

        return super.convertFloat(column, fieldDefn, data);
    }

    @Override
    protected Object convertDouble(Column column, Field fieldDefn, Object data) {
        if (data instanceof BINARY_DOUBLE) {
            try {
                return ((BINARY_DOUBLE)data).doubleValue();
            }
            catch (SQLException e) {
                throw new RuntimeException("Couldn't convert value for column " + column.name(), e);
            }
        }

        return super.convertDouble(column, fieldDefn, data);
    }

    @Override
    protected Object convertDecimal(Column column, Field fieldDefn, Object data) {
        if (data instanceof NUMBER) {
            try {
                data = ((NUMBER)data).bigDecimalValue();
            }
            catch (SQLException e) {
                throw new RuntimeException("Couldn't convert value for column " + column.name(), e);
            }
        }

        return super.convertDecimal(column, fieldDefn, data);
    }

    @Override
    protected Object convertNumeric(Column column, Field fieldDefn, Object data) {
        if (data instanceof NUMBER) {
            try {
                data = ((NUMBER)data).bigDecimalValue();
            }
            catch (SQLException e) {
                throw new RuntimeException("Couldn't convert value for column " + column.name(), e);
            }
        }

        return super.convertNumeric(column, fieldDefn, data);
    }

    protected Object convertVariableScale(Column column, Field fieldDefn, Object data) {
        data = convertNumeric(column, fieldDefn, data);

        if (data == null) {
            return null;
        }
        // TODO Need to handle special values, it is not supported in variable scale decimal
        else if (data instanceof SpecialValueDecimal) {
            return VariableScaleDecimal.fromLogical(fieldDefn.schema(), (SpecialValueDecimal)data);
        }
        else if (data instanceof BigDecimal) {
            return VariableScaleDecimal.fromLogical(fieldDefn.schema(), new SpecialValueDecimal((BigDecimal)data));
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
                final TIMESTAMPTZ ts = (TIMESTAMPTZ)data;
                data = ZonedDateTime.ofInstant(ts.timestampValue(connection.connection()).toInstant(), ts.getTimeZone().toZoneId());
            }
            else if (data instanceof TIMESTAMPLTZ) {
                // JDBC driver throws an exception
//                final TIMESTAMPLTZ ts = (TIMESTAMPLTZ)data;
//                data = ts.offsetDateTimeValue(connection.connection());
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
        return super.convertTimestampToEpochMicros(column, fieldDefn, fromOracleTimeClasses(column, data));
    }

    @Override
    protected Object convertTimestampToEpochMillis(Column column, Field fieldDefn, Object data) {
        return super.convertTimestampToEpochMillis(column, fieldDefn, fromOracleTimeClasses(column, data));
    }

    @Override
    protected Object convertTimestampToEpochNanos(Column column, Field fieldDefn, Object data) {
        return super.convertTimestampToEpochNanos(column, fieldDefn, fromOracleTimeClasses(column, data));
    }

    @Override
    protected Object convertTimestampWithZone(Column column, Field fieldDefn, Object data) {
        return super.convertTimestampWithZone(column, fieldDefn, fromOracleTimeClasses(column, data));
    }

    protected Object convertIntervalYearMonth(Column column, Field fieldDefn, Object data) {
        if (data == null) {
            data = fieldDefn.schema().defaultValue();
        }
        if (data == null) {
            if (column.isOptional()) return null;
            return NumberConversions.DOUBLE_FALSE;
        }
        if (data instanceof Number) {
            // we expect to get back from the plugin a double value
            return ((Number) data).doubleValue();
        }
        if (data instanceof INTERVALYM) {
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
                    return MicroDuration.durationMicros(year, month, 0, 0,
                            0, 0, MicroDuration.DAYS_PER_MONTH_AVG);
                }
            }
        }
        return handleUnknownData(column, fieldDefn, data);
    }

    protected Object convertIntervalDaySecond(Column column, Field fieldDefn, Object data) {
        if (data == null) {
            data = fieldDefn.schema().defaultValue();
        }
        if (data == null) {
            if (column.isOptional()) return null;
            return NumberConversions.DOUBLE_FALSE;
        }
        if (data instanceof Number) {
            // we expect to get back from the plugin a double value
            return ((Number) data).doubleValue();
        }
        if (data instanceof INTERVALDS) {
            final String interval = ((INTERVALDS) data).stringValue();
            final Matcher m = INTERVAL_DAY_SECOND_PATTERN.matcher(interval);
            if (m.matches()) {
                final int sign = "-".equals(m.group(1)) ? -1 : 1;
                return MicroDuration.durationMicros(
                        0,
                        0,
                        sign * Integer.valueOf(m.group(2)),
                        sign * Integer.valueOf(m.group(3)),
                        sign * Integer.valueOf(m.group(4)),
                        sign * Integer.valueOf(m.group(5)),
                        sign * Integer.valueOf(Strings.pad(m.group(6), 6, '0')),
                        MicroDuration.DAYS_PER_MONTH_AVG);
            }
        }
        return handleUnknownData(column, fieldDefn, data);
    }
}
