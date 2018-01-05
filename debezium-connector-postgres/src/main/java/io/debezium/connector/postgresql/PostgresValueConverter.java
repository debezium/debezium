/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.postgresql.geometric.PGpoint;
import org.postgresql.util.PGInterval;
import org.postgresql.util.PGobject;

import io.debezium.connector.postgresql.proto.PgProto;
import io.debezium.data.Bits;
import io.debezium.data.Json;
import io.debezium.data.Uuid;
import io.debezium.data.VariableScaleDecimal;
import io.debezium.data.geometry.Point;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.Column;
import io.debezium.relational.ValueConverter;
import io.debezium.time.MicroDuration;
import io.debezium.time.ZonedTime;
import io.debezium.time.ZonedTimestamp;
import io.debezium.util.NumberConversions;

/**
 * A provider of {@link ValueConverter}s and {@link SchemaBuilder}s for various Postgres specific column types.
 *
 * In addition to handling data type conversion from values coming from JDBC, this is also expected to handle data type
 * conversion for data types coming from the logical decoding plugin.
 *
 * @author Horia Chiorean (hchiorea@redhat.com)
 */
public class PostgresValueConverter extends JdbcValueConverters {

    /**
     * The approximation used by the plugin when converting a duration to micros
     */
    protected static final double DAYS_PER_MONTH_AVG = 365.25 / 12.0d;

    /**
     * Variable scale decimal/numeric is defined by metadata
     * scale - 0
     * length - 131089
     */
    private static final int VARIABLE_SCALE_DECIMAL_LENGTH = 131089;

    /**
     * {@code true} if fields of data type not know should be handle as opaque binary;
     * {@code false} if they should be omitted
     */
    private final boolean includeUnknownDatatypes;

    protected PostgresValueConverter(DecimalMode decimalMode, TemporalPrecisionMode temporalPrecisionMode, ZoneOffset defaultOffset, BigIntUnsignedMode bigIntUnsignedMode, boolean includeUnknownDatatypes) {
        super(decimalMode, temporalPrecisionMode, defaultOffset, null, bigIntUnsignedMode);
        this.includeUnknownDatatypes = includeUnknownDatatypes;
    }

    @Override
    public SchemaBuilder schemaBuilder(Column column) {
        int oidValue = PgOid.jdbcColumnToOid(column);
        switch (oidValue) {
            case PgOid.BIT:
            case PgOid.BIT_ARRAY:
            case PgOid.VARBIT:
                return column.length() > 1 ? Bits.builder(column.length()) : SchemaBuilder.bool();
            case PgOid.INTERVAL:
                return MicroDuration.builder();
            case PgOid.TIMESTAMPTZ:
                // JDBC reports this as "timestamp" even though it's with tz, so we can't use the base class...
                return ZonedTimestamp.builder();
            case PgOid.TIMETZ:
                // JDBC reports this as "time" but this contains TZ information
                return ZonedTime.builder();
            case PgOid.OID:
                return SchemaBuilder.int64();
            case PgOid.JSONB_JDBC_OID:
            case PgOid.JSONB_OID:
            case PgOid.JSON:
                return Json.builder();
            case PgOid.TSTZRANGE_OID:
                return SchemaBuilder.string();
            case PgOid.UUID:
                return Uuid.builder();
            case PgOid.POINT:
                return Point.builder();
            case PgOid.MONEY:
                return Decimal.builder(column.scale());
            case PgOid.NUMERIC:
                return numericSchema(column).optional();
            case PgOid.INT2_ARRAY:
                return SchemaBuilder.array(SchemaBuilder.OPTIONAL_INT16_SCHEMA);
            case PgOid.INT4_ARRAY:
                return SchemaBuilder.array(SchemaBuilder.OPTIONAL_INT32_SCHEMA);
            case PgOid.INT8_ARRAY:
                return SchemaBuilder.array(SchemaBuilder.OPTIONAL_INT64_SCHEMA);
            case PgOid.CHAR_ARRAY:
            case PgOid.VARCHAR_ARRAY:
            case PgOid.TEXT_ARRAY:
            case PgOid.BPCHAR_ARRAY:
                return SchemaBuilder.array(SchemaBuilder.OPTIONAL_STRING_SCHEMA);
            case PgOid.NUMERIC_ARRAY:
                return SchemaBuilder.array(numericSchema(column).optional());
            case PgOid.FLOAT4_ARRAY:
                return SchemaBuilder.array(Schema.OPTIONAL_FLOAT32_SCHEMA);
            case PgOid.FLOAT8_ARRAY:
                return SchemaBuilder.array(Schema.OPTIONAL_FLOAT64_SCHEMA);
            case PgOid.BOOL_ARRAY:
                return SchemaBuilder.array(SchemaBuilder.OPTIONAL_BOOLEAN_SCHEMA);
            case PgOid.DATE_ARRAY:
                if (adaptiveTimePrecisionMode || adaptiveTimeMicrosecondsPrecisionMode) {
                    return SchemaBuilder.array(io.debezium.time.Date.builder().optional().build());
                }
                return SchemaBuilder.array(org.apache.kafka.connect.data.Date.builder().optional().build());
            case PgOid.TIME_ARRAY:
            case PgOid.TIMETZ_ARRAY:
            case PgOid.TIMESTAMP_ARRAY:
            case PgOid.TIMESTAMPTZ_ARRAY:
            case PgOid.BYTEA_ARRAY:
            case PgOid.OID_ARRAY:
            case PgOid.MONEY_ARRAY:
            case PgOid.NAME_ARRAY:
            case PgOid.INTERVAL_ARRAY:
            case PgOid.VARBIT_ARRAY:
            case PgOid.UUID_ARRAY:
            case PgOid.XML_ARRAY:
            case PgOid.POINT_ARRAY:
            case PgOid.JSONB_ARRAY:
            case PgOid.JSON_ARRAY:
            case PgOid.REF_CURSOR_ARRAY:
                // These array types still need to be implemented.  The superclass won't handle them so
                // we return null here until we can code schema implementations for them.
                return null;
            case PgOid.UNSPECIFIED:
                return includeUnknownDatatypes ? SchemaBuilder.bytes() : super.schemaBuilder(column);
            default:
                return super.schemaBuilder(column);
        }
    }

    private SchemaBuilder numericSchema(final Column column) {
        switch (decimalMode) {
            case DOUBLE:
                return SchemaBuilder.float64();
            case PRECISE:
                return isVariableScaleDecimal(column) ? VariableScaleDecimal.builder() : Decimal.builder(column.scale());
            default:
                throw new IllegalArgumentException("Unknown decimalMode");
        }
    }

    @Override
    public ValueConverter converter(Column column, Field fieldDefn) {
        int oidValue = PgOid.jdbcColumnToOid(column);
        switch (oidValue) {
            case PgOid.BIT:
            case PgOid.VARBIT:
                return convertBits(column, fieldDefn);
            case PgOid.INTERVAL:
                return data -> convertInterval(column, fieldDefn, data);
            case PgOid.TIMESTAMPTZ:
                return data -> convertTimestampWithZone(column, fieldDefn, data);
            case PgOid.TIMETZ:
                return data -> convertTimeWithZone(column, fieldDefn, data);
            case PgOid.OID:
                return data -> convertBigInt(column, fieldDefn, data);
            case PgOid.JSONB_JDBC_OID:
            case PgOid.JSONB_OID:
            case PgOid.UUID:
            case PgOid.TSTZRANGE_OID:
            case PgOid.JSON:
                return data -> super.convertString(column, fieldDefn, data);
            case PgOid.POINT:
                return data -> convertPoint(column, fieldDefn, data);
            case PgOid.MONEY:
                return data -> convertMoney(column, fieldDefn, data);
            case PgOid.NUMERIC:
                switch (decimalMode) {
                case DOUBLE:
                    return (data) -> convertDouble(column, fieldDefn, data);
                case PRECISE:
                    return (data) -> convertDecimal(column, fieldDefn, data);
                }
            case PgOid.INT2_ARRAY:
            case PgOid.INT4_ARRAY:
            case PgOid.INT8_ARRAY:
            case PgOid.CHAR_ARRAY:
            case PgOid.VARCHAR_ARRAY:
            case PgOid.TEXT_ARRAY:
            case PgOid.BPCHAR_ARRAY:
            case PgOid.NUMERIC_ARRAY:
            case PgOid.FLOAT4_ARRAY:
            case PgOid.FLOAT8_ARRAY:
            case PgOid.BOOL_ARRAY:
            case PgOid.DATE_ARRAY:
                return data -> convertArray(column, fieldDefn, data);

            // TODO DBZ-459 implement support for these array types; for now we just fall back to the default, i.e.
            // having no converter, so to be consistent with the schema definitions above
            case PgOid.TIME_ARRAY:
            case PgOid.TIMETZ_ARRAY:
            case PgOid.TIMESTAMP_ARRAY:
            case PgOid.TIMESTAMPTZ_ARRAY:
            case PgOid.BYTEA_ARRAY:
            case PgOid.OID_ARRAY:
            case PgOid.MONEY_ARRAY:
            case PgOid.NAME_ARRAY:
            case PgOid.INTERVAL_ARRAY:
            case PgOid.VARBIT_ARRAY:
            case PgOid.UUID_ARRAY:
            case PgOid.XML_ARRAY:
            case PgOid.POINT_ARRAY:
            case PgOid.JSONB_ARRAY:
            case PgOid.JSON_ARRAY:
            case PgOid.REF_CURSOR_ARRAY:
                return super.converter(column, fieldDefn);
            case PgOid.UNSPECIFIED:
                return includeUnknownDatatypes ? data -> convertBinary(column, fieldDefn, data) : super.converter(column, fieldDefn);
            default:
                return super.converter(column, fieldDefn);
        }
    }

    @Override
    protected Object convertDecimal(Column column, Field fieldDefn, Object data) {
        BigDecimal newDecimal = (BigDecimal) super.convertDecimal(column, fieldDefn, data);
        if (newDecimal == null) {
            return newDecimal;
        }
        if (column.scale() > newDecimal.scale()) {
          newDecimal = newDecimal.setScale(column.scale());
        }
        if (isVariableScaleDecimal(column)) {
            return VariableScaleDecimal.fromLogical(fieldDefn.schema(), newDecimal);
        }
        return newDecimal;
    }

    @Override
    protected Object convertBit(Column column, Field fieldDefn, Object data) {
        if (data instanceof String) {
            data = Integer.valueOf((String) data, 2);
        }
        return super.convertBit(column, fieldDefn, data);
    }

    @Override
    protected Object convertBits(Column column, Field fieldDefn, Object data, int numBytes) {
        if (data instanceof PGobject) {
            // returned by the JDBC driver
            data = ((PGobject) data).getValue();
        }
        if (data instanceof String) {
            long longValue = Long.parseLong((String) data, 2);
            // return the smallest possible value
            if (Short.MIN_VALUE <= longValue && longValue <= Short.MAX_VALUE) {
                data = (short) longValue;
            } else if (Integer.MIN_VALUE <= longValue && longValue <= Integer.MAX_VALUE) {
                data = (int) longValue;
            } else {
                data = longValue;
            }
        }
        return super.convertBits(column, fieldDefn, data, numBytes);
    }

    protected Object convertMoney(Column column, Field fieldDefn, Object data) {
        if (data == null) {
            data = fieldDefn.schema().defaultValue();
        }
        if (data == null) {
            if (column.isOptional()) return null;
            return 0L;
        }
        if (data instanceof Double) {
            return BigDecimal.valueOf((Double) data);
        }
        if (data instanceof Number) {
            // the plugin will return a 64bit signed integer where the last 2 are always decimals
            return BigDecimal.valueOf(((Number)data).longValue(), 2);
        }
        return handleUnknownData(column, fieldDefn, data);
    }

    protected Object convertInterval(Column column, Field fieldDefn, Object data) {
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
        if (data instanceof PGInterval) {
            PGInterval interval = (PGInterval) data;
            return MicroDuration.durationMicros(interval.getYears(), interval.getMonths(), interval.getDays(), interval.getHours(),
                                                interval.getMinutes(), interval.getSeconds(), DAYS_PER_MONTH_AVG);
        }
        return handleUnknownData(column, fieldDefn, data);
    }

    @Override
    protected Object convertTimestampToEpochMillis(Column column, Field fieldDefn, Object data) {
        if (data instanceof Long) {
            data = nanosToLocalDateTimeUTC((Long) data);
        }
        return super.convertTimestampToEpochMillis(column, fieldDefn, data);
    }

    @Override
    protected Object convertTimestampToEpochMicros(Column column, Field fieldDefn, Object data) {
        if (data instanceof Long) {
            data = nanosToLocalDateTimeUTC((Long) data);
        }
        return super.convertTimestampToEpochMicros(column, fieldDefn, data);
    }

    @Override
    protected Object convertTimestampToEpochNanos(Column column, Field fieldDefn, Object data) {
        if (data instanceof Long) {
            data = nanosToLocalDateTimeUTC((Long) data);
        }
        return super.convertTimestampToEpochNanos(column, fieldDefn, data);
    }

    @Override
    protected Object convertTimestampToEpochMillisAsDate(Column column, Field fieldDefn, Object data) {
        if (data instanceof Long) {
            data = nanosToLocalDateTimeUTC((Long) data);
        }
        return super.convertTimestampToEpochMillisAsDate(column, fieldDefn, data);
    }

    @Override
    protected Object convertTimestampWithZone(Column column, Field fieldDefn, Object data) {
        if (data instanceof Long) {
            LocalDateTime localDateTime = nanosToLocalDateTimeUTC((Long) data);
            data = OffsetDateTime.of(localDateTime, ZoneOffset.UTC);
        } else if (data instanceof java.util.Date) {
            // any Date like subclasses will be given to us by the JDBC driver, which uses the local VM TZ, so we need to go
            // back to GMT
            data = OffsetDateTime.ofInstant(Instant.ofEpochMilli(((Date) data).getTime()), ZoneOffset.UTC);
        }
        return super.convertTimestampWithZone(column, fieldDefn, data);
    }

    @Override
    protected Object convertTimeWithZone(Column column, Field fieldDefn, Object data) {
        if (data instanceof Long) {
            LocalTime localTime = LocalTime.ofNanoOfDay((Long) data);
            data = OffsetTime.of(localTime, ZoneOffset.UTC);
        } else if (data instanceof java.util.Date) {
            // any Date like subclasses will be given to us by the JDBC driver, which uses the local VM TZ, so we need to go
            // back to GMT
            data = OffsetTime.ofInstant(Instant.ofEpochMilli(((Date) data).getTime()), ZoneOffset.UTC);
        }
        return super.convertTimeWithZone(column, fieldDefn, data);
    }

    private static LocalDateTime nanosToLocalDateTimeUTC(long epocNanos) {
        // the pg plugin stores date/time info as microseconds since epoch
        BigInteger epochMicrosBigInt = BigInteger.valueOf(epocNanos);
        BigInteger[] secondsAndNanos = epochMicrosBigInt.divideAndRemainder(BigInteger.valueOf(TimeUnit.SECONDS.toNanos(1)));
        return LocalDateTime.ofInstant(Instant.ofEpochSecond(secondsAndNanos[0].longValue(), secondsAndNanos[1].longValue()),
                                       ZoneOffset.UTC);
    }

    /**
     * Converts a value representing a Postgres point for a column, to a Kafka Connect value.
     *
     * @param column the JDBC column; never null
     * @param fieldDefn the Connect field definition for this column; never null
     * @param data a data for the point column, either coming from the JDBC driver or logical decoding plugin
     * @return a value which will be used by Connect to represent the actual point value
     */
    protected Object convertPoint(Column column, Field fieldDefn, Object data) {
        if (data == null) {
            data = fieldDefn.schema().defaultValue();
        }
        Schema schema = fieldDefn.schema();
        if (data == null) {
            if (column.isOptional()) return null;
            //TODO author=Horia Chiorean date=28/10/2016 description=is this ok ?
            return Point.createValue(schema, 0, 0);
        }
        if (data instanceof PGpoint) {
            PGpoint pgPoint = (PGpoint) data;
            return Point.createValue(schema, pgPoint.x, pgPoint.y);
        }
        if (data instanceof String) {
            String dataString = data.toString();
            try {
                PGpoint pgPoint = new PGpoint(dataString);
                return Point.createValue(schema, pgPoint.x, pgPoint.y);
            } catch (SQLException e) {
                logger.warn("Error converting the string '{}' to a PGPoint type for the column '{}'", dataString, column);
            }
        }
        if (data instanceof PgProto.Point) {
            return Point.createValue(schema, ((PgProto.Point) data).getX(), ((PgProto.Point) data).getY());
        }
        return handleUnknownData(column, fieldDefn, data);
    }

    protected Object convertArray(Column column, Field fieldDefn, Object data) {
        if (data == null) {
            data = fieldDefn.schema().defaultValue();
        }
        if (data == null) {
            if (column.isOptional()) {
                return null;
            }
            else {
                return Collections.emptyList();
            }
        }
        // RecordStreamProducer and RecordsSnapshotProducer should ensure this arrives as a list
        if (!(data instanceof List)) {
            return handleUnknownData(column, fieldDefn, data);
        }
        return data;
    }

    private boolean isVariableScaleDecimal(final Column column) {
        return (column.scale() == 0 && column.length() == VARIABLE_SCALE_DECIMAL_LENGTH)
                || (column.scale() == -1 && column.length() == -1);
    }
}
