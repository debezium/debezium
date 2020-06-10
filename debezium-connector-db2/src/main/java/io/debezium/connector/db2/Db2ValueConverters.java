/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2;

import java.sql.Types;
import java.time.ZoneOffset;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.SchemaBuilder;

import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.Column;
import io.debezium.relational.ValueConverter;

/**
 * Conversion of DB2 specific datatypes.
 *
 * @author Jiri Pechanec, Peter Urbanetz
 *
 */
public class Db2ValueConverters extends JdbcValueConverters {

    public Db2ValueConverters() {
    }

    /**
     * Create a new instance that always uses UTC for the default time zone when
     * converting values without timezone information to values that require
     * timezones.
     * <p>
     *
     * @param decimalMode
     *            how {@code DECIMAL} and {@code NUMERIC} values should be
     *            treated; may be null if
     *            {@link io.debezium.jdbc.JdbcValueConverters.DecimalMode#PRECISE}
     *            is to be used
     * @param temporalPrecisionMode
     *            date/time value will be represented either as Connect datatypes or Debezium specific datatypes
     */
    public Db2ValueConverters(DecimalMode decimalMode, TemporalPrecisionMode temporalPrecisionMode) {
        super(decimalMode, temporalPrecisionMode, ZoneOffset.UTC, null, null, null);
    }

    @Override
    public SchemaBuilder schemaBuilder(Column column) {
        switch (column.jdbcType()) {
            // Numeric integers
            case Types.TINYINT:
                // values are an 8-bit unsigned integer value between 0 and 255, we thus need to store it in short int
                return SchemaBuilder.int16();
            default:
                return super.schemaBuilder(column);
        }
    }

    @Override
    public ValueConverter converter(Column column, Field fieldDefn) {
        switch (column.jdbcType()) {
            // Numeric integers
            case Types.TINYINT:
                // values are an 8-bit unsigned integer value between 0 and 255, we thus need to store it in short int
                return (data) -> convertSmallInt(column, fieldDefn, data);
            default:
                return super.converter(column, fieldDefn);
        }
    }

    /**
     * Time precision in DB2 is defined in scale, the default one is 7
     */
    @Override
    protected int getTimePrecision(Column column) {
        return column.scale().get();
    }

    protected Object convertTimestampWithZone(Column column, Field fieldDefn, Object data) {
        // dummy return
        return super.convertTimestampWithZone(column, fieldDefn, data);
    }

}