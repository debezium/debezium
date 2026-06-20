/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.postgres;

import java.math.BigDecimal;
import java.sql.Types;
import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import io.debezium.connector.jdbc.type.AbstractType;
import io.debezium.sink.column.ColumnDescriptor;
import io.debezium.sink.valuebinding.ValueBindDescriptor;
import io.debezium.time.StructuredDuration;
import io.debezium.time.StructuredTemporal;

/**
 * PostgreSQL implementation of {@link StructuredDuration} values.
 */
public class StructuredDurationType extends AbstractType {

    public static final StructuredDurationType INSTANCE = new StructuredDurationType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ StructuredDuration.SCHEMA_NAME };
    }

    @Override
    public String getQueryBinding(ColumnDescriptor column, Schema schema, Object value) {
        return "cast(? as interval)";
    }

    @Override
    public String getTypeName(Schema schema, boolean isKey) {
        return "interval";
    }

    @Override
    public String getDefaultValueBinding(Schema schema, Object value) {
        return String.format("'%s'", toIntervalLiteral(requireStruct(value)));
    }

    @Override
    public List<ValueBindDescriptor> bind(int index, Schema schema, Object value) {
        if (value == null) {
            return List.of(new ValueBindDescriptor(index, null));
        }
        return List.of(new ValueBindDescriptor(index, toIntervalLiteral(requireStruct(value)), Types.VARCHAR));
    }

    private String toIntervalLiteral(Struct value) {
        final StringBuilder builder = new StringBuilder();
        append(builder, value.getInt32(StructuredTemporal.YEARS_FIELD), " years");
        append(builder, value.getInt32(StructuredTemporal.MONTHS_FIELD), " months");
        append(builder, value.getInt32(StructuredTemporal.DAYS_FIELD), " days");
        append(builder, value.getInt32(StructuredTemporal.HOURS_FIELD), " hours");
        append(builder, value.getInt32(StructuredTemporal.MINUTES_FIELD), " minutes");

        final Long seconds = value.getInt64(StructuredTemporal.SECONDS_FIELD);
        final Integer nanos = value.getInt32(StructuredTemporal.NANOS_FIELD);
        final BigDecimal fractionalSeconds = BigDecimal.valueOf(seconds == null ? 0 : seconds)
                .add(BigDecimal.valueOf(nanos == null ? 0 : nanos, 9))
                .stripTrailingZeros();
        if (fractionalSeconds.signum() != 0 || builder.length() == 0) {
            append(builder, fractionalSeconds.toPlainString(), " seconds");
        }
        return builder.toString().trim();
    }

    private void append(StringBuilder builder, Integer value, String suffix) {
        if (value != null && value != 0) {
            append(builder, value.toString(), suffix);
        }
    }

    private void append(StringBuilder builder, String value, String suffix) {
        if (builder.length() > 0) {
            builder.append(' ');
        }
        builder.append(value).append(suffix);
    }
}
