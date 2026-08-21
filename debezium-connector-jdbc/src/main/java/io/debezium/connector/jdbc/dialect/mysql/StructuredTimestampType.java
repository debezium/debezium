/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.mysql;

import java.sql.Types;
import java.time.DateTimeException;
import java.time.LocalDateTime;
import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import io.debezium.connector.jdbc.type.debezium.StructuredTemporalSupport;
import io.debezium.sink.valuebinding.ValueBindDescriptor;
import io.debezium.time.StructuredTemporal;

/**
 * MySQL implementation of {@link io.debezium.time.StructuredTimestamp} values.
 */
public class StructuredTimestampType extends io.debezium.connector.jdbc.type.debezium.StructuredTimestampType {

    public static final StructuredTimestampType INSTANCE = new StructuredTimestampType();

    @Override
    public String getDefaultValueBinding(Schema schema, Object value) {
        return "'" + StructuredTemporalLiteral.timestamp(requireStruct(value)) + "'";
    }

    @Override
    public List<ValueBindDescriptor> bind(int index, Schema schema, Object value) {
        if (value == null) {
            return List.of(new ValueBindDescriptor(index, null));
        }
        final Struct struct = requireStruct(value);
        final LocalDateTime clamped = clampIfOutOfRange(struct);
        if (clamped != null) {
            return List.of(new ValueBindDescriptor(index, StructuredTemporalLiteral.timestamp(clamped), Types.VARCHAR));
        }
        return List.of(new ValueBindDescriptor(index, StructuredTemporalLiteral.timestamp(struct), Types.VARCHAR));
    }

    /**
     * Returns the clamped date-time when the structured value is a finite, valid calendar value that
     * lies outside the dialect's supported range; otherwise returns {@code null} so that the value is
     * bound from its raw components, preserving MySQL's tolerance for invalid calendar literals.
     */
    private LocalDateTime clampIfOutOfRange(Struct struct) {
        if (!StructuredTemporal.isFinite(struct)) {
            return null;
        }
        final LocalDateTime localDateTime;
        try {
            localDateTime = StructuredTemporalSupport.toLocalDateTime(struct);
        }
        catch (DateTimeException e) {
            return null;
        }
        final LocalDateTime clamped = clampIfOutOfRange(localDateTime);
        return clamped.equals(localDateTime) ? null : clamped;
    }
}
