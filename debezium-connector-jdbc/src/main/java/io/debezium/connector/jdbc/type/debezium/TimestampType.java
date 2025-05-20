/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.type.debezium;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import org.apache.kafka.connect.data.Schema;

import io.debezium.connector.jdbc.type.Type;
import io.debezium.time.Timestamp;

/**
 * An implementation of {@link Type} for {@link Timestamp} values.
 *
 * @author Chris Cranford
 */
public class TimestampType extends AbstractDebeziumTimestampType {

    public static final TimestampType INSTANCE = new TimestampType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ Timestamp.SCHEMA_NAME };
    }

    @Override
    public String getDefaultValueBinding(Schema schema, Object value) {
        return getDialect().getFormattedDateTime(Instant.ofEpochMilli((long) value).atZone(ZoneOffset.UTC));
    }

    @Override
    protected LocalDateTime getLocalDateTime(long value) {
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(value), ZoneOffset.UTC);
    }

}
