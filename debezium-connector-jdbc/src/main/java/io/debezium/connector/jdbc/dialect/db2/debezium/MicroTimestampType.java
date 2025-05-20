/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.db2.debezium;

import java.time.LocalDateTime;

import org.apache.kafka.connect.data.Schema;

import io.debezium.connector.jdbc.type.Type;
import io.debezium.connector.jdbc.util.DateTimeUtils;
import io.debezium.time.MicroTimestamp;

/**
 * An implementation of {@link Type} for {@link MicroTimestamp} values.
 *
 * @author Chris Cranford
 */
public class MicroTimestampType extends AbstractDebeziumTimestampType {

    public static final MicroTimestampType INSTANCE = new MicroTimestampType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ MicroTimestamp.SCHEMA_NAME };
    }

    @Override
    public String getDefaultValueBinding(Schema schema, Object value) {
        return getDialect().getFormattedDateTime(DateTimeUtils.toZonedDateTimeFromInstantEpochMicros((long) value));
    }

    @Override
    protected LocalDateTime getLocalDateTime(long value) {
        return DateTimeUtils.toLocalDateTimeFromInstantEpochMicros(value);
    }

}
