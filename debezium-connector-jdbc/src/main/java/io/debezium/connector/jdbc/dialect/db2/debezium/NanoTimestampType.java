/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.db2.debezium;

import java.time.LocalDateTime;

import org.apache.kafka.connect.data.Schema;

import io.debezium.connector.jdbc.type.JdbcType;
import io.debezium.connector.jdbc.util.DateTimeUtils;
import io.debezium.time.MicroTimestamp;
import io.debezium.time.NanoTimestamp;

/**
 * An implementation of {@link JdbcType} for {@link MicroTimestamp} values.
 *
 * @author Chris Cranford
 */
public class NanoTimestampType extends AbstractDebeziumTimestampType {

    public static final NanoTimestampType INSTANCE = new NanoTimestampType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ NanoTimestamp.SCHEMA_NAME };
    }

    @Override
    public String getDefaultValueBinding(Schema schema, Object value) {
        return getDialect().getFormattedDateTimeWithNanos(DateTimeUtils.toZonedDateTimeFromInstantEpochNanos((long) value));
    }

    @Override
    protected LocalDateTime getLocalDateTime(long value) {
        return DateTimeUtils.toLocalDateTimeFromInstantEpochNanos(value);
    }

}
