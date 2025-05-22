/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.type.debezium;

import java.time.LocalTime;

import io.debezium.connector.jdbc.type.JdbcType;
import io.debezium.connector.jdbc.util.DateTimeUtils;
import io.debezium.time.MicroTime;

/**
 * An implementation of {@link JdbcType} for {@link MicroTime} values.
 *
 * @author Chris Cranford
 */
public class MicroTimeType extends AbstractDebeziumTimeType {

    public static final MicroTimeType INSTANCE = new MicroTimeType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ MicroTime.SCHEMA_NAME };
    }

    @Override
    protected LocalTime getLocalTime(Number value) {
        return DateTimeUtils.toLocalTimeFromDurationMicroseconds(value.longValue());
    }

}
