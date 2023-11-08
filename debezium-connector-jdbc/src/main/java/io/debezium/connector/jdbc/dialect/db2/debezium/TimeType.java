/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.db2.debezium;

import java.time.LocalTime;

import io.debezium.connector.jdbc.type.Type;
import io.debezium.connector.jdbc.util.DateTimeUtils;
import io.debezium.time.Time;

/**
 * An implementation of {@link Type} for {@link Time} values.
 *
 * @author Chris Cranford
 */
public class TimeType extends AbstractDebeziumTimeType {

    public static final TimeType INSTANCE = new TimeType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ Time.SCHEMA_NAME };
    }

    @Override
    protected LocalTime getLocalTime(Number value) {
        return DateTimeUtils.toLocalTimeFromDurationMilliseconds(value.longValue());
    }

}
