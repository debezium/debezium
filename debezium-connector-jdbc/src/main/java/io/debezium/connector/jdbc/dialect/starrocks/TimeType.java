/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.starrocks;

import java.time.Duration;

import io.debezium.connector.jdbc.type.JdbcType;
import io.debezium.time.Time;

/**
 * An implementation of {@link JdbcType} for {@link Time} values for StarRocks; times are
 * stored as formatted strings.
 */
class TimeType extends AbstractTimeToStringType {

    public static final TimeType INSTANCE = new TimeType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ Time.SCHEMA_NAME };
    }

    @Override
    protected Duration toDuration(Number value) {
        return Duration.ofMillis(value.longValue());
    }
}
