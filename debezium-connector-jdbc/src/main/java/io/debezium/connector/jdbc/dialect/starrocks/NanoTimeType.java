/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.starrocks;

import java.time.Duration;

import io.debezium.connector.jdbc.type.JdbcType;
import io.debezium.time.NanoTime;

/**
 * An implementation of {@link JdbcType} for {@link NanoTime} values for StarRocks; times are
 * stored as formatted strings with microsecond precision (nanoseconds are truncated).
 */
class NanoTimeType extends AbstractTimeToStringType {

    public static final NanoTimeType INSTANCE = new NanoTimeType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ NanoTime.SCHEMA_NAME };
    }

    @Override
    protected Duration toDuration(Number value) {
        return Duration.ofNanos(value.longValue());
    }
}
