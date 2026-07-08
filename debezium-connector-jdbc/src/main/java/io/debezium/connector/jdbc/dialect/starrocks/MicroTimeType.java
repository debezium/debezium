/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.starrocks;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

import io.debezium.connector.jdbc.type.JdbcType;
import io.debezium.time.MicroTime;

/**
 * An implementation of {@link JdbcType} for {@link MicroTime} values for StarRocks; times are
 * stored as formatted strings.
 */
class MicroTimeType extends AbstractTimeToStringType {

    public static final MicroTimeType INSTANCE = new MicroTimeType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ MicroTime.SCHEMA_NAME };
    }

    @Override
    protected Duration toDuration(Number value) {
        return Duration.of(value.longValue(), ChronoUnit.MICROS);
    }
}
