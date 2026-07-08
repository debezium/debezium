/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.starrocks;

import org.apache.kafka.connect.data.Schema;

import io.debezium.connector.jdbc.type.AbstractType;
import io.debezium.connector.jdbc.type.JdbcType;
import io.debezium.time.Year;

/**
 * An implementation of {@link JdbcType} for {@code YEAR} column types for StarRocks, which
 * has no native YEAR type; values are stored as integers without loss.
 */
class YearType extends AbstractType {

    public static final YearType INSTANCE = new YearType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ Year.SCHEMA_NAME };
    }

    @Override
    public String getTypeName(Schema schema, boolean isKey) {
        return "int";
    }
}
