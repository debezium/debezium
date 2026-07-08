/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.starrocks;

import org.apache.kafka.connect.data.Schema;

import io.debezium.connector.jdbc.type.AbstractType;
import io.debezium.connector.jdbc.type.JdbcType;
import io.debezium.data.EnumSet;

/**
 * An implementation of {@link JdbcType} for {@code SET} column types for StarRocks, which
 * has no native SET type; values are stored as comma-separated strings.
 */
class SetType extends AbstractType {

    public static final SetType INSTANCE = new SetType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ EnumSet.LOGICAL_NAME };
    }

    @Override
    public String getTypeName(Schema schema, boolean isKey) {
        return "string";
    }
}
