/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.starrocks;

import org.apache.kafka.connect.data.Schema;

import io.debezium.connector.jdbc.type.AbstractType;
import io.debezium.connector.jdbc.type.JdbcType;
import io.debezium.data.Enum;

/**
 * An implementation of {@link JdbcType} for {@code ENUM} column types for StarRocks, which
 * has no native ENUM type; values are stored as strings. The allowed value set is not
 * enforced by the sink database.
 */
class EnumType extends AbstractType {

    public static final EnumType INSTANCE = new EnumType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ Enum.LOGICAL_NAME };
    }

    @Override
    public String getTypeName(Schema schema, boolean isKey) {
        return "string";
    }
}
