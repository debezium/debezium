/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.cockroachdb;

import org.apache.kafka.connect.data.Schema;

import io.debezium.connector.jdbc.type.AbstractType;
import io.debezium.connector.jdbc.type.JdbcType;

/**
 * An implementation of {@link JdbcType} for {@code MONEY} column types. CockroachDB has no money
 * type, so the value is stored as a fixed-scale decimal.
 *
 * @author Virag Tripathi
 */
class MoneyType extends AbstractType {

    public static final MoneyType INSTANCE = new MoneyType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ "MONEY" };
    }

    @Override
    public String getTypeName(Schema schema, boolean isKey) {
        return "decimal(19,2)";
    }

}
