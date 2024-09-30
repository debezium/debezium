/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.mysql;

import org.apache.kafka.connect.data.Schema;

import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.type.AbstractType;
import io.debezium.connector.jdbc.type.Type;

/**
 * An implementation of {@link Type} for {@code BOOLEAN} data types.
 *
 * @author Chris Cranford
 */
class BooleanType extends AbstractType {

    public static final BooleanType INSTANCE = new BooleanType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ "BOOLEAN" };
    }

    @Override
    public String getTypeName(DatabaseDialect dialect, Schema schema, boolean key) {
        // Hibernate picks "BIT" over "BOOLEAN"; this allows us to override that behavior.
        return "boolean";
    }

}
