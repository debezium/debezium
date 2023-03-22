/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.db2;

import org.apache.kafka.connect.data.Schema;

import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.type.AbstractType;
import io.debezium.connector.jdbc.type.Type;

/**
 * An implementation of {@link Type} for {@code BYTES} column types.
 *
 * @author Chris Cranford
 */
class BytesType extends AbstractType {

    public static final BytesType INSTANCE = new BytesType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ "BYTES" };
    }

    @Override
    public String getTypeName(DatabaseDialect dialect, Schema schema, boolean key) {
        // Hibernate defaults to VARCHAR(n) FOR BIT DATA when using Types.VARBINARY.
        // Override this behavior and explicitly map any BYTES type to a "blob".
        return "blob";
    }

    @Override
    public String getDefaultValueBinding(DatabaseDialect dialect, Schema schema, Object value) {
        // Cannot bind default value to BLOB columns
        return null;
    }
}
