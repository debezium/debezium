/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.oracle;

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
        // Hibernate will elect to use RAW(n) when column propagation is enabled, and we ideally do not want
        // to use that data type since RAW has been deprecated by Oracle. This explicitly always forces any
        // BYTES data type to be written as a BLOB.
        return "blob";
    }

    @Override
    public String getDefaultValueBinding(DatabaseDialect dialect, Schema schema, Object value) {
        // Cannot bind default value to BLOB columns
        return null;
    }
}
