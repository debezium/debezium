/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.oracle;

import org.apache.kafka.connect.data.Schema;

import io.debezium.connector.jdbc.type.AbstractBytesType;
import io.debezium.connector.jdbc.type.Type;

/**
 * An implementation of {@link Type} for {@code BYTES} column types.
 *
 * @author Chris Cranford
 */
class BytesType extends AbstractBytesType {

    public static final BytesType INSTANCE = new BytesType();

    @Override
    public String getTypeName(Schema schema, boolean isKey) {
        // Hibernate will elect to use RAW(n) when column propagation is enabled, and we ideally do not want
        // to use that data type since RAW has been deprecated by Oracle. This explicitly always forces any
        // BYTES data type to be written as a BLOB.
        return "blob";
    }

    @Override
    public String getDefaultValueBinding(Schema schema, Object value) {
        // Cannot bind default value to BLOB columns
        return null;
    }
}
