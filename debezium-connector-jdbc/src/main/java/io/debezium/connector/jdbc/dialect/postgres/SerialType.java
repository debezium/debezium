/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.postgres;

import org.apache.kafka.connect.data.Schema;

import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.type.AbstractType;
import io.debezium.connector.jdbc.type.Type;

/**
 * An implementation of {@link Type} for {@code SMALLSERIAL}, {@code SERIAL}, and {@code BIGSERIAL}
 * column types.
 *
 * @author Chris Cranford
 */
public class SerialType extends AbstractType {

    public static final SerialType INSTANCE = new SerialType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ "SMALLSERIAL", "SERIAL", "BIGSERIAL" };
    }

    @Override
    public String getTypeName(DatabaseDialect dialect, Schema schema, boolean key) {
        return getSourceColumnType(schema).orElseThrow();
    }

    @Override
    public String getDefaultValueBinding(DatabaseDialect dialect, Schema schema, Object value) {
        // PostgreSQL does not allow specifying a default value for these data types
        // By returning a null value, no default value clause gets bound
        return null;
    }

}
