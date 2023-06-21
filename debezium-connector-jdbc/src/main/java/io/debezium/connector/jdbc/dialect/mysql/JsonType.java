/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.mysql;

import org.apache.kafka.connect.data.Schema;

import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.relational.ColumnDescriptor;
import io.debezium.connector.jdbc.type.AbstractType;
import io.debezium.connector.jdbc.type.Type;
import io.debezium.data.Json;

/**
 * An implementation of {@link Type} for {@link Json} types.
 *
 * @author Chris Cranford
 */
class JsonType extends AbstractType {

    public static final JsonType INSTANCE = new JsonType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ Json.LOGICAL_NAME };
    }

    @Override
    public String getQueryBinding(ColumnDescriptor column, Schema schema) {
        return "cast(? as json)";
    }

    @Override
    public String getDefaultValueBinding(DatabaseDialect dialect, Schema schema, Object value) {
        // No default value is permitted
        return null;
    }

    @Override
    public String getTypeName(DatabaseDialect dialect, Schema schema, boolean key) {
        return "json";
    }

}
