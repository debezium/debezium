/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.maria;

import org.apache.kafka.connect.data.Schema;

import io.debezium.connector.jdbc.type.AbstractType;
import io.debezium.connector.jdbc.type.JdbcType;
import io.debezium.data.Json;
import io.debezium.sink.column.ColumnDescriptor;

/**
 * An implementation of {@link JdbcType} for {@link Json} types for MariaDB.
 *
 * MariaDB's JSON type is an alias for LONGTEXT and does not support
 * {@code CAST(? AS json)} syntax in parameterized queries like MySQL does.
 *
 * @see <a href="https://mariadb.com/kb/en/json-data-type/">MariaDB JSON Data Type</a>
 */
public class JsonType extends AbstractType {

    public static final JsonType INSTANCE = new JsonType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ Json.LOGICAL_NAME };
    }

    @Override
    public String getQueryBinding(ColumnDescriptor column, Schema schema, Object value) {
        // MariaDB doesn't support CAST(? AS json) for parameterized queries.
        // Since JSON is LONGTEXT in MariaDB, we bind the value directly.
        return "?";
    }

    @Override
    public String getDefaultValueBinding(Schema schema, Object value) {
        // No default value is permitted
        return null;
    }

    @Override
    public String getTypeName(Schema schema, boolean isKey) {
        // MariaDB JSON is actually LONGTEXT
        return "longtext";
    }
}
