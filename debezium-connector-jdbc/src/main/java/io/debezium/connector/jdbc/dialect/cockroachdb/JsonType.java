/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.cockroachdb;

import org.apache.kafka.connect.data.Schema;

import io.debezium.connector.jdbc.type.AbstractType;
import io.debezium.connector.jdbc.type.JdbcType;
import io.debezium.data.Json;
import io.debezium.sink.column.ColumnDescriptor;

/**
 * An implementation of {@link JdbcType} for {@code io.debezium.data.Json} types. CockroachDB's
 * JSON type is an alias of JSONB, and there is no hstore type, so all JSON-based logical values,
 * including propagated HSTORE columns, are stored as {@code jsonb} JSON documents.
 *
 * @author Virag Tripathi
 */
class JsonType extends AbstractType {

    public static final JsonType INSTANCE = new JsonType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ Json.LOGICAL_NAME };
    }

    @Override
    public String getQueryBinding(ColumnDescriptor column, Schema schema, Object value) {
        return "cast(? as jsonb)";
    }

    @Override
    public String getTypeName(Schema schema, boolean isKey) {
        return "jsonb";
    }

}
