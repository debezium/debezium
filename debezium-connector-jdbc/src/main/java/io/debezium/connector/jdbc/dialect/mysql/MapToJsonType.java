/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.mysql;

import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.hibernate.query.Query;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.type.Type;
import io.debezium.connector.jdbc.type.connect.AbstractConnectMapType;

/**
 * An implementation of {@link Type} for {@code MAP} schema types that are mapped to
 * a {@code JSON} column type.
 *
 * @author Chris Cranford
 */
class MapToJsonType extends AbstractConnectMapType {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static final MapToJsonType INSTANCE = new MapToJsonType();

    @Override
    public String getQueryBinding(Schema schema) {
        return JsonType.INSTANCE.getQueryBinding(schema);
    }

    @Override
    public String getTypeName(DatabaseDialect dialect, Schema schema, boolean key) {
        return JsonType.INSTANCE.getTypeName(dialect, schema, key);
    }

    @Override
    public void bind(Query<?> query, int index, Schema schema, Object value) {
        if (value instanceof Map) {
            try {
                value = OBJECT_MAPPER.writeValueAsString(value);
            }
            catch (JsonProcessingException e) {
                throw new ConnectException("Failed to deserialize MAP data to JSON", e);
            }
        }
        JsonType.INSTANCE.bind(query, index, schema, value);
    }

}
