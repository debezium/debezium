/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.postgres;

import java.util.List;

import org.apache.kafka.connect.data.Schema;

import io.debezium.connector.jdbc.type.AbstractType;
import io.debezium.connector.jdbc.type.JdbcType;
import io.debezium.data.Json;
import io.debezium.sink.column.ColumnDescriptor;
import io.debezium.sink.valuebinding.ValueBindDescriptor;

/**
 * An implementation of {@link JdbcType} for {@link Json} types.
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
    public String getQueryBinding(ColumnDescriptor column, Schema schema, Object value) {
        if (isHstore(schema)) {
            return "cast(? as hstore)";
            // return super.getQueryBinding(schema);
        }
        return String.format("cast(? as %s)", isJsonb(schema) ? "jsonb" : "json");
    }

    @Override
    public String getTypeName(Schema schema, boolean isKey) {
        return resolveType(schema);
    }

    @Override
    public List<ValueBindDescriptor> bind(int index, Schema schema, Object value) {
        if (isHstore(schema)) {
            value = HstoreConverter.jsonToString((String) value);
        }
        return super.bind(index, schema, value);
    }

    private String resolveType(Schema schema) {
        return isHstore(schema) ? "hstore" : isJsonb(schema) ? "jsonb" : "json";
    }

    private boolean isJsonb(Schema schema) {
        // Unless column type propagation is enabled; Debezium emits JSON and JSONB data as the Json
        // logical type and there is no differentiation that can be made to determine if the source
        // was JSONB; therefore column type propagation must be enabled for this to be possible.
        return "JSONB".equals(getSourceColumnType(schema).orElse("JSON"));
    }

    private boolean isHstore(Schema schema) {
        // Debezium emits HSTORE data as Json logical types.
        return "HSTORE".equals(getSourceColumnType(schema).orElse("JSON"));
    }

}
