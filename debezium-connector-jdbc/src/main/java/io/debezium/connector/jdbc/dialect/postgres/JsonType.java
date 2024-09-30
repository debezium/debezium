/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.postgres;

import org.apache.kafka.connect.data.Schema;
import org.hibernate.query.Query;

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
        if (isHstore(schema)) {
            return "cast(? as hstore)";
            // return super.getQueryBinding(schema);
        }
        return String.format("cast(? as %s)", isJsonb(schema) ? "jsonb" : "json");
    }

    @Override
    public String getTypeName(DatabaseDialect dialect, Schema schema, boolean key) {
        return resolveType(schema);
    }

    @Override
    public int bind(Query<?> query, int index, Schema schema, Object value) {
        if (isHstore(schema)) {
            value = HstoreConverter.jsonToString((String) value);
        }
        return super.bind(query, index, schema, value);
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
