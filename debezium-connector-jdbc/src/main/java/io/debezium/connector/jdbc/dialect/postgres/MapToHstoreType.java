/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.postgres;

import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.hibernate.query.Query;

import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.relational.ColumnDescriptor;
import io.debezium.connector.jdbc.type.Type;
import io.debezium.connector.jdbc.type.connect.AbstractConnectMapType;

/**
 * An implementation of {@link Type} for {@code MAP} schema types that get mapped to
 * a PostgreSQL {@code HSTORE} column type.
 *
 * @author Chris Cranford
 */
class MapToHstoreType extends AbstractConnectMapType {

    public static final MapToHstoreType INSTANCE = new MapToHstoreType();

    @Override
    public String getQueryBinding(ColumnDescriptor column, Schema schema) {
        return "cast(? as hstore)";
    }

    @Override
    public String getTypeName(DatabaseDialect dialect, Schema schema, boolean key) {
        // This type explicitly maps the MAP schema type to HSTORE
        return "hstore";
    }

    @Override
    @SuppressWarnings("unchecked")
    public void bind(Query<?> query, int index, Schema schema, Object value) {
        super.bind(query, index, schema, HstoreConverter.mapToString((Map<String, String>) value));
    }

}
