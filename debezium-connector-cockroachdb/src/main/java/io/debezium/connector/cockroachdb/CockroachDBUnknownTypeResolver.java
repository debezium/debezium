/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0,
 * available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.cockroachdb;

import java.sql.Types;

import org.apache.kafka.connect.data.SchemaBuilder;

import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.ValueConverterProvider;
import io.debezium.relational.spi.Column;

/**
 * Fallback handler for unknown or unmapped CockroachDB column types.
 * Ensures all types are assigned a default converter and schema,
 * to avoid runtime failures when encountering uncommon SQL types.
 *
 * @author Virag Tripathi
 */
public class CockroachDBUnknownTypeResolver implements ValueConverterProvider.UnknownTypeResolver {

    private final CockroachDBValueConverter converters;

    public CockroachDBUnknownTypeResolver(RelationalDatabaseConnectorConfig config) {
        this.converters = new CockroachDBValueConverter(config);
    }

    @Override
    public SchemaBuilder schemaBuilder(Column column) {
        return converters.schemaBuilder(Types.OTHER);
    }

    @Override
    public Object convert(Column column, Object value) {
        return converters.converter(Types.OTHER, column.name(), column.typeName()).convert(value);
    }
}
