/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.source.snapshot.incremental.RowValueConstructorChunkQueryBuilder;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.Table;
import io.debezium.spi.schema.DataCollectionId;

/**
 * PostgreSQL chunk query builder that consults {@link PostgresSchema}'s generated-column side map.
 *
 * <p>For pgoutput, {@link PostgresSchema#refreshFromIncrementalSnapshot} prunes generated columns from
 * the in-memory {@link Table}. Without the side map, {@code buildProjection} would see no generated
 * columns and fall back to {@code SELECT *}, which returns those columns from the database and fails
 * with "Column 'X' not found in result set" (DBZ-2020).</p>
 *
 * @param <T> data collection id type
 */
public class PostgresChunkQueryBuilder<T extends DataCollectionId> extends RowValueConstructorChunkQueryBuilder<T> {

    private final PostgresSchema schema;

    public PostgresChunkQueryBuilder(RelationalDatabaseConnectorConfig config,
                                     JdbcConnection jdbcConnection,
                                     PostgresSchema schema) {
        super(config, jdbcConnection);
        this.schema = schema;
    }

    @Override
    protected boolean hasAdditionalGeneratedColumns(Table table) {
        return !schema.getGeneratedColumnsForTableId(table.id()).isEmpty();
    }

    @Override
    protected boolean isAdditionalGeneratedColumn(Table table, String columnName) {
        // After pruning, generated columns are absent from table.columns(), so this filter is a
        // no-op for the stream over table.columns(). Kept for symmetry with the detection hook.
        return schema.getGeneratedColumnsForTableId(table.id()).stream()
                .anyMatch(name -> name.equalsIgnoreCase(columnName));
    }
}
