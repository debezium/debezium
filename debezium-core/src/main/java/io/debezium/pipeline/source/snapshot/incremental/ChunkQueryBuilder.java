/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot.incremental;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.spi.schema.DataCollectionId;

/**
 * Builds queries for reading incremental snapshot chunks from a table.
 */
public interface ChunkQueryBuilder<T extends DataCollectionId> {

    /**
     * Builds a query for reading the next incremental snapshot chunk from a table using the chunk size configured by the connector.
     */
    String buildChunkQuery(IncrementalSnapshotContext<T> context, Table table, Optional<String> additionalCondition);

    /**
     * Builds a query for reading the next incremental snapshot chunk from a table using the specified limit.
     */
    String buildChunkQuery(IncrementalSnapshotContext<T> context, Table table, int limit, Optional<String> additionalCondition);

    /**
     * Prepares a statement for reading the next incremental snapshot chunk from a table using the SQL statement returned by buildChunkQuery.
     */
    PreparedStatement readTableChunkStatement(IncrementalSnapshotContext<T> context, Table table, String sql) throws SQLException;

    /**
     * Builds a query for reading the maximum primary key value from a table.
     */
    String buildMaxPrimaryKeyQuery(IncrementalSnapshotContext<T> context, Table table, Optional<String> additionalCondition);

    /**
     * Returns the columns that are used for paginating the incremental snapshot chunks.
     */
    List<Column> getQueryColumns(IncrementalSnapshotContext<T> context, Table table);

}
