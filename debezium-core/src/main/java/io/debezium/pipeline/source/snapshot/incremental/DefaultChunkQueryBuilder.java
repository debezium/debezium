/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot.incremental;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.spi.schema.DataCollectionId;

/**
 * Builds queries for reading incremental snapshot chunks from a table using ANSI standard SQL queries.
 * <p>
 * These queries are compatible with most databases, but may not be the most efficient for some databases.
 */
public class DefaultChunkQueryBuilder<T extends DataCollectionId> extends AbstractChunkQueryBuilder<T> {

    public DefaultChunkQueryBuilder(RelationalDatabaseConnectorConfig config,
                                    JdbcConnection jdbcConnection) {
        super(config, jdbcConnection);
    }
}
