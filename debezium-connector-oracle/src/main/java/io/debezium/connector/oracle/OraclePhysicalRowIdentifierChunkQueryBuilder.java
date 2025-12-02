/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import java.sql.Types;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.source.snapshot.incremental.PhysicalRowIdentifierChunkQueryBuilder;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.spi.schema.DataCollectionId;

import oracle.jdbc.OracleTypes;

/**
 * Oracle implementation that exposes {@code ROWID} as a physical row identifier for incremental snapshots.
 */
public class OraclePhysicalRowIdentifierChunkQueryBuilder<T extends DataCollectionId>
        extends PhysicalRowIdentifierChunkQueryBuilder<T> {

    private static final String ROWID = "ROWID";
    private static final String ROWID_TABLE_ALIAS = "DBZ_ROWID_ALIAS";

    public OraclePhysicalRowIdentifierChunkQueryBuilder(RelationalDatabaseConnectorConfig config,
                                                        JdbcConnection jdbcConnection) {
        super(config,
                jdbcConnection,
                ROWID,
                ROWID,
                Types.ROWID,
                OracleTypes.ROWID,
                ROWID,
                null,
                null,
                true,
                ROWID_TABLE_ALIAS);
    }
}
