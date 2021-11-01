/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import java.sql.SQLException;

import io.debezium.DebeziumException;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotContext;
import io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.DataChangeEventListener;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.TableId;
import io.debezium.schema.DatabaseSchema;
import io.debezium.util.Clock;

/**
 * @author Chris Cranford
 */
public class OracleSignalBasedIncrementalSnapshotChangeEventSource extends SignalBasedIncrementalSnapshotChangeEventSource<TableId> {

    private final String pdbName;
    private final OracleConnection connection;

    public OracleSignalBasedIncrementalSnapshotChangeEventSource(RelationalDatabaseConnectorConfig config,
                                                                 JdbcConnection jdbcConnection,
                                                                 EventDispatcher<TableId> dispatcher,
                                                                 DatabaseSchema<?> databaseSchema,
                                                                 Clock clock,
                                                                 SnapshotProgressListener progressListener,
                                                                 DataChangeEventListener dataChangeEventListener) {
        super(config, jdbcConnection, dispatcher, databaseSchema, clock, progressListener, dataChangeEventListener);
        this.pdbName = ((OracleConnectorConfig) config).getPdbName();
        this.connection = (OracleConnection) jdbcConnection;
    }

    @Override
    protected String getSignalTableName(String dataCollectionId) {
        final TableId tableId = TableId.parse(dataCollectionId);
        return tableId.schema() + "." + tableId.table();
    }

    @Override
    protected void preReadChunk(IncrementalSnapshotContext<TableId> context) {
        super.preReadChunk(context);

        if (pdbName != null) {
            connection.setSessionToPdb(pdbName);
        }
    }

    @Override
    protected void postReadChunk(IncrementalSnapshotContext<TableId> context) {
        super.postReadChunk(context);

        if (pdbName != null) {
            connection.resetSessionToCdb();
        }
    }

    @Override
    protected void postIncrementalSnapshotCompleted() {
        super.postIncrementalSnapshotCompleted();

        try {
            connection.close();
        }
        catch (SQLException e) {
            throw new DebeziumException("Failed to close snapshot connection", e);
        }
    }
}
