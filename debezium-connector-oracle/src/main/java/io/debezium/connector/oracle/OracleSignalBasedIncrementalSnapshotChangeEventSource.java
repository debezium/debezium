/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import java.sql.SQLException;
import java.util.List;

import io.debezium.DebeziumException;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
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
public class OracleSignalBasedIncrementalSnapshotChangeEventSource extends SignalBasedIncrementalSnapshotChangeEventSource<OraclePartition, TableId> {

    private final List<String> pdbNames;
    private final OracleConnection connection;

    public OracleSignalBasedIncrementalSnapshotChangeEventSource(RelationalDatabaseConnectorConfig config,
                                                                 JdbcConnection jdbcConnection,
                                                                 EventDispatcher<OraclePartition, TableId> dispatcher,
                                                                 DatabaseSchema<?> databaseSchema,
                                                                 Clock clock,
                                                                 SnapshotProgressListener<OraclePartition> progressListener,
                                                                 DataChangeEventListener<OraclePartition> dataChangeEventListener,
                                                                 NotificationService<OraclePartition, OracleOffsetContext> notificationService) {
        super(config, jdbcConnection, dispatcher, databaseSchema, clock, progressListener, dataChangeEventListener, notificationService);
        this.pdbNames = ((OracleConnectorConfig) config).getPdbNames();
        this.connection = (OracleConnection) jdbcConnection;
    }

    @Override
    protected String getSignalTableName(String dataCollectionId) {
        final TableId tableId = OracleTableIdParser.parse(dataCollectionId);
        return OracleTableIdParser.quoteIfNeeded(tableId, false, true, connection.getSQLKeywords());
    }

    @Override
    protected void preReadChunk(IncrementalSnapshotContext<TableId> context) {
        super.preReadChunk(context);

        if (!pdbNames.isEmpty()) {
            connection.setSessionToPdb(resolvePdbName(context));
        }
    }

    @Override
    protected void postReadChunk(IncrementalSnapshotContext<TableId> context) {
        super.postReadChunk(context);

        if (!pdbNames.isEmpty()) {
            connection.resetSessionToCdb();
        }
    }

    /**
     * Resolves the pluggable database that should be current for the chunk read, preferring the
     * catalog of the current data collection when multiple pluggable databases are configured.
     *
     * @param context the incremental snapshot context, should not be {@code null}
     * @return the pluggable database name, never {@code null}
     */
    private String resolvePdbName(IncrementalSnapshotContext<TableId> context) {
        if (pdbNames.size() > 1 && context.currentDataCollectionId() != null) {
            final TableId tableId = context.currentDataCollectionId().getId();
            if (tableId.catalog() != null && pdbNames.contains(tableId.catalog())) {
                return tableId.catalog();
            }
        }
        return pdbNames.get(0);
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

    @Override
    protected String getTableDDL(TableId dataCollectionId) throws SQLException {
        return connection.getTableMetadataDdl(dataCollectionId);
    }
}
