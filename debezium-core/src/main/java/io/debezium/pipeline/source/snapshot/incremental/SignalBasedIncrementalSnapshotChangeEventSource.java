/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot.incremental;

import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.NotThreadSafe;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.DataChangeEventListener;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.schema.DataCollectionId;
import io.debezium.schema.DatabaseSchema;
import io.debezium.util.Clock;

@NotThreadSafe
public class SignalBasedIncrementalSnapshotChangeEventSource<T extends DataCollectionId> extends AbstractIncrementalSnapshotChangeEventSource<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SignalBasedIncrementalSnapshotChangeEventSource.class);
    private final String signalWindowStatement;

    public SignalBasedIncrementalSnapshotChangeEventSource(RelationalDatabaseConnectorConfig config,
                                                           JdbcConnection jdbcConnection,
                                                           EventDispatcher<T> dispatcher, DatabaseSchema<?> databaseSchema,
                                                           Clock clock,
                                                           SnapshotProgressListener progressListener,
                                                           DataChangeEventListener dataChangeEventListener) {
        super(config, jdbcConnection, dispatcher, databaseSchema, clock, progressListener, dataChangeEventListener);
        signalWindowStatement = "INSERT INTO " + getSignalTableName(config.getSignalingDataCollectionId())
                + " VALUES (?, ?, null)";
    }

    @Override
    @SuppressWarnings("unchecked")
    public void processMessage(Partition partition, DataCollectionId dataCollectionId, Object key, OffsetContext offsetContext) {
        context = (IncrementalSnapshotContext<T>) offsetContext.getIncrementalSnapshotContext();
        if (context == null) {
            LOGGER.warn("Context is null, skipping message processing");
            return;
        }
        LOGGER.trace("Checking window for table '{}', key '{}', window contains '{}'", dataCollectionId, key, window);
        if (!window.isEmpty() && context.deduplicationNeeded()) {
            deduplicateWindow(dataCollectionId, key);
        }
    }

    @Override
    protected void emitWindowOpen() throws SQLException {
        jdbcConnection.prepareUpdate(signalWindowStatement, x -> {
            LOGGER.trace("Emitting open window for chunk = '{}'", context.currentChunkId());
            x.setString(1, context.currentChunkId() + "-open");
            x.setString(2, OpenIncrementalSnapshotWindow.NAME);
        });
        jdbcConnection.commit();
    }

    @Override
    protected void emitWindowClose() throws SQLException {
        jdbcConnection.prepareUpdate(signalWindowStatement, x -> {
            LOGGER.trace("Emitting close window for chunk = '{}'", context.currentChunkId());
            x.setString(1, context.currentChunkId() + "-close");
            x.setString(2, CloseIncrementalSnapshotWindow.NAME);
        });
        jdbcConnection.commit();
    }
}
