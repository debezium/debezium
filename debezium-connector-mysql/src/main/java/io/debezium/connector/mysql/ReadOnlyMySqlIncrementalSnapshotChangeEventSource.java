/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.sql.SQLException;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.snapshot.incremental.AbstractIncrementalSnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.DataChangeEventListener;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.schema.DataCollectionId;
import io.debezium.schema.DatabaseSchema;
import io.debezium.util.Clock;

/**
 * A MySQL specific read-only incremental snapshot change event source.
 * Uses executed GTID set as low/high watermarks for incremental snapshot window to support read-only connection.
 */
public class ReadOnlyMySqlIncrementalSnapshotChangeEventSource<T extends DataCollectionId> extends AbstractIncrementalSnapshotChangeEventSource<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReadOnlyMySqlIncrementalSnapshotChangeEventSource.class);
    private final String showMasterStmt = "SHOW MASTER STATUS";
    protected ReadOnlyMySqlIncrementalSnapshotContext<T> context = null;

    public ReadOnlyMySqlIncrementalSnapshotChangeEventSource(CommonConnectorConfig config, JdbcConnection jdbcConnection,
                                                             EventDispatcher<T> dispatcher,
                                                             DatabaseSchema<?> databaseSchema, Clock clock,
                                                             SnapshotProgressListener progressListener,
                                                             DataChangeEventListener dataChangeEventListener) {
        super(config, jdbcConnection, dispatcher, databaseSchema, clock, progressListener, dataChangeEventListener);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void processMessage(DataCollectionId dataCollectionId, Object key, OffsetContext offsetContext) throws InterruptedException {
        context = (ReadOnlyMySqlIncrementalSnapshotContext<T>) offsetContext.getIncrementalSnapshotContext();
        if (context == null) {
            LOGGER.warn("Context is null, skipping message processing");
            return;
        }
        LOGGER.trace("Checking window for table '{}', key '{}', window contains '{}'", dataCollectionId, key, window);
        boolean windowClosed = context.updateWindowState(offsetContext);
        if (!window.isEmpty() && context.deduplicationNeeded()) {
            deduplicateWindow(dataCollectionId, key);
        }
        if (windowClosed) {
            sendWindowEvents(offsetContext);
            readChunk();
        }
    }

    protected void updateLowWatermark() {
        ReadOnlyMySqlIncrementalSnapshotContext<T> mySqlContext = context;
        getExecutedGtidSet(mySqlContext::setLowWatermark);
    }

    protected void updateHighWatermark() {
        ReadOnlyMySqlIncrementalSnapshotContext<T> mySqlContext = context;
        getExecutedGtidSet(mySqlContext::setHighWatermark);
    }

    private void getExecutedGtidSet(Consumer<GtidSet> watermark) {
        try {
            jdbcConnection.query(showMasterStmt, rs -> {
                if (rs.next()) {
                    if (rs.getMetaData().getColumnCount() > 4) {
                        // This column exists only in MySQL 5.6.5 or later ...
                        final String gtidSet = rs.getString(5); // GTID set, may be null, blank, or contain a GTID set
                        watermark.accept(new GtidSet(gtidSet));
                    }
                    else {
                        throw new UnsupportedOperationException("Need to add support for executed GTIDs for versions prior to 5.6.5");
                    }
                }
            });
        }
        catch (SQLException e) {
            throw new DebeziumException(e);
        }
    }

    @Override
    protected void emitWindowOpen() {
        updateLowWatermark();
    }

    @Override
    protected void emitWindowClose() {
        updateHighWatermark();
    }
}
