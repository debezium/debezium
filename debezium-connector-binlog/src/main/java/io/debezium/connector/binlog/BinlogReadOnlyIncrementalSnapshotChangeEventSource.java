/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.binlog.gtid.GtidSet;
import io.debezium.connector.binlog.gtid.GtidSetFactory;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.signal.SignalPayload;
import io.debezium.pipeline.signal.actions.snapshotting.SnapshotConfiguration;
import io.debezium.pipeline.signal.channels.KafkaSignalChannel;
import io.debezium.pipeline.source.snapshot.incremental.AbstractIncrementalSnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.DataChangeEventListener;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.TableId;
import io.debezium.schema.DatabaseSchema;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.util.Clock;

/**
 * @author Chris Cranford
 */
public abstract class BinlogReadOnlyIncrementalSnapshotChangeEventSource<P extends BinlogPartition, O extends BinlogOffsetContext>
        extends AbstractIncrementalSnapshotChangeEventSource<P, TableId> {

    private static final Logger LOGGER = LoggerFactory.getLogger(BinlogReadOnlyIncrementalSnapshotChangeEventSource.class);
    private static final String SHOW_MASTER_STMT = "SHOW MASTER STATUS";

    private final GtidSetFactory gtidSetFactory;

    public BinlogReadOnlyIncrementalSnapshotChangeEventSource(BinlogConnectorConfig connectorConfig,
                                                              JdbcConnection jdbcConnection,
                                                              EventDispatcher<P, TableId> dispatcher,
                                                              DatabaseSchema<?> databaseSchema,
                                                              Clock clock,
                                                              SnapshotProgressListener<P> progressListener,
                                                              DataChangeEventListener<P> dataChangeEventListener,
                                                              NotificationService<P, O> notificationService) {
        super(connectorConfig, jdbcConnection, dispatcher, databaseSchema, clock, progressListener, dataChangeEventListener, notificationService);
        this.gtidSetFactory = connectorConfig.getGtidSetFactory();
    }

    @Override
    public void processMessage(P partition, DataCollectionId dataCollectionId, Object key, OffsetContext offsetContext)
            throws InterruptedException {
        if (getContext() == null) {
            LOGGER.warn("Context is null, skipping message processing");
            return;
        }
        LOGGER.trace("Checking window for table '{}', key '{}', window contains '{}'", dataCollectionId, key, window);
        boolean windowClosed = getContext().updateWindowState(offsetContext);
        if (windowClosed) {
            sendWindowEvents(partition, offsetContext);
            readChunk(partition, offsetContext);
        }
        else if (!window.isEmpty() && getContext().deduplicationNeeded()) {
            deduplicateWindow(dataCollectionId, key);
        }
    }

    @Override
    public void processHeartbeat(P partition, OffsetContext offsetContext) throws InterruptedException {
        if (getContext() == null) {
            LOGGER.warn("Context is null, skipping message processing");
            return;
        }
        readUntilGtidChange(partition, offsetContext);
    }

    @Override
    public void processFilteredEvent(P partition, OffsetContext offsetContext) throws InterruptedException {
        if (getContext() == null) {
            LOGGER.warn("Context is null, skipping message processing");
            return;
        }
        boolean windowClosed = getContext().updateWindowState(offsetContext);
        if (windowClosed) {
            sendWindowEvents(partition, offsetContext);
            readChunk(partition, offsetContext);
        }
    }

    @Override
    public void processTransactionStartedEvent(P partition, OffsetContext offsetContext) throws InterruptedException {
        if (getContext() == null) {
            LOGGER.warn("Context is null, skipping message processing");
            return;
        }
        boolean windowClosed = getContext().updateWindowState(offsetContext);
        if (windowClosed) {
            sendWindowEvents(partition, offsetContext);
            readChunk(partition, offsetContext);
        }
    }

    @Override
    public void processTransactionCommittedEvent(P partition, OffsetContext offsetContext) throws InterruptedException {
        if (getContext() == null) {
            LOGGER.warn("Context is null, skipping message processing");
            return;
        }
        readUntilGtidChange(partition, offsetContext);
    }

    @Override
    protected void emitWindowOpen() {
        updateLowWatermark();
    }

    @Override
    protected void emitWindowClose(P partition, OffsetContext offsetContext) throws Exception {
        updateHighWatermark();
        if (getContext().hasServerIdentifierChanged()) {
            rereadChunk(partition, offsetContext);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void sendEvent(P partition, EventDispatcher<P, TableId> dispatcher, OffsetContext offsetContext, Object[] row)
            throws InterruptedException {
        BinlogSourceInfo sourceInfo = ((O) offsetContext).getSource();
        String query = sourceInfo.getQuery();
        sourceInfo.setQuery(null);
        super.sendEvent(partition, dispatcher, offsetContext, row);
        sourceInfo.setQuery(query);
    }

    @Override
    public void addDataCollectionNamesToSnapshot(SignalPayload<P> signalPayload, SnapshotConfiguration snapshotConfiguration)
            throws InterruptedException {
        final Map<String, Object> additionalData = signalPayload.additionalData;
        super.addDataCollectionNamesToSnapshot(signalPayload, snapshotConfiguration);
        getContext().setSignalOffset((Long) additionalData.get(KafkaSignalChannel.CHANNEL_OFFSET));
    }

    @Override
    public void requestStopSnapshot(P partition, OffsetContext offsetContext, Map<String, Object> additionalData, List<String> dataCollectionIds) {
        super.requestStopSnapshot(partition, offsetContext, additionalData, dataCollectionIds);
        getContext().setSignalOffset((Long) additionalData.get(KafkaSignalChannel.CHANNEL_OFFSET));
    }

    private BinlogReadOnlyIncrementalSnapshotContext<TableId> getContext() {
        return (BinlogReadOnlyIncrementalSnapshotContext<TableId>) context;
    }

    private void readUntilGtidChange(P partition, OffsetContext offsetContext) throws InterruptedException {
        String currentGtid = getContext().getCurrentGtid(offsetContext);
        while (getContext().snapshotRunning() && getContext().reachedHighWatermark(currentGtid)) {
            getContext().closeWindow();
            sendWindowEvents(partition, offsetContext);
            readChunk(partition, offsetContext);
            if (currentGtid == null && getContext().watermarksChanged()) {
                return;
            }
        }
    }

    private void updateLowWatermark() {
        getExecutedGtidSet(getContext()::setLowWatermark);
    }

    private void updateHighWatermark() {
        getExecutedGtidSet(getContext()::setHighWatermark);
    }

    protected abstract void getExecutedGtidSet(Consumer<GtidSet> watermark);
}
