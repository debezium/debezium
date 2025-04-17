/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot.incremental;

import static io.debezium.config.CommonConnectorConfig.WatermarkStrategy.INSERT_DELETE;
import static io.debezium.util.Loggings.maybeRedactSensitiveData;

import java.sql.SQLException;
import java.time.Instant;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.NotThreadSafe;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.signal.actions.snapshotting.OpenIncrementalSnapshotWindow;
import io.debezium.pipeline.source.spi.DataChangeEventListener;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.schema.DatabaseSchema;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.util.Clock;

@NotThreadSafe
public class SignalBasedIncrementalSnapshotChangeEventSource<P extends Partition, T extends DataCollectionId>
        extends AbstractIncrementalSnapshotChangeEventSource<P, T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SignalBasedIncrementalSnapshotChangeEventSource.class);
    private final String signalWindowStatement;
    private SignalMetadata signalMetadata;

    public SignalBasedIncrementalSnapshotChangeEventSource(RelationalDatabaseConnectorConfig config,
                                                           JdbcConnection jdbcConnection,
                                                           EventDispatcher<P, T> dispatcher, DatabaseSchema<?> databaseSchema,
                                                           Clock clock,
                                                           SnapshotProgressListener<P> progressListener,
                                                           DataChangeEventListener<P> dataChangeEventListener,
                                                           NotificationService<P, ? extends OffsetContext> notificationService) {
        super(config, jdbcConnection, dispatcher, databaseSchema, clock, progressListener, dataChangeEventListener, notificationService);
        signalWindowStatement = "INSERT INTO " + getSignalTableName(config.getSignalingDataCollectionId())
                + " VALUES (?, ?, ?)";
    }

    @Override
    @SuppressWarnings("unchecked")
    public void processMessage(Partition partition, DataCollectionId dataCollectionId, Object key, OffsetContext offsetContext) {
        context = (IncrementalSnapshotContext<T>) offsetContext.getIncrementalSnapshotContext();
        if (context == null) {
            LOGGER.warn("Context is null, skipping message processing");
            return;
        }
        LOGGER.trace("Checking window for table '{}', key '{}', window contains '{}'", dataCollectionId, maybeRedactSensitiveData(key), window);
        if (!window.isEmpty() && context.deduplicationNeeded()) {
            deduplicateWindow(dataCollectionId, key);
        }
    }

    @Override
    protected void emitWindowOpen() throws SQLException {
        signalMetadata = new SignalMetadata(Instant.now(), null);
        jdbcConnection.prepareUpdate(signalWindowStatement, x -> {
            LOGGER.trace("Emitting open window for chunk = '{}'", context.currentChunkId());
            x.setString(1, context.currentChunkId() + "-open");
            x.setString(2, OpenIncrementalSnapshotWindow.NAME);
            x.setString(3, signalMetadata.metadataString());
        });
        jdbcConnection.commit();
    }

    @Override
    protected void emitWindowClose(Partition partition, OffsetContext offsetContext) throws Exception {

        String signalTableName = getSignalTableName(connectorConfig.getSignalingDataCollectionId());

        WatermarkWindowCloser watermarkWindowCloser = getWatermarkWindowCloser(connectorConfig, jdbcConnection, signalTableName);

        watermarkWindowCloser.closeWindow(partition, offsetContext, context.currentChunkId());
    }

    private WatermarkWindowCloser getWatermarkWindowCloser(CommonConnectorConfig connectorConfig, JdbcConnection jdbcConnection, String signalTable) {

        if (Objects.requireNonNull(connectorConfig.getIncrementalSnapshotWatermarkingStrategy()) == INSERT_DELETE) {
            return new DeleteWindowCloser<>(jdbcConnection, signalTable, this);
        }

        return new InsertWindowCloser(jdbcConnection, signalTable, new SignalMetadata(signalMetadata.getOpenWindowTimestamp(), Instant.now()));
    }
}
