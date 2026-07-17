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
import java.util.Map;
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
import io.debezium.util.DelayStrategy;
import io.debezium.util.LoggingContext;
import io.debezium.util.RetryingRunnable;

@NotThreadSafe
public class SignalBasedIncrementalSnapshotChangeEventSource<P extends Partition, T extends DataCollectionId>
        extends AbstractIncrementalSnapshotChangeEventSource<P, T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SignalBasedIncrementalSnapshotChangeEventSource.class);
    private SignalMetadata signalMetadata;

    public SignalBasedIncrementalSnapshotChangeEventSource(RelationalDatabaseConnectorConfig config,
                                                           JdbcConnection jdbcConnection,
                                                           EventDispatcher<P, T> dispatcher, DatabaseSchema<?> databaseSchema,
                                                           Clock clock,
                                                           SnapshotProgressListener<P> progressListener,
                                                           DataChangeEventListener<P> dataChangeEventListener,
                                                           NotificationService<P, ? extends OffsetContext> notificationService) {
        super(config, jdbcConnection, dispatcher, databaseSchema, clock, progressListener, dataChangeEventListener, notificationService);
    }

    /**
     * Get the appropriate signal table name for the given partition.
     * For multi-database connectors with multiple signal tables, this method matches the signal table
     * to the database of the partition.
     *
     * @param partition the partition for which to get the signal table
     * @return the signal table name, or null if not configured
     */
    protected String getSignalTableNameForPartition(Partition partition) {

        if (connectorConfig.getSignalingDataCollectionIds().size() == 1) {
            return getSignalTableName(connectorConfig.getSignalingDataCollectionIds().get(0));
        }

        String partitionDatabase = getDatabaseNameFromPartition(partition);
        if (partitionDatabase == null) {
            return null;
        }

        return connectorConfig.getSignalingDataCollectionIds().stream()
                .filter(signalCollection -> partitionDatabase.equals(getDatabaseName(signalCollection).get()))
                .map(this::getSignalTableName)
                .findFirst()
                .orElse(null);
    }

    /**
     * Extract the database name from a partition.
     * Default implementation tries to use AbstractPartition's databaseName field via reflection.
     * Subclasses should override this for connector-specific partition types.
     *
     * @param partition the partition
     * @return the database name, or null if it cannot be determined
     */
    protected String getDatabaseNameFromPartition(Partition partition) {
        // Default implementation: try to extract from relational AbstractPartition
        if (partition instanceof io.debezium.relational.AbstractPartition) {
            // Access the protected field via the source partition
            Map<String, String> loggingContext = partition.getLoggingContext();
            return loggingContext.get(LoggingContext.DATABASE_NAME);
        }
        return null;
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
    protected void emitWindowOpen(P partition, OffsetContext offsetContext) throws SQLException {
        String signalTableName = getSignalTableNameForPartition(partition);
        if (signalTableName == null) {
            LOGGER.warn("Not able to determine signal table, cannot emit window open signal");
            return;
        }

        RetryingRunnable.<SQLException> builder()
                .retries(connectorConfig.getSignalEmitFailureMaxRetries())
                .delayStrategy(DelayStrategy.constant(connectorConfig.getSignalEmitFailureBackoff()))
                .doRun(() -> insertOpenWindow(signalTableName))
                .doAutoHeal(this::autoHealJdbcConnection)
                .build()
                .runWrapped(cause -> new SQLException("Interrupted", cause));
    }

    @Override
    protected void emitWindowClose(P partition, OffsetContext offsetContext) throws Exception {
        String signalTableName = getSignalTableNameForPartition(partition);
        if (signalTableName == null) {
            LOGGER.warn("Not able to detect signal table, cannot emit window close signal");
            return;
        }

        LOGGER.trace("Emitting close window for chunk = '{}' to signal table '{}'", context.currentChunkId(), signalTableName);
        WatermarkWindowCloser watermarkWindowCloser = getWatermarkWindowCloser(connectorConfig, jdbcConnection, signalTableName);

        RetryingRunnable.builder()
                .retries(connectorConfig.getSignalEmitFailureMaxRetries())
                .delayStrategy(DelayStrategy.constant(connectorConfig.getSignalEmitFailureBackoff()))
                .doRun(() -> watermarkWindowCloser.closeWindow(partition, offsetContext, context.currentChunkId()))
                .doAutoHeal(this::autoHealJdbcConnection)
                .build()
                .run();
    }

    private void insertOpenWindow(String signalTableName) throws SQLException {
        String signalWindowStatement = "INSERT INTO " + signalTableName + " VALUES (?, ?, ?)";
        // Attribution is captured here and reused for the close, because the data collection queue
        // may already have advanced to the next collection by the time the close is emitted.
        final DataCollection<T> currentDataCollection = context.currentDataCollectionId();
        String correlationId = null;
        String dataCollectionId = null;
        if (currentDataCollection != null) {
            correlationId = currentDataCollection.getCorrelationId().orElse(null);
            dataCollectionId = currentDataCollection.getId().identifier();
        }
        signalMetadata = new SignalMetadata(Instant.now(), null, correlationId, dataCollectionId);
        jdbcConnection.prepareUpdate(signalWindowStatement, x -> {
            LOGGER.trace("Emitting open window for chunk = '{}' to signal table '{}'", context.currentChunkId(), signalTableName);
            x.setString(1, context.currentChunkId() + "-open");
            x.setString(2, OpenIncrementalSnapshotWindow.NAME);
            x.setString(3, signalMetadata.metadataString());
        });
        jdbcConnection.commit();
    }

    private void autoHealJdbcConnection() throws SQLException {
        LOGGER.info("Closing JDBC connection and re-connecting");
        try {
            jdbcConnection.close();
        }
        catch (Exception ignore) {
            // Log and ignore
            LOGGER.info("Failed to close JDBC connection during auto heal. Ignoring.", ignore);
        }
        LOGGER.trace("Reconnecting JDBC connection");
        jdbcConnection.connect();
    }

    private WatermarkWindowCloser getWatermarkWindowCloser(CommonConnectorConfig connectorConfig, JdbcConnection jdbcConnection, String signalTable) {

        if (Objects.requireNonNull(connectorConfig.getIncrementalSnapshotWatermarkingStrategy()) == INSERT_DELETE) {
            return new DeleteWindowCloser<>(jdbcConnection, signalTable, this);
        }

        return new InsertWindowCloser(jdbcConnection, signalTable, signalMetadata.withCloseWindowTimestamp(Instant.now()));
    }
}
