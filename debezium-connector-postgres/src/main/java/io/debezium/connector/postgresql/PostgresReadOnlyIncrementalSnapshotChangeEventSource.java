/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import java.sql.SQLException;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.snapshot.incremental.AbstractIncrementalSnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.DataChangeEventListener;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.schema.DatabaseSchema;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.util.Clock;

public class PostgresReadOnlyIncrementalSnapshotChangeEventSource<P extends PostgresPartition>
        extends AbstractIncrementalSnapshotChangeEventSource<P, TableId> {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresReadOnlyIncrementalSnapshotChangeEventSource.class);

    private static final String FORCE_NEW_TRANSACTION = "SELECT * FROM pg_current_xact_id();";
    private static final String CURRENT_SNAPSHOT = "SELECT * FROM pg_current_snapshot();";

    private final PostgresConnection jdbcConnection;
    private final PostgresSchema schema;

    public PostgresReadOnlyIncrementalSnapshotChangeEventSource(RelationalDatabaseConnectorConfig config,
                                                                JdbcConnection jdbcConnection,
                                                                EventDispatcher<P, TableId> dispatcher,
                                                                DatabaseSchema<?> databaseSchema,
                                                                Clock clock,
                                                                SnapshotProgressListener<P> progressListener,
                                                                DataChangeEventListener<P> dataChangeEventListener,
                                                                NotificationService<P, ? extends OffsetContext> notificationService) {

        super(config, jdbcConnection, dispatcher, databaseSchema, clock, progressListener, dataChangeEventListener, notificationService);
        this.jdbcConnection = (PostgresConnection) jdbcConnection;
        this.schema = (PostgresSchema) databaseSchema;
    }

    @Override
    protected void preIncrementalSnapshotStart() {
        super.preIncrementalSnapshotStart();

        forceNewTransactionId();
    }

    private PostgresReadOnlyIncrementalSnapshotContext<TableId> getContext() {
        return (PostgresReadOnlyIncrementalSnapshotContext<TableId>) context;
    }

    @Override
    protected void emitWindowOpen() {

        getCurrentSnapshot(getContext()::setLowWatermark);
    }

    @Override
    protected void emitWindowClose(P partition, OffsetContext offsetContext) {

        getCurrentSnapshot(getContext()::setHighWatermark);
    }

    @Override
    public void processMessage(P partition, DataCollectionId dataCollectionId, Object key, OffsetContext offsetContext) throws InterruptedException {

        if (getContext() == null) {
            LOGGER.warn("Context is null, skipping message processing");
            return;
        }

        LOGGER.debug("Checking window for table '{}', key '{}', window contains '{}'", dataCollectionId, key, window);
        getContext().updateWindowState(offsetContext);

        boolean windowClosed = getContext().isWindowClosed();
        if (windowClosed) {
            sendWindowEvents(partition, offsetContext);
            readChunk(partition, offsetContext);
        }
        else if (!window.isEmpty() && getContext().deduplicationNeeded()) {
            LOGGER.trace("Deduplicating");
            deduplicateWindow(dataCollectionId, key);
        }
    }

    @Override
    public void processTransactionCommittedEvent(P partition, OffsetContext offsetContext) throws InterruptedException {

        if (getContext() == null) {
            LOGGER.warn("Context is null, skipping message processing");
            return;
        }

        LOGGER.trace("Processing transaction event");
        readUntilNewTransactionChange(partition, offsetContext);
        LOGGER.trace("Finished processing transaction event");
    }

    @Override
    public void processHeartbeat(P partition, OffsetContext offsetContext) throws InterruptedException {

        if (getContext() == null) {
            LOGGER.warn("Context is null, skipping message processing");
            return;
        }

        LOGGER.trace("Processing heartbeat event");
        readUntilNewTransactionChange(partition, offsetContext);
        LOGGER.trace("Finished processing heartbeat event");
    }

    @Override
    protected Table refreshTableSchema(Table table) throws SQLException {
        LOGGER.debug("Refreshing table '{}' schema for incremental snapshot.", table.id());
        schema.refresh(jdbcConnection, table.id(), true);
        return schema.tableFor(table.id());
    }

    private void readUntilNewTransactionChange(P partition, OffsetContext offsetContext) throws InterruptedException {

        Long eventTxId = offsetContext.getSourceInfo().getInt64(SourceInfo.TXID_KEY);

        LOGGER.debug("Event txId {}, snapshot is running {}, reachedHighWatermark {}",
                eventTxId, getContext().snapshotRunning(), getContext().isTransactionVisible(eventTxId));

        LOGGER.trace("Incremental snapshot context {}", getContext());
        if (getContext().snapshotRunning() && maxInProgressTransactionCommitted(eventTxId)) {
            getContext().closeWindow();
            sendWindowEvents(partition, offsetContext);
            readChunk(partition, offsetContext);

            return;
        }

        while (getContext().snapshotRunning() && getContext().isTransactionVisible(eventTxId)) {

            LOGGER.debug("Finishing snapshot, snapshot is running {}, reachedHighWatermark {}", getContext().snapshotRunning(),
                    getContext().isTransactionVisible(eventTxId));

            getContext().closeWindow();
            sendWindowEvents(partition, offsetContext);
            readChunk(partition, offsetContext);
            if (getContext().watermarksChanged()) {
                LOGGER.trace("Watermarks changed");
                return;
            }

            LOGGER.trace("Re read chunk finished, snapshot is running {}, reachedHighWatermark {}", getContext().snapshotRunning(),
                    getContext().isTransactionVisible(eventTxId));
        }
    }

    private void getCurrentSnapshot(Consumer<PgSnapshot> watermark) {

        try {

            PgSnapshot pgSnapshot = jdbcConnection.queryAndMap(CURRENT_SNAPSHOT, singleResultMapper(rs -> {

                String currentSnapshot = rs.getString(1);
                LOGGER.trace("Current snapshot {}", currentSnapshot);

                return PgSnapshot.from(currentSnapshot);
            }, "Unable to get current snapshot"));

            watermark.accept(pgSnapshot);
        }
        catch (SQLException e) {
            throw new DebeziumException(e);
        }
    }

    private boolean maxInProgressTransactionCommitted(Long eventTxId) {

        if (getContext().getHighWatermark() == null) {
            return false;
        }

        return getContext().getHighWatermark().getXMax().equals(eventTxId);
    }

    private void forceNewTransactionId() {
        try {
            jdbcConnection.query(FORCE_NEW_TRANSACTION, rs -> {
                if (rs.next()) {
                    LOGGER.trace("Created new transaction ID {}", rs.getString(1));
                }
            });
        }
        catch (SQLException e) {
            throw new DebeziumException(e);
        }
    }

    private <T> JdbcConnection.ResultSetMapper<T> singleResultMapper(JdbcConnection.ResultSetExtractor<T> extractor, String error) throws SQLException {
        return (rs) -> {
            if (rs.next()) {
                final T ret = extractor.apply(rs);
                if (!rs.next()) {
                    return ret;
                }
            }
            throw new IllegalStateException(error);
        };
    }
}
