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

/**
 * A PostgreSQL specific read-only incremental snapshot change event source.
 * Uses {@code PgSnapshot} as low/high watermarks for incremental snapshot window.
 * <p>
 * <b>Prerequisites</b>
 * <ul>
 *      <li> PostgreSQL version >= 13 </li>
 * </ul>
 * </p>
 * <p>
 * <b>How a chunk is snapshotted</b>
 * <ul>
 *      <li> a pg_current_snapshot() query is executed and the low watermark is set to lowWatermark </li>
 *      <li> a new data chunk is read from a database by generating the SELECT statement and placed into a window buffer keyed by primary keys </li>
 *      <li> a pg_current_snapshot() query is executed and the high watermark is set to highWatermark </li>
 * </ul>
 * </p>
 * <p>
 * <b>During the subsequent streaming</b>
 * <ul>
 *      <li> if WAL event is received and its txId is greater then or equal to xMin of the low watermark, then window processing mode is enabled </li>
 *      <li> if WAL event is received and its txId is greater then the xMax of high watermark, then window processing mode is disabled and the rest of the window’s buffer is streamed </li>
 *      <li> if window processing mode is enabled then if the event key is contained in the window buffer then it is removed from the window buffer </li>
 *      <li> event is streamed </li>
 * </ul>
 * </p>
 * <br/>
 * <br/>
 * <b>No binlog events</b>
 * <p>Heartbeat events (events that are sent when there are not changes from the WAL) are used to update the window processing mode when the rate of WAL updates is low.</p>
 * <p>The heartbeat has the same txId as the latest WAL event at the moment and it will used to continue to read chunks even there are not event coming from the WAL. This processing will end if the watermarks changes because that means there are some transaction potentially modifying data.</p>
 * <br/>
 * <b>No changes between watermarks</b>
 * <p>A window can be opened and closed right away by the same event. This can happen when a high watermark and low watermark are the same, which means there were no active transaction during the chunk select. Chunk will get inserted right after the low watermark, no events will be deduplicated from the chunk</p>
 * <br/>
 *
 * @author Mario Fiore Vitale
 */
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
        if (getContext().snapshotRunning() && windowClosed) {
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
        schema.refreshFromIncrementalSnapshot(jdbcConnection, table.id());
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

            PgSnapshot pgSnapshot = jdbcConnection.queryAndMap(CURRENT_SNAPSHOT, jdbcConnection.singleResultMapper(rs -> {

                String currentSnapshot = rs.getString(1);
                LOGGER.trace("Current snapshot {}", currentSnapshot);

                return PgSnapshot.valueOf(currentSnapshot);
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

}
