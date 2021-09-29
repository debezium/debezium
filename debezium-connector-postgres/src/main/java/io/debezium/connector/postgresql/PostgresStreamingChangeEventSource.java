/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import java.sql.SQLException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.errors.ConnectException;
import org.postgresql.core.BaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.postgresql.connection.Lsn;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.ReplicationConnection;
import io.debezium.connector.postgresql.connection.ReplicationMessage.Operation;
import io.debezium.connector.postgresql.connection.ReplicationStream;
import io.debezium.connector.postgresql.connection.WalPositionLocator;
import io.debezium.connector.postgresql.spi.Snapshotter;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import io.debezium.util.DelayStrategy;
import io.debezium.util.ElapsedTimeStrategy;
import io.debezium.util.Threads;

/**
 *
 * @author Horia Chiorean (hchiorea@redhat.com), Jiri Pechanec
 */
public class PostgresStreamingChangeEventSource implements StreamingChangeEventSource<PostgresPartition, PostgresOffsetContext> {

    private static final String KEEP_ALIVE_THREAD_NAME = "keep-alive";

    /**
     * Number of received events without sending anything to Kafka which will
     * trigger a "WAL backlog growing" warning.
     */
    private static final int GROWING_WAL_WARNING_LOG_INTERVAL = 10_000;

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresStreamingChangeEventSource.class);

    // PGOUTPUT decoder sends the messages with larger time gaps than other decoders
    // We thus try to read the message multiple times before we make poll pause
    private static final int THROTTLE_NO_MESSAGE_BEFORE_PAUSE = 5;

    private final PostgresConnection connection;
    private final EventDispatcher<TableId> dispatcher;
    private final ErrorHandler errorHandler;
    private final Clock clock;
    private final PostgresSchema schema;
    private final PostgresConnectorConfig connectorConfig;
    private final PostgresTaskContext taskContext;
    private final ReplicationConnection replicationConnection;
    private final AtomicReference<ReplicationStream> replicationStream = new AtomicReference<>();
    private final Snapshotter snapshotter;
    private final DelayStrategy pauseNoMessage;
    private final ElapsedTimeStrategy connectionProbeTimer;

    /**
     * The minimum of (number of event received since the last event sent to Kafka,
     * number of event received since last WAL growing warning issued).
     */
    private long numberOfEventsSinceLastEventSentOrWalGrowingWarning = 0;
    private Lsn lastCompletelyProcessedLsn;

    public PostgresStreamingChangeEventSource(PostgresConnectorConfig connectorConfig, Snapshotter snapshotter,
                                              PostgresConnection connection, EventDispatcher<TableId> dispatcher, ErrorHandler errorHandler, Clock clock,
                                              PostgresSchema schema, PostgresTaskContext taskContext, ReplicationConnection replicationConnection) {
        this.connectorConfig = connectorConfig;
        this.connection = connection;
        this.dispatcher = dispatcher;
        this.errorHandler = errorHandler;
        this.clock = clock;
        this.schema = schema;
        pauseNoMessage = DelayStrategy.constant(taskContext.getConfig().getPollInterval().toMillis());
        this.taskContext = taskContext;
        this.snapshotter = snapshotter;
        this.replicationConnection = replicationConnection;
        this.connectionProbeTimer = ElapsedTimeStrategy.constant(Clock.system(), connectorConfig.statusUpdateInterval());

    }

    public void init() {
        // refresh the schema so we have a latest view of the DB tables
        try {
            taskContext.refreshSchema(connection, true);
        }
        catch (SQLException e) {
            throw new DebeziumException("Error whil executing initial schema load");
        }
    }

    @Override
    public void execute(ChangeEventSourceContext context, PostgresPartition partition, PostgresOffsetContext offsetContext)
            throws InterruptedException {
        if (!snapshotter.shouldStream()) {
            LOGGER.info("Streaming is not enabled in correct configuration");
            return;
        }

        // replication slot could exist at the time of starting Debezium so we will stream from the position in the slot
        // instead of the last position in the database
        boolean hasStartLsnStoredInContext = offsetContext != null;

        if (!hasStartLsnStoredInContext) {
            offsetContext = PostgresOffsetContext.initialContext(connectorConfig, connection, clock);
        }

        try {
            final WalPositionLocator walPosition;

            if (hasStartLsnStoredInContext) {
                // start streaming from the last recorded position in the offset
                final Lsn lsn = offsetContext.lastCompletelyProcessedLsn() != null ? offsetContext.lastCompletelyProcessedLsn() : offsetContext.lsn();
                LOGGER.info("Retrieved latest position from stored offset '{}'", lsn);
                walPosition = new WalPositionLocator(offsetContext.lastCommitLsn(), lsn);
                replicationStream.compareAndSet(null, replicationConnection.startStreaming(lsn, walPosition));
            }
            else {
                LOGGER.info("No previous LSN found in Kafka, streaming from the latest xlogpos or flushed LSN...");
                walPosition = new WalPositionLocator();
                replicationStream.compareAndSet(null, replicationConnection.startStreaming(walPosition));
            }
            // for large dbs, the refresh of schema can take too much time
            // such that the connection times out. We must enable keep
            // alive to ensure that it doesn't time out
            ReplicationStream stream = this.replicationStream.get();
            stream.startKeepAlive(Threads.newSingleThreadExecutor(PostgresConnector.class, connectorConfig.getLogicalName(), KEEP_ALIVE_THREAD_NAME));

            init();

            // If we need to do a pre-snapshot streaming catch up, we should allow the snapshot transaction to persist
            // but normally we want to start streaming without any open transactions.
            if (!isInPreSnapshotCatchUpStreaming(offsetContext)) {
                connection.commit();
            }

            this.lastCompletelyProcessedLsn = replicationStream.get().startLsn();

            if (walPosition.searchingEnabled()) {
                searchWalPosition(context, stream, walPosition);
                try {
                    if (!isInPreSnapshotCatchUpStreaming(offsetContext)) {
                        connection.commit();
                    }
                }
                catch (Exception e) {
                    LOGGER.info("Commit failed while preparing for reconnect", e);
                }
                walPosition.enableFiltering();
                stream.stopKeepAlive();
                replicationConnection.reconnect();
                replicationStream.set(replicationConnection.startStreaming(walPosition.getLastEventStoredLsn(), walPosition));
                stream = this.replicationStream.get();
                stream.startKeepAlive(Threads.newSingleThreadExecutor(PostgresConnector.class, connectorConfig.getLogicalName(), KEEP_ALIVE_THREAD_NAME));
            }
            processMessages(context, partition, offsetContext, stream);
        }
        catch (Throwable e) {
            errorHandler.setProducerThrowable(e);
        }
        finally {
            if (replicationConnection != null) {
                LOGGER.debug("stopping streaming...");
                // stop the keep alive thread, this also shuts down the
                // executor pool
                ReplicationStream stream = replicationStream.get();
                if (stream != null) {
                    stream.stopKeepAlive();
                }
                // TODO author=Horia Chiorean date=08/11/2016 description=Ideally we'd close the stream, but it's not reliable atm (see javadoc)
                // replicationStream.close();
                // close the connection - this should also disconnect the current stream even if it's blocking
                try {
                    if (!isInPreSnapshotCatchUpStreaming(offsetContext)) {
                        connection.commit();
                    }
                    replicationConnection.close();
                }
                catch (Exception e) {
                    LOGGER.debug("Exception while closing the connection", e);
                }
                replicationStream.set(null);
            }
        }
    }

    private void processMessages(ChangeEventSourceContext context, PostgresPartition partition, PostgresOffsetContext offsetContext, final ReplicationStream stream)
            throws SQLException, InterruptedException {
        LOGGER.info("Processing messages");
        int noMessageIterations = 0;
        while (context.isRunning() && (offsetContext.getStreamingStoppingLsn() == null ||
                (lastCompletelyProcessedLsn.compareTo(offsetContext.getStreamingStoppingLsn()) < 0))) {

            boolean receivedMessage = stream.readPending(message -> {
                final Lsn lsn = stream.lastReceivedLsn();

                if (message.isLastEventForLsn()) {
                    lastCompletelyProcessedLsn = lsn;
                }

                // Tx BEGIN/END event
                if (message.isTransactionalMessage()) {
                    if (!connectorConfig.shouldProvideTransactionMetadata()) {
                        LOGGER.trace("Received transactional message {}", message);
                        // Don't skip on BEGIN message as it would flush LSN for the whole transaction
                        // too early
                        if (message.getOperation() == Operation.COMMIT) {
                            commitMessage(partition, offsetContext, lsn);
                        }
                        return;
                    }

                    offsetContext.updateWalPosition(lsn, lastCompletelyProcessedLsn, message.getCommitTime(), message.getTransactionId(), null,
                            taskContext.getSlotXmin(connection));
                    if (message.getOperation() == Operation.BEGIN) {
                        dispatcher.dispatchTransactionStartedEvent(partition, Long.toString(message.getTransactionId()), offsetContext);
                    }
                    else if (message.getOperation() == Operation.COMMIT) {
                        commitMessage(partition, offsetContext, lsn);
                        dispatcher.dispatchTransactionCommittedEvent(partition, offsetContext);
                    }
                    maybeWarnAboutGrowingWalBacklog(true);
                }
                // DML event
                else {
                    TableId tableId = null;
                    if (message.getOperation() != Operation.NOOP) {
                        tableId = PostgresSchema.parse(message.getTable());
                        Objects.requireNonNull(tableId);
                    }

                    offsetContext.updateWalPosition(lsn, lastCompletelyProcessedLsn, message.getCommitTime(), message.getTransactionId(), tableId,
                            taskContext.getSlotXmin(connection));

                    boolean dispatched = message.getOperation() != Operation.NOOP && dispatcher.dispatchDataChangeEvent(
                            tableId,
                            new PostgresChangeRecordEmitter(
                                    partition,
                                    offsetContext,
                                    clock,
                                    connectorConfig,
                                    schema,
                                    connection,
                                    tableId,
                                    message));

                    maybeWarnAboutGrowingWalBacklog(dispatched);
                }
            });

            probeConnectionIfNeeded();

            if (receivedMessage) {
                noMessageIterations = 0;
            }
            else {
                if (offsetContext.hasCompletelyProcessedPosition()) {
                    dispatcher.dispatchHeartbeatEvent(partition, offsetContext);
                }
                noMessageIterations++;
                if (noMessageIterations >= THROTTLE_NO_MESSAGE_BEFORE_PAUSE) {
                    noMessageIterations = 0;
                    pauseNoMessage.sleepWhen(true);
                }
            }
            if (!isInPreSnapshotCatchUpStreaming(offsetContext)) {
                // During catch up streaming, the streaming phase needs to hold a transaction open so that
                // the phase can stream event up to a specific lsn and the snapshot that occurs after the catch up
                // streaming will not lose the current view of data. Since we need to hold the transaction open
                // for the snapshot, this block must not commit during catch up streaming.
                connection.commit();
            }
        }
    }

    private void searchWalPosition(ChangeEventSourceContext context, final ReplicationStream stream, final WalPositionLocator walPosition)
            throws SQLException, InterruptedException {
        AtomicReference<Lsn> resumeLsn = new AtomicReference<>();
        int noMessageIterations = 0;

        LOGGER.info("Searching for WAL resume position");
        while (context.isRunning() && resumeLsn.get() == null) {

            boolean receivedMessage = stream.readPending(message -> {
                final Lsn lsn = stream.lastReceivedLsn();
                resumeLsn.set(walPosition.resumeFromLsn(lsn, message).orElse(null));
            });

            if (receivedMessage) {
                noMessageIterations = 0;
            }
            else {
                noMessageIterations++;
                if (noMessageIterations >= THROTTLE_NO_MESSAGE_BEFORE_PAUSE) {
                    noMessageIterations = 0;
                    pauseNoMessage.sleepWhen(true);
                }
            }

            probeConnectionIfNeeded();
        }
        LOGGER.info("WAL resume position '{}' discovered", resumeLsn.get());
    }

    private void probeConnectionIfNeeded() throws SQLException {
        if (connectionProbeTimer.hasElapsed()) {
            connection.prepareQuery("SELECT 1");
            connection.commit();
        }
    }

    private void commitMessage(PostgresPartition partition, PostgresOffsetContext offsetContext, final Lsn lsn) throws SQLException, InterruptedException {
        lastCompletelyProcessedLsn = lsn;
        offsetContext.updateCommitPosition(lsn, lastCompletelyProcessedLsn);
        maybeWarnAboutGrowingWalBacklog(false);
        dispatcher.dispatchHeartbeatEvent(partition, offsetContext);
    }

    /**
     * If we receive change events but all of them get filtered out, we cannot
     * commit any new offset with Apache Kafka. This in turn means no LSN is ever
     * acknowledged with the replication slot, causing any ever growing WAL backlog.
     * <p>
     * This situation typically occurs if there are changes on the database server,
     * (e.g. in an excluded database), but none of them is in table.include.list.
     * To prevent this, heartbeats can be used, as they will allow us to commit
     * offsets also when not propagating any "real" change event.
     * <p>
     * The purpose of this method is to detect this situation and log a warning
     * every {@link #GROWING_WAL_WARNING_LOG_INTERVAL} filtered events.
     *
     * @param dispatched
     *            Whether an event was sent to the broker or not
     */
    private void maybeWarnAboutGrowingWalBacklog(boolean dispatched) {
        if (dispatched) {
            numberOfEventsSinceLastEventSentOrWalGrowingWarning = 0;
        }
        else {
            numberOfEventsSinceLastEventSentOrWalGrowingWarning++;
        }

        if (numberOfEventsSinceLastEventSentOrWalGrowingWarning > GROWING_WAL_WARNING_LOG_INTERVAL && !dispatcher.heartbeatsEnabled()) {
            LOGGER.warn("Received {} events which were all filtered out, so no offset could be committed. "
                    + "This prevents the replication slot from acknowledging the processed WAL offsets, "
                    + "causing a growing backlog of non-removeable WAL segments on the database server. "
                    + "Consider to either adjust your filter configuration or enable heartbeat events "
                    + "(via the {} option) to avoid this situation.",
                    numberOfEventsSinceLastEventSentOrWalGrowingWarning, Heartbeat.HEARTBEAT_INTERVAL_PROPERTY_NAME);

            numberOfEventsSinceLastEventSentOrWalGrowingWarning = 0;
        }
    }

    @Override
    public void commitOffset(Map<String, ?> offset) {
        try {
            ReplicationStream replicationStream = this.replicationStream.get();
            final Lsn commitLsn = Lsn.valueOf((Long) offset.get(PostgresOffsetContext.LAST_COMMIT_LSN_KEY));
            final Lsn changeLsn = Lsn.valueOf((Long) offset.get(PostgresOffsetContext.LAST_COMPLETELY_PROCESSED_LSN_KEY));
            final Lsn lsn = (commitLsn != null) ? commitLsn : changeLsn;

            if (replicationStream != null && lsn != null) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Flushing LSN to server: {}", lsn);
                }
                // tell the server the point up to which we've processed data, so it can be free to recycle WAL segments
                replicationStream.flushLsn(lsn);
            }
            else {
                LOGGER.debug("Streaming has already stopped, ignoring commit callback...");
            }
        }
        catch (SQLException e) {
            throw new ConnectException(e);
        }
    }

    /**
     * Returns whether the current streaming phase is running a catch up streaming
     * phase that runs before a snapshot. This is useful for transaction
     * management.
     *
     * During pre-snapshot catch up streaming, we open the snapshot transaction
     * early and hold the transaction open throughout the pre snapshot catch up
     * streaming phase so that we know where to stop streaming and can start the
     * snapshot phase at a consistent location. This is opposed the regular streaming,
     * where we do not a lingering open transaction.
     *
     * @return true if the current streaming phase is performing catch up streaming
     */
    private boolean isInPreSnapshotCatchUpStreaming(PostgresOffsetContext offsetContext) {
        return offsetContext.getStreamingStoppingLsn() != null;
    }

    @FunctionalInterface
    public static interface PgConnectionSupplier {
        BaseConnection get() throws SQLException;
    }
}
