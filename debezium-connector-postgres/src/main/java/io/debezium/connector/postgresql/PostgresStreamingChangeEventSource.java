/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import java.sql.SQLException;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.errors.ConnectException;
import com.yugabyte.core.BaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.postgresql.connection.LogicalDecodingMessage;
import io.debezium.connector.postgresql.connection.Lsn;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.PostgresReplicationConnection;
import io.debezium.connector.postgresql.connection.ReplicationConnection;
import io.debezium.connector.postgresql.connection.ReplicationMessage;
import io.debezium.connector.postgresql.connection.ReplicationMessage.Operation;
import io.debezium.connector.postgresql.connection.ReplicationStream;
import io.debezium.connector.postgresql.connection.WalPositionLocator;
import io.debezium.connector.postgresql.spi.Snapshotter;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.pipeline.ErrorHandler;
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
    private final PostgresEventDispatcher<TableId> dispatcher;
    private final ErrorHandler errorHandler;
    private final Clock clock;
    private final PostgresSchema schema;
    private final PostgresConnectorConfig connectorConfig;
    private final PostgresTaskContext taskContext;
    private final PostgresReplicationConnection replicationConnection;
    private final AtomicReference<ReplicationStream> replicationStream = new AtomicReference<>();
    private final Snapshotter snapshotter;
    private final DelayStrategy pauseNoMessage;
    private final ElapsedTimeStrategy connectionProbeTimer;

    // Offset committing is an asynchronous operation.
    // When connector is restarted we cannot be sure about timing of recovery, offset committing etc.
    // as this is driven by Kafka Connect. This might be a root cause of DBZ-5163.
    // This flag will ensure that LSN is flushed only if we are really in message processing mode.
    private volatile boolean lsnFlushingAllowed = false;

    /**
     * The minimum of (number of event received since the last event sent to Kafka,
     * number of event received since last WAL growing warning issued).
     */
    private long numberOfEventsSinceLastEventSentOrWalGrowingWarning = 0;
    private Lsn lastCompletelyProcessedLsn;
    private Lsn lastSentFeedback = Lsn.valueOf(2L);
    private PostgresOffsetContext effectiveOffset;

    protected ConcurrentLinkedQueue<Lsn> commitTimes;

    /**
     * For DEBUGGING
     */
    private OptionalLong lastTxnidForWhichCommitSeen = OptionalLong.empty();
    private long recordCount = 0;

    public PostgresStreamingChangeEventSource(PostgresConnectorConfig connectorConfig, Snapshotter snapshotter,
                                              PostgresConnection connection, PostgresEventDispatcher<TableId> dispatcher, ErrorHandler errorHandler, Clock clock,
                                              PostgresSchema schema, PostgresTaskContext taskContext, ReplicationConnection replicationConnection) {
        this.connectorConfig = connectorConfig;
        this.connection = connection;
        this.dispatcher = dispatcher;
        this.errorHandler = errorHandler;
        this.clock = clock;
        this.schema = schema;
        pauseNoMessage = DelayStrategy.constant(taskContext.getConfig().getPollInterval());
        this.taskContext = taskContext;
        this.snapshotter = snapshotter;
        this.replicationConnection = (PostgresReplicationConnection) replicationConnection;
        this.connectionProbeTimer = ElapsedTimeStrategy.constant(Clock.system(), connectorConfig.statusUpdateInterval());
        this.commitTimes = new ConcurrentLinkedQueue<>();
    }

    @Override
    public void init(PostgresOffsetContext offsetContext) {

        this.effectiveOffset = offsetContext == null ? PostgresOffsetContext.initialContext(connectorConfig, connection, clock) : offsetContext;
        // refresh the schema so we have a latest view of the DB tables
        initSchema();
    }

    private void initSchema() {
        try {
            taskContext.refreshSchema(connection, true);
        }
        catch (SQLException e) {
            throw new DebeziumException("Error while executing initial schema load", e);
        }
    }

    public Lsn getLsn(PostgresOffsetContext offsetContext, PostgresConnectorConfig.LsnType lsnType) {
        if (lsnType.isSequence()) {
            return this.effectiveOffset.lastCompletelyProcessedLsn() != null ? this.effectiveOffset.lastCompletelyProcessedLsn()
                    : this.effectiveOffset.lsn();
        } else {
            // We are in the block for HYBRID_TIME lsn type and last commit can be null for cases
            // where we have just started/restarted the connector, in that case, we simply sent the
            // initial value of lastSentFeedback and let the server handle the time we
            // should get the changes from.
            return this.effectiveOffset.lastCommitLsn() == null ?
                    lastSentFeedback : this.effectiveOffset.lastCommitLsn();
        }
    }

    @Override
    public void execute(ChangeEventSourceContext context, PostgresPartition partition, PostgresOffsetContext offsetContext)
            throws InterruptedException {
        if (!snapshotter.shouldStream()) {
            LOGGER.info("Streaming is not enabled in correct configuration");
            return;
        }

        lsnFlushingAllowed = false;

        // replication slot could exist at the time of starting Debezium so we will stream from the position in the slot
        // instead of the last position in the database
        boolean hasStartLsnStoredInContext = offsetContext != null;

        try {
            final WalPositionLocator walPosition;

            // This log can be printed either once or twice.
            // once - it means that the wal position is not being searched
            // twice - the wal position locator is searching for a wal position
            if (YugabyteDBServer.isEnabled()) {
                LOGGER.info("PID for replication connection: {} on node {}",
                  replicationConnection.getBackendPid(),
                  replicationConnection.getConnectedNodeIp());
            }

            if (hasStartLsnStoredInContext) {
                final Lsn lsn = getLsn(this.effectiveOffset, connectorConfig.slotLsnType());
                final Operation lastProcessedMessageType = this.effectiveOffset.lastProcessedMessageType();

                if (this.effectiveOffset.lastCommitLsn() == null) {
                    LOGGER.info("Last commit stored in offset is null");
                }

                LOGGER.info("Retrieved last committed LSN from stored offset '{}'", lsn);

                walPosition = new WalPositionLocator(this.effectiveOffset.lastCommitLsn(), lsn,
                        lastProcessedMessageType, connectorConfig.slotLsnType().isHybridTime() /* isLsnTypeHybridTime */);

                replicationStream.compareAndSet(null, replicationConnection.startStreaming(lsn, walPosition));
                lastSentFeedback = lsn;
            }
            else {
                LOGGER.info("No previous LSN found in Kafka, streaming from the latest xlogpos or flushed LSN...");
                walPosition = new WalPositionLocator(this.connectorConfig.slotLsnType().isHybridTime());
                replicationStream.compareAndSet(null, replicationConnection.startStreaming(walPosition));
            }
            // for large dbs, the refresh of schema can take too much time
            // such that the connection times out. We must enable keep
            // alive to ensure that it doesn't time out
            ReplicationStream stream = this.replicationStream.get();
            stream.startKeepAlive(Threads.newSingleThreadExecutor(YugabyteDBConnector.class, connectorConfig.getLogicalName(), KEEP_ALIVE_THREAD_NAME));

            initSchema();

            // If we need to do a pre-snapshot streaming catch up, we should allow the snapshot transaction to persist
            // but normally we want to start streaming without any open transactions.
            if (!isInPreSnapshotCatchUpStreaming(this.effectiveOffset)) {
                connection.commit();
            }

            this.lastCompletelyProcessedLsn = replicationStream.get().startLsn();

            // Against YB, filtering of records based on Wal position is only enabled when connector config provide.transaction.metadata is set to false.
            if (!YugabyteDBServer.isEnabled() || (YugabyteDBServer.isEnabled() && !connectorConfig.shouldProvideTransactionMetadata())) {
                if (walPosition.searchingEnabled()) {
                    searchWalPosition(context, partition, this.effectiveOffset, stream, walPosition);
                    try {
                        if (!isInPreSnapshotCatchUpStreaming(this.effectiveOffset)) {
                            connection.commit();
                        }
                    } catch (Exception e) {
                        LOGGER.info("Commit failed while preparing for reconnect", e);
                    }
                    
                    // Do not filter anything when lsn type is hybrid time. This is to avoid the WalPositionLocator complaining
                    // about the LSN not being present in the lsnSeen set.
                    if (connectorConfig.slotLsnType().isSequence()) {
                        walPosition.enableFiltering();
                    }

                    stream.stopKeepAlive();
                    replicationConnection.reconnect();

                    if (YugabyteDBServer.isEnabled()) {
                        LOGGER.info("PID for replication connection: {} on node {}",
                                replicationConnection.getBackendPid(),
                                replicationConnection.getConnectedNodeIp());
                    }

                    // For the HybridTime mode, we always want to resume from the position of last commit so that we
                    // send complete transactions and do not resume from the last event stored LSN.
                    Lsn lastStoredLsn = connectorConfig.slotLsnType().isHybridTime() ? walPosition.getLastCommitStoredLsn() : walPosition.getLastEventStoredLsn();
                    replicationStream.set(replicationConnection.startStreaming(lastStoredLsn, walPosition));

                    stream = this.replicationStream.get();
                    stream.startKeepAlive(Threads.newSingleThreadExecutor(YugabyteDBConnector.class, connectorConfig.getLogicalName(), KEEP_ALIVE_THREAD_NAME));
                }
            } else {
                LOGGER.info("Connector config provide.transaction.metadata is set to true. Therefore, skip records filtering in order to ship entire transactions.");
            }

            processMessages(context, partition, this.effectiveOffset, stream);
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

            boolean receivedMessage = stream.readPending(message -> processReplicationMessages(partition, offsetContext, stream, message));

            probeConnectionIfNeeded();

            if (receivedMessage) {
                noMessageIterations = 0;
                lsnFlushingAllowed = true;
            }
            else {
                dispatcher.dispatchHeartbeatEvent(partition, offsetContext);
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

            if (context.isPaused()) {
                LOGGER.info("Streaming will now pause");
                context.streamingPaused();
                context.waitSnapshotCompletion();
                LOGGER.info("Streaming resumed");
            }
        }
    }

    private void processReplicationMessages(PostgresPartition partition, PostgresOffsetContext offsetContext, ReplicationStream stream, ReplicationMessage message)
            throws SQLException, InterruptedException {

        final Lsn lsn = stream.lastReceivedLsn();

        if (message.isLastEventForLsn()) {
            lastCompletelyProcessedLsn = lsn;
        }

        // Tx BEGIN/END event
        if (message.isTransactionalMessage()) {
            if(message.getOperation() == Operation.BEGIN) {
                LOGGER.debug("Processing BEGIN with end LSN {} and txnid {}", lsn, message.getTransactionId());
            } else {
                LOGGER.debug("Processing COMMIT with end LSN {} and txnid {}", lsn, message.getTransactionId());
                LOGGER.debug("Record count in the txn {} is {} with commit time {}", message.getTransactionId(), recordCount, lsn.asLong() - 1);
                recordCount = 0;
            }

            OptionalLong currentTxnid = message.getTransactionId();
            if (lastTxnidForWhichCommitSeen.isPresent() && currentTxnid.isPresent()) {
                long delta = currentTxnid.getAsLong() - lastTxnidForWhichCommitSeen.getAsLong() - 1;
                if (delta > 0) {
                    LOGGER.debug("Skipped {} empty transactions between {} and {}", delta, lastTxnidForWhichCommitSeen, currentTxnid);
                }
            }
            lastTxnidForWhichCommitSeen = currentTxnid;

            if (!connectorConfig.shouldProvideTransactionMetadata()) {
                LOGGER.trace("Received transactional message {}", message);
                // Don't skip on BEGIN message as it would flush LSN for the whole transaction
                // too early
                if (message.getOperation() == Operation.COMMIT) {
                    commitMessage(partition, offsetContext, lsn, message);
                }
                return;
            }

            offsetContext.updateWalPosition(lsn, lastCompletelyProcessedLsn, message.getCommitTime(), toLong(message.getTransactionId()),
                    taskContext.getSlotXmin(connection),
                    null,
                    message.getOperation());
            if (message.getOperation() == Operation.BEGIN) {
                dispatcher.dispatchTransactionStartedEvent(partition, toString(message.getTransactionId()), offsetContext, message.getCommitTime());
            }
            else if (message.getOperation() == Operation.COMMIT) {
                commitMessage(partition, offsetContext, lsn, message);
                dispatcher.dispatchTransactionCommittedEvent(partition, offsetContext, message.getCommitTime());
            }
            maybeWarnAboutGrowingWalBacklog(true);
        }
        else if (message.getOperation() == Operation.MESSAGE) {
            offsetContext.updateWalPosition(lsn, lastCompletelyProcessedLsn, message.getCommitTime(), toLong(message.getTransactionId()),
                    taskContext.getSlotXmin(connection),
                    message.getOperation());

            // non-transactional message that will not be followed by a COMMIT message
            if (message.isLastEventForLsn()) {
                commitMessage(partition, offsetContext, lsn, message);
            }

            dispatcher.dispatchLogicalDecodingMessage(
                    partition,
                    offsetContext,
                    clock.currentTimeAsInstant().toEpochMilli(),
                    (LogicalDecodingMessage) message);

            maybeWarnAboutGrowingWalBacklog(true);
        }
        // DML event
        else {
            LOGGER.trace("Processing DML event with lsn {} and lastCompletelyProcessedLsn {}", lsn, lastCompletelyProcessedLsn);
            ++recordCount;

            TableId tableId = null;
            if (message.getOperation() != Operation.NOOP) {
                tableId = PostgresSchema.parse(message.getTable());
                Objects.requireNonNull(tableId);
            }

            offsetContext.updateWalPosition(lsn, lastCompletelyProcessedLsn, message.getCommitTime(), toLong(message.getTransactionId()),
                    taskContext.getSlotXmin(connection),
                    tableId,
                    message.getOperation());

            boolean dispatched = message.getOperation() != Operation.NOOP && dispatcher.dispatchDataChangeEvent(
                    partition,
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
    }

    private void searchWalPosition(ChangeEventSourceContext context, PostgresPartition partition, PostgresOffsetContext offsetContext,
                                   final ReplicationStream stream, final WalPositionLocator walPosition)
            throws SQLException, InterruptedException {
        AtomicReference<Lsn> resumeLsn = new AtomicReference<>();
        int noMessageIterations = 0;

        LOGGER.info("Searching for WAL resume position");
        while (context.isRunning() && resumeLsn.get() == null) {

            boolean receivedMessage = stream.readPending(message -> {
                final Lsn lsn;
                if (connectorConfig.slotLsnType().isHybridTime()) {
                    // Last commit can be null for cases where
                    // we have just started/restarted the connector, in that case, we simply sent the
                    // initial value of lastSentFeedback and let the server handle the time we
                    // should get the changes from.

                    lsn = walPosition.getLastCommitStoredLsn() != null ? walPosition.getLastCommitStoredLsn() : lastSentFeedback;
                } else {
                    lsn = stream.lastReceivedLsn();
                }
                resumeLsn.set(walPosition.resumeFromLsn(lsn, message).orElse(null));
            });

            if (receivedMessage) {
                noMessageIterations = 0;
            }
            else {
                dispatcher.dispatchHeartbeatEvent(partition, offsetContext);
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

    private void commitMessage(PostgresPartition partition, PostgresOffsetContext offsetContext, final Lsn lsn, ReplicationMessage message) throws SQLException, InterruptedException {
        lastCompletelyProcessedLsn = lsn;
        offsetContext.updateCommitPosition(lsn, lastCompletelyProcessedLsn);

        if (this.connectorConfig.slotLsnType().isHybridTime()) {
            if (message.getOperation() == Operation.COMMIT) {
                LOGGER.debug("Adding '{}' as lsn to the commit times queue", Lsn.valueOf(lsn.asLong() - 1));
                commitTimes.add(Lsn.valueOf(lsn.asLong() - 1));
            }
        }

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
    public void commitOffset(Map<String, ?> partition, Map<String, ?> offset) {
        try {
            ReplicationStream replicationStream = this.replicationStream.get();
            final Lsn commitLsn = Lsn.valueOf((Long) offset.get(PostgresOffsetContext.LAST_COMMIT_LSN_KEY));
            final Lsn changeLsn = Lsn.valueOf((Long) offset.get(PostgresOffsetContext.LAST_COMPLETELY_PROCESSED_LSN_KEY));
            final Lsn lsn = (commitLsn != null) ? commitLsn : changeLsn;

            LOGGER.debug("Received offset commit request on commit LSN '{}' and change LSN '{}'", commitLsn, changeLsn);
            if (replicationStream != null && lsn != null) {
                if (!lsnFlushingAllowed) {
                    LOGGER.info("Received offset commit request on '{}', but ignoring it. LSN flushing is not allowed yet", lsn);
                    return;
                }

                Lsn finalLsn;
                if (this.connectorConfig.slotLsnType().isHybridTime()) {
                    finalLsn = getLsnToBeFlushed(lsn);
                } else {
                    finalLsn = lsn;
                }

                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Flushing LSN to server: {}", finalLsn);
                }
                // tell the server the point up to which we've processed data, so it can be free to recycle WAL segments
                replicationStream.flushLsn(finalLsn);

                if (this.connectorConfig.slotLsnType().isHybridTime()) {
                    lastSentFeedback = finalLsn;
                    cleanCommitTimeQueue(finalLsn);
                }
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
     * Returns the LSN that should be flushed to the service. The {@code commitTimes} list will have
     * a list of all the commit times for which we have received a commit record. All we want now
     * is that whenever we get a commit callback, we should be flushing a time just smaller than
     * the one we have gotten the callback on.
     * @param lsn the {@link Lsn} received in callback
     * @return the {@link Lsn} to be flushed
     */
    protected Lsn getLsnToBeFlushed(Lsn lsn) {
        if (commitTimes == null || commitTimes.isEmpty()) {
            // This means that the queue has not been initialised and the task is still starting.
            return lastSentFeedback;
        }

        Lsn result = lastSentFeedback;

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Queue at this time: {}", commitTimes);
        }

        for (Lsn commitLsn : commitTimes) {
            if (commitLsn.compareTo(lsn) < 0) {
                LOGGER.debug("Assigning result as {}", commitLsn);
                result = commitLsn;
            } else {
                // This will be the loop exit when we encounter any bigger element.
                break;
            }
        }

        return result;
    }

    protected void cleanCommitTimeQueue(Lsn lsn) {
        if (commitTimes != null) {
            commitTimes.removeIf(ele -> ele.compareTo(lsn) < 1);
        }
    }

    @Override
    public PostgresOffsetContext getOffsetContext() {
        return effectiveOffset;
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

    private Long toLong(OptionalLong l) {
        return l.isPresent() ? Long.valueOf(l.getAsLong()) : null;
    }

    private String toString(OptionalLong l) {
        return l.isPresent() ? String.valueOf(l.getAsLong()) : null;
    }

    @FunctionalInterface
    public interface PgConnectionSupplier {
        BaseConnection get() throws SQLException;
    }
}
