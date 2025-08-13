/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.unbuffered;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.common.annotation.Incubating;
import io.debezium.config.Configuration;
import io.debezium.connector.oracle.CommitScn;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OraclePartition;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.AbstractLogMinerStreamingChangeEventSource;
import io.debezium.connector.oracle.logminer.LogMinerChangeRecordEmitter;
import io.debezium.connector.oracle.logminer.LogMinerStreamingChangeEventSourceMetrics;
import io.debezium.connector.oracle.logminer.TransactionCommitConsumer;
import io.debezium.connector.oracle.logminer.events.DmlEvent;
import io.debezium.connector.oracle.logminer.events.EventType;
import io.debezium.connector.oracle.logminer.events.LogMinerEvent;
import io.debezium.connector.oracle.logminer.events.LogMinerEventRow;
import io.debezium.connector.oracle.logminer.events.RedoSqlDmlEvent;
import io.debezium.connector.oracle.logminer.events.TruncateEvent;
import io.debezium.connector.oracle.logminer.parser.LogMinerDmlEntry;
import io.debezium.data.Envelope;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import io.debezium.util.Loggings;
import io.debezium.util.Stopwatch;
import io.debezium.util.Strings;

/**
 * An Oracle LogMiner {@link io.debezium.pipeline.source.spi.StreamingChangeEventSource} implementation that
 * relies on using Oracle LogMiner's {@code COMMITTED_DATA_ONLY} mode to capture changes without requiring
 * that the connector buffer large transactions.
 *
 * @author Chris Cranford
 */
@Incubating
public class UnbufferedLogMinerStreamingChangeEventSource extends AbstractLogMinerStreamingChangeEventSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(UnbufferedLogMinerStreamingChangeEventSource.class);

    private final String miningQuery;
    private final boolean includeSql;
    private final TransactionCommitConsumer accumulator;
    private final List<LogMinerEventRow> ddlQueue = new ArrayList<>();
    private final ResumePositionProvider resumePositionProvider;

    private boolean skipCurrentTransaction = false;
    private ZoneOffset databaseOffset;
    private Scn lastCommitScn = Scn.NULL;

    public UnbufferedLogMinerStreamingChangeEventSource(OracleConnectorConfig connectorConfig,
                                                        OracleConnection jdbcConnection,
                                                        EventDispatcher<OraclePartition, TableId> dispatcher,
                                                        ErrorHandler errorHandler,
                                                        Clock clock,
                                                        OracleDatabaseSchema schema,
                                                        Configuration jdbcConfig,
                                                        LogMinerStreamingChangeEventSourceMetrics metrics) {
        super(connectorConfig, jdbcConnection, dispatcher, errorHandler, clock, schema, jdbcConfig, metrics);
        this.miningQuery = new UnbufferedLogMinerQueryBuilder(connectorConfig).getQuery();
        this.includeSql = connectorConfig.isLogMiningIncludeRedoSql();
        this.accumulator = new TransactionCommitConsumer(this::dispatchEvent, connectorConfig, schema);
        this.resumePositionProvider = new ResumePositionProvider(connectorConfig, getJdbcConfiguration());
    }

    @Override
    protected void executeLogMiningStreaming() throws Exception {

        Scn upperBoundsScn = Scn.NULL;

        Scn minLogScn = getOffsetContext().getScn();
        Scn minCommitScn = getOffsetContext().getCommitScn().getMinCommittedScn();

        if (minCommitScn.isNull()) {
            minCommitScn = minLogScn;
        }

        Stopwatch watch = Stopwatch.accumulating().start();
        int miningStartAttempts = 1;

        prepareLogsForMining(false, minLogScn);

        while (getContext().isRunning()) {

            // Check if we should break when using archive log only mode
            if (getConfig().isArchiveLogOnlyMode()) {
                if (waitForRangeAvailabilityInArchiveLogs(minLogScn, upperBoundsScn)) {
                    break;
                }
            }

            final Instant batchStartTime = Instant.now();

            updateDatabaseTimeDifference();

            // This avoids the AtomicReference for each event as this value isn't updated
            // except once per iteration.
            databaseOffset = getMetrics().getDatabaseOffset();

            minLogScn = computeResumeScnAndUpdateOffsets(minLogScn, minCommitScn);

            getMetrics().setOffsetScn(minLogScn);

            Scn currentScn = getCurrentScn();
            getMetrics().setCurrentScn(currentScn);

            upperBoundsScn = calculateUpperBounds(minLogScn, upperBoundsScn, currentScn);
            if (upperBoundsScn.isNull()) {
                LOGGER.debug("Delaying mining transaction logs by one iteration");
                pauseBetweenMiningSessions();
                continue;
            }

            if (isMiningSessionRestartRequired(watch) || checkLogSwitchOccurredAndUpdate()) {
                // Mining session is active, so end the current session and restart if necessary
                endMiningSession();
                if (getConfig().isLogMiningRestartConnection()) {
                    prepareJdbcConnection(true);
                }

                prepareLogsForMining(true, minLogScn);

                // Recreate the stop watch
                watch = Stopwatch.accumulating().start();
            }

            if (startMiningSession(minLogScn, Scn.NULL, miningStartAttempts)) {
                miningStartAttempts = 1;
                minCommitScn = process(minCommitScn);

                getMetrics().setLastBatchProcessingDuration(Duration.between(batchStartTime, Instant.now()));
            }
            else {
                miningStartAttempts++;
            }

            captureJdbcSessionMemoryStatistics();

            pauseBetweenMiningSessions();

            if (getContext().isPaused()) {
                // Blocking snapshots will be based on the last commit flush
                // So we need to temporarily advance the SCN to that point.
                Scn currentOffsetScn = getOffsetContext().getScn();
                getOffsetContext().setScn(minCommitScn);

                executeBlockingSnapshot();

                // Now restore the old resume SCN position.
                getOffsetContext().setScn(currentOffsetScn);
            }
        }
    }

    @Override
    protected boolean isUsingCommittedDataOnly() {
        return true;
    }

    @Override
    public void close() {
        try {
            resumePositionProvider.close();
        }
        catch (Exception e) {
            LOGGER.warn("Failed to gracefully shutdown the resume position provider", e);
        }
    }

    @Override
    protected void enqueueEvent(LogMinerEventRow event, LogMinerEvent dispatchedEvent) throws InterruptedException {
        getMetrics().calculateLagFromSource(event.getChangeTime());
        accumulator.accept(dispatchedEvent);
    }

    @Override
    protected void executeDataChangeEventPreDispatchSteps(LogMinerEventRow event) throws InterruptedException {
        // If we receive a DML event within a transaction that contains DDL events, flush the DDL
        // events before processing the DML event.
        if (!ddlQueue.isEmpty()) {
            if (!event.getCommitScn().equals(getOffsetContext().getEventCommitScn())) {
                // Given that DDL events do not carry a COMMIT_SCN, overlay the DML's COMMIT_SCN
                // since its part of the same transaction.
                getOffsetContext().setEventCommitScn(event.getCommitScn());
            }
            dispatchSchemaChanges();
        }
    }

    /**
     * Recomputes the resume position, optionally updating the offsets if the resume position
     * changes from its prior value.
     *
     * @param resumeScn the current resume system change number, should not be {@code null}
     * @param commitScn the current commit system change number, should not be {@code null}
     * @return the next iteration's resume system change number, never {@code null}
     * @throws SQLException if a database exception occurs
     */
    private Scn computeResumeScnAndUpdateOffsets(Scn resumeScn, Scn commitScn) throws SQLException {
        final Scn computedResumeScn = resumePositionProvider.computeResumePositionFromLogs(
                resumeScn, commitScn, getCurrentLogFiles());

        if (!computedResumeScn.equals(resumeScn)) {
            LOGGER.debug("Advancing offset low-watermark scn to {}", computedResumeScn);
            getOffsetContext().setScn(computedResumeScn);
        }

        return computedResumeScn;
    }

    private PreparedStatement createQueryStatement() throws SQLException {
        final PreparedStatement statement = getConnection().connection()
                .prepareStatement(miningQuery,
                        ResultSet.TYPE_FORWARD_ONLY,
                        ResultSet.CONCUR_READ_ONLY,
                        ResultSet.HOLD_CURSORS_OVER_COMMIT);
        statement.setQueryTimeout((int) getConnection().config().getQueryTimeout().toSeconds());
        return statement;
    }

    /**
     * Processes the Oracle LogMiner data between the specified bounds.
     *
     * @param minCommitScn mining range lower bounds SCN, should not be {@code null}
     * @return the next iteration's lower bounds SCN, never {@code null}
     * @throws SQLException if a database exception occurred
     * @throws InterruptedException if the thread is interrupted
     */
    private Scn process(Scn minCommitScn) throws SQLException, InterruptedException {
        getBatchMetrics().reset();

        try (PreparedStatement statement = createQueryStatement()) {
            LOGGER.debug("Fetching results with COMMIT_SCN >= {}", minCommitScn);
            statement.setFetchSize(getConfig().getQueryFetchSize());
            statement.setFetchDirection(ResultSet.FETCH_FORWARD);
            statement.setString(1, minCommitScn.toString());

            if (getConfig().isLogMiningUseCteQuery()) {
                statement.setString(2, minCommitScn.toString());
            }

            lastCommitScn = minCommitScn;

            executeAndProcessQuery(statement);

            if (!minCommitScn.equals(lastCommitScn)) {
                LOGGER.debug("Adjusting Min Commit SCN from {} to {}.", minCommitScn, lastCommitScn);
            }

            return lastCommitScn;
        }
    }

    @Override
    protected void processEvent(LogMinerEventRow event) throws SQLException, InterruptedException {
        super.processEvent(event);

        // Regardless of whether events are processed, we should always update the lastCommitScn
        if (EventType.COMMIT.equals(event.getEventType())) {
            lastCommitScn = event.getCommitScn();

            // If the current transaction was marked skipped, reset it.
            // It's safe to do this for all commits because this implementation does not overlap
            // transactions, they're emitted in commit chronological order.
            skipCurrentTransaction = false;
        }
    }

    @Override
    protected boolean isEventSkipped(LogMinerEventRow event) {
        return skipCurrentTransaction || isEventIncludedInSnapshot(event) || isNonSchemaChangeEventSkipped(event);
    }

    @Override
    protected boolean hasEventBeenProcessed(LogMinerEventRow event) {
        // DDL events and their corresponding START events do not have a COMMIT_SCN value.
        // In such cases, we default to the event's SCN instead.
        final Scn scn = event.getCommitScn().isNull() ? event.getScn() : event.getCommitScn();
        if (getOffsetContext().getCommitScn().hasBeenHandled(event.getThread(), scn, event.getTransactionId())) {
            return true;
        }

        if (Objects.equals(getOffsetContext().getTransactionId(), event.getTransactionId())) {
            if (getOffsetContext().getTransactionSequence() != null) {
                return getOffsetContext().getTransactionSequence() >= event.getTransactionSequence();
            }
        }

        return false;
    }

    @Override
    protected void handleStartEvent(LogMinerEventRow event) {
        skipCurrentTransaction = false;

        if (!event.getCommitScn().isNull()) {
            final CommitScn offsetCommitScn = getOffsetContext().getCommitScn();
            if (offsetCommitScn.hasBeenHandled(event.getThread(), event.getCommitScn(), event.getTransactionId())) {
                // Transaction has already been fully handled, skip.
                LOGGER.info("Skipping transaction {} with SCN {}, already committed.", event.getTransactionId(), event.getScn());
                skipCurrentTransaction = true;
                return;
            }
        }

        // Check whether transaction should be skipped by USERNAME or CLIENT_ID field
        if (isUserNameSkipped(event.getUserName()) || isClientIdSkipped(event.getClientId())) {
            skipCurrentTransaction = true;
            return;
        }

        Loggings.logDebugAndTraceRecord(
                LOGGER,
                event,
                "Starting transaction {} with SCN {}",
                event.getTransactionId(),
                event.getScn());

        // For some events, START_TIMESTAMP may be null
        Instant startTime = event.getStartTime();
        if (startTime != null) {
            startTime = startTime.minusSeconds(databaseOffset.getTotalSeconds());
        }

        // For some events, COMMIT_TIMESTAMP may be null
        Instant commitTime = event.getCommitTime();
        if (commitTime != null) {
            commitTime = commitTime.minusSeconds(databaseOffset.getTotalSeconds());
        }

        // These are START specific attributes that must be recorded
        getOffsetContext().setStartScn(event.getScn());
        getOffsetContext().setRedoThread(event.getThread());
        getOffsetContext().setStartTime(startTime);
        getOffsetContext().setCommitTime(commitTime);
        getOffsetContext().setEventCommitScn(event.getCommitScn());
        getOffsetContext().setUserName(event.getUserName());
        getOffsetContext().setTransactionId(event.getTransactionId());
        getOffsetContext().setTransactionSequence(null);

        getMetrics().setActiveTransactionCount(1L);
    }

    @Override
    protected void handleCommitEvent(LogMinerEventRow event) throws InterruptedException {
        Loggings.logDebugAndTraceRecord(
                LOGGER,
                event,
                "Commit transaction {} at SCN {}.",
                event.getTransactionId(),
                event.getScn());

        final Instant commitStartTime = Instant.now();

        // These are COMMIT specific attributes that must be recorded
        getOffsetContext().getCommitScn().recordCommit(event);
        getOffsetContext().setEventScn(event.getScn());
        getOffsetContext().setEventCommitScn(event.getScn());

        if (!ddlQueue.isEmpty()) {
            dispatchSchemaChanges();
        }

        accumulator.close();

        getEventDispatcher().dispatchTransactionCommittedEvent(getPartition(), getOffsetContext(), event.getChangeTime());

        getBatchMetrics().commitObserved();

        getMetrics().setActiveTransactionCount(0L);
        updateCommitMetrics(event, Duration.between(commitStartTime, Instant.now()));
    }

    @Override
    protected void handleRollbackEvent(LogMinerEventRow event) throws InterruptedException {
        if (accumulator.getTotalEvents() == 0) {
            // Check if the accumulator has no events, and silently ignore rollback transaction with a warning.
            LOGGER.warn("A rollback transaction {} with SCN {} detected with no captured changes", event.getTransactionId(), event.getScn());
            getMetrics().incrementWarningCount();

            accumulator.close();
            return;
        }

        throw new DebeziumException(String.format("Potential Oracle LogMiner Bug - " +
                "Rollback transaction %s with SCN %s found emitted %d captured changes. " +
                "A re-snapshot may be required. Please review your topics populated by this transaction.",
                event.getTransactionId(), event.getScn().toString(), accumulator.getTotalEvents()));
    }

    @Override
    protected void handleSchemaChangeEvent(LogMinerEventRow event) {
        if (isSchemaChangeEventSkipped(event)) {
            return;
        }

        if (!Strings.isNullOrBlank(event.getTableName())) {
            Loggings.logDebugAndTraceRecord(LOGGER, event, "Processing DDL event with SCN {}: {}",
                    event.getScn(), event.getRedoSql());

            // In transactions that wrap DDL operations, the START and DDL event markers do not carry
            // START_SCN, START_TIMESTAMP, COMMIT_SCN, nor COMMIT_TIMESTAMP values. If we emit the
            // DDL immediately, this leads to issues with synchronization points. So instead, all
            // DDL events within the transaction scope are delayed until the COMMIT.
            ddlQueue.add(event);
        }
    }

    @Override
    protected void handleReplicationMarkerEvent(LogMinerEventRow event) {
        // Normally we would do something with this; however this can be safely ignored.
        LOGGER.trace("Skipped GoldenGate replication marker event: {}", Loggings.maybeRedactSensitiveData(event));
    }

    @Override
    protected void handleTruncateEvent(LogMinerEventRow event) throws InterruptedException {
        try {
            final Table table = getTableForDataEvent(event);
            if (table != null) {
                LOGGER.debug("Dispatching TRUNCATE event for table '{}' with SCN {}", table.id(), event.getScn());

                final LogMinerDmlEntry parsedEvent = parseTruncateEvent(event);

                // Truncate events are wrapped in START/COMMIT markers
                getOffsetContext().getCommitScn().recordCommit(event);
                // getOffsetContext().setScn(event.getScn());
                getOffsetContext().setEventScn(event.getScn());
                getOffsetContext().setRedoThread(event.getThread());
                getOffsetContext().setRsId(event.getRsId());
                getOffsetContext().setRowId("");
                getOffsetContext().setTransactionId(event.getTransactionId());
                getOffsetContext().setTransactionSequence(event.getTransactionSequence());

                if (includeSql) {
                    getOffsetContext().setRedoSql(event.getRedoSql());
                }

                getEventDispatcher().dispatchDataChangeEvent(
                        getPartition(),
                        table.id(),
                        new LogMinerChangeRecordEmitter(
                                getConfig(),
                                getPartition(),
                                getOffsetContext(),
                                Envelope.Operation.TRUNCATE,
                                parsedEvent.getOldValues(),
                                parsedEvent.getNewValues(),
                                table,
                                getSchema(),
                                getClock()));
            }
        }
        catch (SQLException e) {
            LOGGER.warn("Failed to process truncate event", e);
            getMetrics().incrementWarningCount();
        }
        finally {
            if (includeSql) {
                getOffsetContext().setRedoSql("");
            }
        }
    }

    @Override
    protected boolean isNoDataProcessedInBatchAndAtEndOfArchiveLogs() {
        return !getMetrics().getBatchMetrics().hasProcessedAnyTransactions();
    }

    /**
     * Handles the dispatch of events added to the {@link #accumulator}.
     *
     * @param event the event to be dispatched, never {@code null}
     * @param eventsProcessed the number of events dispatched thus far
     * @throws InterruptedException if the thread is interrupted
     */
    protected void dispatchEvent(LogMinerEvent event, long eventsProcessed) throws InterruptedException {
        if (event instanceof TruncateEvent truncateEvent) {
            final int databaseOffsetSeconds = databaseOffset.getTotalSeconds();

            getMetrics().calculateLagFromSource(truncateEvent.getChangeTime());

            // Set per-event details
            getOffsetContext().setEventScn(truncateEvent.getScn());
            getOffsetContext().setTransactionId(truncateEvent.getTransactionId());
            getOffsetContext().setTransactionSequence(truncateEvent.getTransactionSequence());
            getOffsetContext().setSourceTime(truncateEvent.getChangeTime().minusSeconds(databaseOffsetSeconds));
            getOffsetContext().setTableId(truncateEvent.getTableId());
            getOffsetContext().setRsId(truncateEvent.getRsId());
            getOffsetContext().setRowId(truncateEvent.getRowId());

            getEventDispatcher().dispatchDataChangeEvent(
                    getPartition(),
                    truncateEvent.getTableId(),
                    new LogMinerChangeRecordEmitter(
                            getConfig(),
                            getPartition(),
                            getOffsetContext(),
                            Envelope.Operation.TRUNCATE,
                            truncateEvent.getDmlEntry().getOldValues(),
                            truncateEvent.getDmlEntry().getNewValues(),
                            getSchema().tableFor(truncateEvent.getTableId()),
                            getSchema(),
                            Clock.system()));

        }
        else if (event instanceof DmlEvent dmlEvent) {
            final int databaseOffsetSeconds = databaseOffset.getTotalSeconds();

            getMetrics().calculateLagFromSource(dmlEvent.getChangeTime());

            // Set per-event details
            getOffsetContext().setEventScn(dmlEvent.getScn());
            getOffsetContext().setTransactionId(dmlEvent.getTransactionId());
            getOffsetContext().setTransactionSequence(dmlEvent.getTransactionSequence());
            getOffsetContext().setSourceTime(dmlEvent.getChangeTime().minusSeconds(databaseOffsetSeconds));
            getOffsetContext().setTableId(dmlEvent.getTableId());
            getOffsetContext().setRsId(dmlEvent.getRsId());
            getOffsetContext().setRowId(dmlEvent.getRowId());

            if (event instanceof RedoSqlDmlEvent redoDmlEvent) {
                getOffsetContext().setRedoSql(redoDmlEvent.getRedoSql());
            }

            getEventDispatcher().dispatchDataChangeEvent(
                    getPartition(),
                    dmlEvent.getTableId(),
                    new LogMinerChangeRecordEmitter(
                            getConfig(),
                            getPartition(),
                            getOffsetContext(),
                            dmlEvent.getDmlEntry().getEventType(),
                            dmlEvent.getDmlEntry().getOldValues(),
                            dmlEvent.getDmlEntry().getNewValues(),
                            getSchema().tableFor(dmlEvent.getTableId()),
                            getSchema(),
                            Clock.system()));
        }

        getOffsetContext().setRedoSql(null);
    }

    /**
     * Handles dispatching any queue schema changes.
     *
     * @throws InterruptedException if the thread is interrupted
     */
    private void dispatchSchemaChanges() throws InterruptedException {
        for (LogMinerEventRow event : ddlQueue) {
            dispatchSchemaChangeEventInternal(event);
        }
        ddlQueue.clear();
    }

}
