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
            if (isArchiveLogOnlyModeAndScnIsNotAvailable(minLogScn)) {
                break;
            }

            final Instant batchStartTime = Instant.now();

            updateDatabaseTimeDifference();

            // This avoids the AtomicReference for each event as this value isn't updated
            // except once per iteration.
            databaseOffset = getMetrics().getDatabaseOffset();

            minLogScn = resumePositionProvider.computeResumePositionFromLogs(minLogScn, minCommitScn, getCurrentLogFiles());

            getMetrics().setOffsetScn(minLogScn);

            Scn currentScn = getCurrentScn();
            getMetrics().setCurrentScn(currentScn);

            upperBoundsScn = calculateUpperBounds(minLogScn, upperBoundsScn, currentScn);
            if (upperBoundsScn.isNull()) {
                LOGGER.debug("Delaying mining transaction logs by one iteration");
                pauseBetweenMiningSessions();
                continue;
            }

            // This is a small window where when archive log only mode has completely caught up to the last
            // record in the archive logs that both the lower and upper values are identical. In this use
            // case we want to pause and restart the loop waiting for a new archive log before proceeding.
            if (getConfig().isArchiveLogOnlyMode() && minLogScn.equals(upperBoundsScn)) {
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

            final Instant queryStartTime = Instant.now();
            try (ResultSet resultSet = statement.executeQuery()) {
                getMetrics().setLastDurationOfFetchQuery(Duration.between(queryStartTime, Instant.now()));

                final Instant startProcessTime = Instant.now();
                final String catalogName = getConfig().getCatalogName();

                Scn newMinCommitScn = minCommitScn;
                while (getContext().isRunning() && hasNextWithMetricsUpdate(resultSet)) {
                    getBatchMetrics().rowObserved();

                    final LogMinerEventRow event = LogMinerEventRow.fromResultSet(resultSet, catalogName);
                    processEvent(event);

                    if (EventType.COMMIT.equals(event.getEventType())) {
                        newMinCommitScn = event.getCommitScn();
                        skipCurrentTransaction = false;
                    }
                }

                getBatchMetrics().updateStreamingMetrics();

                if (getBatchMetrics().hasProcessedAnyTransactions()) {
                    getOffsetActivityMonitor().checkForStaleOffsets();
                }

                LOGGER.debug("{}.", getBatchMetrics());
                LOGGER.debug(
                        "Processed in {} ms. Lag: {}. Query Min Commit SCN: {}, New Min Commit SCN: {}, Offset SCN: {}, Offset Commit SCN: {}, Sleep: {}",
                        Duration.between(startProcessTime, Instant.now()),
                        getMetrics().getLagFromSourceInMilliseconds(),
                        minCommitScn,
                        newMinCommitScn,
                        getOffsetContext().getScn(),
                        getOffsetContext().getCommitScn(),
                        getMetrics().getSleepTimeInMilliseconds());

                return newMinCommitScn;
            }
        }
    }

    /**
     * Process a specific event.
     *
     * @param event the event, should not be {@code null}
     * @throws SQLException if a database exception occurs
     * @throws InterruptedException if the thread is interrupted
     */
    private void processEvent(LogMinerEventRow event) throws SQLException, InterruptedException {
        if (!hasEventBeenProcessed(event)) {
            if (!isEventSkipped(event)) {
                getBatchMetrics().rowProcessed();

                switch (event.getEventType()) {
                    case MISSING_SCN -> handleMissingScnEvent(event);
                    case START -> handleStartEvent(event);
                    case COMMIT -> handleCommitEvent(event);
                    case ROLLBACK -> handleRollbackEvent(event);
                    case DDL -> handleSchemaChangeEvent(event);
                    case INSERT, UPDATE, DELETE -> handleDataChangeEvent(event);
                    case REPLICATION_MARKER -> handleReplicationMarkerEvent(event);
                    case UNSUPPORTED -> handleUnsupportedEvent(event);
                    case SELECT_LOB_LOCATOR -> handleSelectLobLocatorEvent(event);
                    case LOB_WRITE -> handleLobWriteEvent(event);
                    case LOB_ERASE -> handleLobEraseEvent(event);
                    case XML_BEGIN -> handleXmlBeginEvent(event);
                    case XML_WRITE -> handleXmlWriteEvent(event);
                    case XML_END -> handleXmlEndEvent(event);
                    case EXTENDED_STRING_BEGIN -> handleExtendedStringBeginEvent(event);
                    case EXTENDED_STRING_WRITE -> handleExtendedStringWriteEvent(event);
                    case EXTENDED_STRING_END -> handleExtendedStringEndEvent(event);
                    default -> Loggings.logDebugAndTraceRecord(LOGGER, event, "Skipped event {}", event.getEventType());
                }
            }
        }
    }

    /**
     * Checks whether the event should be skipped.
     *
     * @param event the event, should not be {@code null}
     * @return true if the event is skipped, false otherwise
     */
    private boolean isEventSkipped(LogMinerEventRow event) {
        return skipCurrentTransaction || isEventIncludedInSnapshot(event) || isNonSchemaChangeEventSkipped(event);
    }

    /**
     * Checks whether the event has previously been processed.
     *
     * @param event the event, should not be {@code null}
     * @return true if the event has previously been processed, false otherwise
     */
    private boolean hasEventBeenProcessed(LogMinerEventRow event) {
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

    /**
     * Handles processing {@code START} operation events.
     *
     * @param event the event, should never be {@code null}
     */
    private void handleStartEvent(LogMinerEventRow event) {
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

        // Check whether transaction should be skipped by USERNAME field
        if (!Strings.isNullOrBlank(event.getUserName())) {
            if (getConfig().getLogMiningUsernameExcludes().contains(event.getUserName())) {
                LOGGER.debug("Skipped transaction with excluded username {}", event.getUserName());
                skipCurrentTransaction = true;
                return;
            }
        }

        // Check whether transaction should be skipped by CLIENTID field
        if (!Strings.isNullOrBlank(event.getClientId())) {
            if (getConfig().getLogMiningClientIdExcludes().contains(event.getClientId())) {
                LOGGER.debug("Skipped transaction with excluded client id {}", event.getClientId());
                skipCurrentTransaction = true;
                return;
            }
            else if (!getConfig().getLogMiningClientIdIncludes().isEmpty()) {
                if (!getConfig().getLogMiningClientIdIncludes().contains(event.getClientId())) {
                    LOGGER.debug("Skipped transaction with client id {}", event.getClientId());
                    skipCurrentTransaction = true;
                    return;
                }
            }
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
    }

    /**
     * Handles processing {@code COMMIT} operation events.
     *
     * @param event the event, should never be {@code null}
     * @throws InterruptedException if the thread is interrupted
     */
    private void handleCommitEvent(LogMinerEventRow event) throws InterruptedException {
        Loggings.logDebugAndTraceRecord(
                LOGGER,
                event,
                "Commit transaction {} at SCN {}.",
                event.getTransactionId(),
                event.getScn());

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
    }

    /**
     * Handles processing {@code ROLLBACK} operation events.
     *
     * @param event the event, should not be {@code null}
     */
    private void handleRollbackEvent(LogMinerEventRow event) {
        throw new DebeziumException("Rollback event with SCN " + event.getScn() + " found, but should not be in this mode");
    }

    /**
     * Handles processing {@code DDL} operation events.
     *
     * @param event the event, should not be {@code null}
     */
    private void handleSchemaChangeEvent(LogMinerEventRow event) {
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

    /**
     * Handles processing {@code REPLICATION_MARKER} operation events.
     *
     * @param event the event, should not be {@code null}
     */
    private void handleReplicationMarkerEvent(LogMinerEventRow event) {
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
