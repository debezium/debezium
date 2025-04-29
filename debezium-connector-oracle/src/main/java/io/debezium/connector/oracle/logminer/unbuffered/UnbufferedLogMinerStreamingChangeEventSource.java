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
import java.util.Map;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.common.annotation.Incubating;
import io.debezium.config.Configuration;
import io.debezium.connector.oracle.CommitScn;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleConnectorConfig.LogMiningStrategy;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.OraclePartition;
import io.debezium.connector.oracle.OracleSchemaChangeEventEmitter;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.AbstractLogMinerStreamingChangeEventSource;
import io.debezium.connector.oracle.logminer.LogMinerChangeRecordEmitter;
import io.debezium.connector.oracle.logminer.LogMinerStreamingChangeEventSourceMetrics;
import io.debezium.connector.oracle.logminer.TransactionCommitConsumer;
import io.debezium.connector.oracle.logminer.events.DmlEvent;
import io.debezium.connector.oracle.logminer.events.EventType;
import io.debezium.connector.oracle.logminer.events.ExtendedStringBeginEvent;
import io.debezium.connector.oracle.logminer.events.ExtendedStringWriteEvent;
import io.debezium.connector.oracle.logminer.events.LobEraseEvent;
import io.debezium.connector.oracle.logminer.events.LobWriteEvent;
import io.debezium.connector.oracle.logminer.events.LogMinerEvent;
import io.debezium.connector.oracle.logminer.events.LogMinerEventRow;
import io.debezium.connector.oracle.logminer.events.RedoSqlDmlEvent;
import io.debezium.connector.oracle.logminer.events.SelectLobLocatorEvent;
import io.debezium.connector.oracle.logminer.events.TruncateEvent;
import io.debezium.connector.oracle.logminer.events.XmlBeginEvent;
import io.debezium.connector.oracle.logminer.events.XmlEndEvent;
import io.debezium.connector.oracle.logminer.events.XmlWriteEvent;
import io.debezium.connector.oracle.logminer.parser.DmlParserException;
import io.debezium.connector.oracle.logminer.parser.ExtendedStringParser;
import io.debezium.connector.oracle.logminer.parser.LobWriteParser;
import io.debezium.connector.oracle.logminer.parser.LobWriteParser.LobWrite;
import io.debezium.connector.oracle.logminer.parser.LogMinerColumnResolverDmlParser;
import io.debezium.connector.oracle.logminer.parser.LogMinerDmlEntry;
import io.debezium.connector.oracle.logminer.parser.LogMinerDmlEntryImpl;
import io.debezium.connector.oracle.logminer.parser.LogMinerDmlParser;
import io.debezium.connector.oracle.logminer.parser.SelectLobParser;
import io.debezium.connector.oracle.logminer.parser.XmlBeginParser;
import io.debezium.connector.oracle.logminer.parser.XmlWriteParser;
import io.debezium.connector.oracle.logminer.parser.XmlWriteParser.XmlWrite;
import io.debezium.data.Envelope;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables.TableFilter;
import io.debezium.text.ParsingException;
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
    private final TableFilter tableFilter;
    private final LogMinerDmlParser dmlParser;
    private final LogMinerColumnResolverDmlParser reconstructColumnDmlParser;
    private final boolean includeSql;
    private final TransactionCommitConsumer accumulator;
    private final SelectLobParser selectLobParser;
    private final XmlBeginParser xmlBeginParser;
    private final ExtendedStringParser extendedStringParser;
    private final List<LogMinerEventRow> ddlQueue = new ArrayList<>();
    private final ResumePositionProvider resumePositionProvider;

    private boolean sequenceUnavailable = false;
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
        this.tableFilter = connectorConfig.getTableFilters().dataCollectionFilter();
        this.dmlParser = new LogMinerDmlParser(connectorConfig);
        this.reconstructColumnDmlParser = new LogMinerColumnResolverDmlParser(connectorConfig);
        this.includeSql = connectorConfig.isLogMiningIncludeRedoSql();
        this.accumulator = new TransactionCommitConsumer(this::dispatchEvent, connectorConfig, schema);
        this.selectLobParser = new SelectLobParser();
        this.xmlBeginParser = new XmlBeginParser();
        this.extendedStringParser = new ExtendedStringParser();
        this.resumePositionProvider = new ResumePositionProvider(connectorConfig, getJdbcConfiguration());
    }

    @Override
    protected void executeLogMiningStreaming(ChangeEventSource.ChangeEventSourceContext context, OraclePartition partition, OracleOffsetContext offsetContext)
            throws Exception {

        Scn upperBoundsScn = Scn.NULL;

        Scn minLogScn = getOffsetContext().getScn();
        Scn minCommitScn = getOffsetContext().getCommitScn().getMinCommittedScn();

        if (minCommitScn.isNull()) {
            minCommitScn = minLogScn;
        }

        Stopwatch watch = Stopwatch.accumulating().start();
        int miningStartAttempts = 1;

        prepareLogsForMining(false, minLogScn);

        while (context.isRunning()) {

            // Check if we should break when using archive log only mode
            if (isArchiveLogOnlyModeAndScnIsNotAvailable(context, minLogScn)) {
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
                minCommitScn = process(context, minCommitScn);

                getMetrics().setLastBatchProcessingDuration(Duration.between(batchStartTime, Instant.now()));
            }
            else {
                miningStartAttempts++;
            }

            captureJdbcSessionMemoryStatistics();

            pauseBetweenMiningSessions();

            if (context.isPaused()) {
                LOGGER.info("Streaming will now pause");

                context.streamingPaused();
                // Blocking snapshots will be based on the last commit flush
                // So we need to temporarily advance the SCN to that point.
                Scn currentOffsetScn = getOffsetContext().getScn();
                getOffsetContext().setScn(minCommitScn);
                context.waitSnapshotCompletion();

                // Now restore the old resume SCN position.
                getOffsetContext().setScn(currentOffsetScn);
                LOGGER.info("Streaming resumed");
            }
        }
    }

    @Override
    protected boolean isUsingCommittedDataOnly() {
        return true;
    }

    @Override
    protected void cleanup() {
        try {
            resumePositionProvider.close();
        }
        catch (Exception e) {
            LOGGER.warn("Failed to gracefully shutdown the resume position provider", e);
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
     * @param context the change event source context, should not be {@code null}
     * @param minCommitScn mining range lower bounds SCN, should not be {@code null}
     * @return the next iteration's lower bounds SCN, never {@code null}
     * @throws SQLException if a database exception occurred
     * @throws InterruptedException if the thread is interrupted
     */
    private Scn process(ChangeEventSource.ChangeEventSourceContext context, Scn minCommitScn)
            throws SQLException, InterruptedException {
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
                while (context.isRunning() && hasNextWithMetricsUpdate(resultSet)) {
                    getBatchMetrics().rowObserved();

                    final LogMinerEventRow event = LogMinerEventRow.fromResultSet(resultSet, catalogName);
                    if (!hasEventBeenProcessed(event)) {
                        if (!isEventSkipped(event)) {
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

                            getBatchMetrics().rowProcessed();
                        }
                    }

                    if (EventType.COMMIT.equals(event.getEventType())) {
                        newMinCommitScn = event.getCommitScn();
                        skipCurrentTransaction = false;
                    }
                }

                getBatchMetrics().updateStreamingMetrics();

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

    private boolean hasNextWithMetricsUpdate(ResultSet resultSet) throws SQLException {
        final Instant start = Instant.now();
        boolean result = false;
        try {
            if (resultSet.next()) {
                getMetrics().setLastResultSetNextDuration(Duration.between(start, Instant.now()));
                result = true;
            }

            // Reset sequence unavailability on successful read from the result set
            if (sequenceUnavailable) {
                LOGGER.debug("The previous batch's unavailable log problem has been cleared.");
                sequenceUnavailable = false;
            }
        }
        catch (SQLException e) {
            // Oracle's online redo logs can be defined with dynamic names using the instance
            // configuration property LOG_ARCHIVE_FORMAT.
            //
            // Dynamically named online redo logs can lead to ORA-00310 errors if a log switch
            // happens while the processor is iterating the LogMiner session's result set and
            // LogMiner can no longer read the next batch of records from the log.
            //
            // LogMiner only validates that there are no gaps and that the logs are available
            // when the session is first started and any change in the logs later will raise
            // these types of errors.
            //
            // Catching the ORA-00310 and treating it as the end of the result set will allow
            // the connector's outer loop to re-evaluate the log state and start a new LogMiner
            // session with the new logs. The connector will then begin streaming from where
            // it left off. If any other exception is caught here, it'll be thrown.
            if (!e.getMessage().startsWith("ORA-00310")) {
                // throw any non ORA-00310 error, old behavior
                throw e;
            }
            else if (sequenceUnavailable) {
                // If an ORA-00310 error was raised on the previous iteration and wasn't cleared
                // after re-evaluation of the log availability and the mining session, we will
                // explicitly stop the connector to avoid an infinite loop.
                LOGGER.error("The log availability error '{}' wasn't cleared, stop requested.", e.getMessage());
                throw e;
            }

            LOGGER.debug("A mined log is no longer available: {}", e.getMessage());
            LOGGER.warn("Restarting mining session after a log became unavailable.");

            // Track that we gracefully stopped due to a ORA-00310.
            // Will be used to detect an infinite loop of this error across sequential iterations
            sequenceUnavailable = true;
        }
        return result;
    }

    private boolean isEventSkipped(LogMinerEventRow event) {
        if (skipCurrentTransaction) {
            return true;
        }

        if (event.getScn().compareTo(getOffsetContext().getSnapshotScn()) < 0) {
            final Map<String, Scn> snapshotPendingTrxs = getOffsetContext().getSnapshotPendingTransactions();
            if (snapshotPendingTrxs == null || !snapshotPendingTrxs.containsKey(event.getTransactionId())) {
                LOGGER.info("Skipping event {} (SCN {}) because it is already included by the initial snapshot",
                        event.getEventType(), event.getScn());
                return true;
            }
        }

        if (event.getTableId() != null) {
            if (!EventType.DDL.equals(event.getEventType()) && !tableFilter.isIncluded(event.getTableId())) {
                if (isNonIncludedTableSkipped(event)) {
                    LOGGER.debug("Skipping change associated with table '{}' which does not match filters.", event.getTableId());
                    return true;
                }
            }
        }
        return false;
    }

    private boolean isNonIncludedTableSkipped(LogMinerEventRow row) {
        if (isUsingHybridStrategy()) {
            if (isTableLookupByObjectIdRequired(row)) {
                // Special use case where the table has been dropped and purged, and we are processing an
                // old event for the table that comes prior to the drop.
                LOGGER.trace("Found DML for dropped table in history with object-id based table name {}.", row.getTableId().table());
                final TableId tableId = getSchema().getTableIdByObjectId(row.getObjectId(), null);
                if (tableId != null) {
                    row.setTableId(tableId);
                }
                return !tableFilter.isIncluded(row.getTableId());
            }
        }
        return true;
    }

    private boolean isUsingHybridStrategy() {
        return LogMiningStrategy.HYBRID.equals(getConfig().getLogMiningStrategy());
    }

    private boolean isTableLookupByObjectIdRequired(LogMinerEventRow row) {
        final String tableName = row.getTableId().table();
        if (tableName.startsWith("OBJ# ")) {
            // This is a table that has been dropped and purged
            return true;
        }
        else if (tableName.startsWith("BIN$") && tableName.endsWith("==$0") && tableName.length() == 30) {
            // This is a table that has been dropped, but not yet purged from the RECYCLEBIN
            return true;
        }
        return false;
    }

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

    private void handleMissingScnEvent(LogMinerEventRow event) {
        Loggings.logWarningAndTraceRecord(LOGGER, event, "Event with `MISSING_SCN` operation found with SCN {}", event.getScn());
    }

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

    private void handleRollbackEvent(LogMinerEventRow event) {
        throw new DebeziumException("Rollback event with SCN " + event.getScn() + " found, but should not be in this mode");
    }

    private void handleSchemaChangeEvent(LogMinerEventRow event) throws InterruptedException {
        // todo: need to check that if a transaction is stopped mid-dispatch with a DDL, the DDL are not reprocessed

        final TableId tableId = event.getTableId();
        final boolean skipEvent = getConfig().getLogMiningSchemaChangesUsernameExcludes().stream()
                .anyMatch(name -> name.equalsIgnoreCase(event.getUserName()));

        if (skipEvent) {
            Loggings.logDebugAndTraceRecord(LOGGER, event,
                    "User '{}' is in schema change exclusions, DDL skipped.", event.getUserName());
        }
        else if (!Strings.isNullOrEmpty(event.getInfo()) && event.getInfo().startsWith("INTERNAL DDL")) {
            // Internal DDL operations are skipped.
            Loggings.logDebugAndTraceRecord(LOGGER, event, "Internal DDL skipped.");
        }
        else if (tableId != null && getSchema().storeOnlyCapturedTables() && !tableFilter.isIncluded(tableId)) {
            Loggings.logDebugAndTraceRecord(LOGGER, event,
                    "Skipped DDL associated with table '{}' because schema history only stores included tables.", tableId);
        }
        else if (getOffsetContext().getCommitScn().hasEventScnBeenHandled(event)) {
            final Scn commitScn = getOffsetContext().getCommitScn().getCommitScnForRedoThread(event.getThread());
            LOGGER.trace("DDL skipped with SCN {} <= Commit SCN {} for thread {}: {}",
                    event.getScn(), commitScn, event.getRowId(), Loggings.maybeRedactSensitiveData(event));
        }
        else if (tableId != null) {
            if (!Strings.isNullOrBlank(event.getTableName())) {
                Loggings.logDebugAndTraceRecord(LOGGER, event, "Processing DDL event with SCN {}: {}",
                        event.getScn(), event.getRedoSql());

                // In transactions that wrap DDL operations, the START and DDL event markers do not carry
                // START_SCN, START_TIMESTAMP, COMMIT_SCN, nor COMMIT_TIMESTAMP values. If we emit the
                // DDL immediately, this leads to issues with synchronization points. So instead, all
                // DDL events within the transaction scope are delayed until the COMMIT..
                ddlQueue.add(event);
            }
        }
    }

    private void handleDataChangeEvent(LogMinerEventRow event) throws SQLException, InterruptedException {
        if (Strings.isNullOrBlank(event.getRedoSql())) {
            LOGGER.trace("Data event in transaction {} with SCN {} has empty redo SQL: {}",
                    event.getTransactionId(), event.getScn(), Loggings.maybeRedactSensitiveData(event));
            return;
        }

        Loggings.logDebugAndTraceRecord(LOGGER, event, "DML: {}", event);

        // LogMiner reports LONG data types with STATUS=2 on UPDATE events, but there is no value
        // in the INFO column, and the record can be managed by the connector successfully. To
        // be backward compatible, only trigger this behavior if there is an error reason when
        // STATUS=2 in the INFO column.
        if (event.hasErrorStatus() && !Strings.isNullOrBlank(event.getInfo())) {
            if (!isUsingHybridStrategy() || (isUsingHybridStrategy() && !isTableKnown(event.getTableId()))) {
                // Fail-fast: The SQL_REDO column is not valid and cannot be parsed
                notifyEventProcessingFailure(event);
                return;
            }
        }

        getBatchMetrics().dataChangeEventObserved(event.getEventType());

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

        final Table table = getTableForDataEvent(event);
        if (table != null) {
            final LogMinerDmlEntry parsedEvent = parseDmlStatement(event, table);
            accumulator.accept(includeSql
                    ? new RedoSqlDmlEvent(event, parsedEvent, event.getRedoSql())
                    : new DmlEvent(event, parsedEvent));
        }
    }

    private void handleReplicationMarkerEvent(LogMinerEventRow event) {
        // Normally we would do something with this; however this can be safely ignored.
        LOGGER.trace("Skipped GoldenGate replication marker event: {}", Loggings.maybeRedactSensitiveData(event));
    }

    private void handleUnsupportedEvent(LogMinerEventRow event) {
        if (!Strings.isNullOrEmpty(event.getTableName())) {
            Loggings.logWarningAndTraceRecord(LOGGER, event,
                    "An unsupported operation detected for table '{}' in transaction {} with SCN {} on redo thread {}.",
                    event.getTableId(), event.getTransactionId(), event.getScn(), event.getThread());
        }
    }

    private void handleSelectLobLocatorEvent(LogMinerEventRow event) throws InterruptedException {
        if (getConfig().isLobEnabled()) {
            final TableId tableId = event.getTableId();
            final Table table = getSchema().tableFor(tableId);
            if (table == null) {
                return;
            }

            final LogMinerDmlEntry parsedEvent = selectLobParser.parse(event.getRedoSql(), table);
            parsedEvent.setObjectName(event.getTableName());
            parsedEvent.setObjectOwner(event.getTablespaceName());

            accumulator.accept(new SelectLobLocatorEvent(
                    event,
                    parsedEvent,
                    selectLobParser.getColumnName(),
                    selectLobParser.isBinary()));

            getMetrics().incrementTotalChangesCount();
        }
    }

    private void handleLobWriteEvent(LogMinerEventRow event) throws InterruptedException {
        if (getConfig().isLobEnabled() && !Strings.isNullOrEmpty(event.getRedoSql())) {
            final TableId tableId = event.getTableId();
            final Table table = getSchema().tableFor(tableId);
            if (table == null) {
                return;
            }

            final LobWrite parsedEvent = LobWriteParser.parse(event.getRedoSql());
            if (parsedEvent == null) {
                return;
            }

            accumulator.accept(new LobWriteEvent(event, parsedEvent.data(), parsedEvent.offset(), parsedEvent.length()));
        }
    }

    private void handleLobEraseEvent(LogMinerEventRow event) throws InterruptedException {
        if (getConfig().isLobEnabled()) {
            final TableId tableId = event.getTableId();
            final Table table = getSchema().tableFor(tableId);
            if (table != null) {
                accumulator.accept(new LobEraseEvent(event));
            }
        }
    }

    private void handleXmlBeginEvent(LogMinerEventRow event) throws InterruptedException {
        if (getConfig().isLobEnabled()) {
            final TableId tableId = event.getTableId();
            final Table table = getSchema().tableFor(tableId);
            if (table != null) {
                final LogMinerDmlEntry parsedEvent = xmlBeginParser.parse(event.getRedoSql(), table);
                parsedEvent.setObjectName(event.getTableName());
                parsedEvent.setObjectOwner(event.getTablespaceName());
                accumulator.accept(new XmlBeginEvent(event, parsedEvent, xmlBeginParser.getColumnName()));
            }
        }
    }

    private void handleXmlWriteEvent(LogMinerEventRow event) throws InterruptedException {
        if (getConfig().isLobEnabled()) {
            final TableId tableId = event.getTableId();
            final Table table = getSchema().tableFor(tableId);
            if (table != null) {
                final XmlWrite parsedEvent = XmlWriteParser.parse(event.getRedoSql());
                accumulator.accept(new XmlWriteEvent(event, parsedEvent.data(), parsedEvent.length()));
            }
        }
    }

    private void handleXmlEndEvent(LogMinerEventRow event) throws InterruptedException {
        if (getConfig().isLobEnabled()) {
            final TableId tableId = event.getTableId();
            final Table table = getSchema().tableFor(tableId);
            if (table != null) {
                accumulator.accept(new XmlEndEvent(event));
            }
        }
    }

    private void handleExtendedStringBeginEvent(LogMinerEventRow event) throws InterruptedException {
        if (getConfig().isLobEnabled()) {
            final TableId tableId = event.getTableId();
            final Table table = getSchema().tableFor(tableId);
            if (table != null) {
                final LogMinerDmlEntry parsedEvent = extendedStringParser.parse(event.getRedoSql(), table);
                parsedEvent.setObjectName(event.getTableName());
                parsedEvent.setObjectOwner(event.getTablespaceName());
                accumulator.accept(new ExtendedStringBeginEvent(event, parsedEvent, extendedStringParser.getColumnName()));
            }
        }
    }

    private void handleExtendedStringWriteEvent(LogMinerEventRow event) throws InterruptedException {
        if (getConfig().isLobEnabled()) {
            final TableId tableId = event.getTableId();
            final Table table = getSchema().tableFor(tableId);
            if (table != null) {

                final String data;
                try {
                    final String sql = event.getRedoSql();
                    int endIndex = sql.lastIndexOf(";");
                    if (endIndex == -1) {
                        throw new DebeziumException("Failed to find end index on 32K_WRITE operation");
                    }

                    endIndex = sql.lastIndexOf(";", endIndex - 1);
                    if (endIndex == -1) {
                        throw new DebeziumException("Failed to find end index on 32K_WRITE operation");
                    }

                    data = sql.substring(12, endIndex - 1);
                }
                catch (Exception e) {
                    throw new ParsingException(null, "Failed to parse 32K_WRITE event", e);
                }

                accumulator.accept(new ExtendedStringWriteEvent(event, data));
            }
        }
    }

    private void handleExtendedStringEndEvent(LogMinerEventRow event) {
        // no-op
    }

    private void handleTruncateEvent(LogMinerEventRow event) {
        try {
            final Table table = getTableForDataEvent(event);
            if (table != null) {
                LOGGER.debug("Dispatching TRUNCATE event for table '{}' with SCN {}", table.id(), event.getScn());

                final LogMinerDmlEntry parsedEvent = LogMinerDmlEntryImpl.forValuelessDdl();

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
        catch (SQLException | InterruptedException e) {
            LOGGER.warn("Failed to process truncate event", e);
            getMetrics().incrementWarningCount();
        }
        finally {
            if (includeSql) {
                getOffsetContext().setRedoSql("");
            }
        }
    }

    private Table getTableForDataEvent(LogMinerEventRow event) throws SQLException, InterruptedException {
        final TableId tableId = getTableIdForDataEvent(event);
        if (tableId != null) {
            final Table table = getSchema().tableFor(tableId);
            if (table != null) {
                return table;
            }
            if (tableFilter.isIncluded(tableId)) {
                return dispatchSchemaChangeEventAndGetTableForNewConfiguredTable(tableId);
            }
        }
        return null;
    }

    private TableId getTableIdForDataEvent(LogMinerEventRow event) throws SQLException {
        final TableId tableId = event.getTableId();
        if (tableId != null && isUsingHybridStrategy()) {
            if (tableId.table().startsWith("BIN$")) {
                // Object was dropped but has not been purged.
                try (OracleConnection connection = new OracleConnection(getConfig().getJdbcConfig())) {
                    return connection.prepareQueryAndMap("SELECT OWNER, ORIGINAL_NAME FROM DBA_RECYCLEBIN WHERE OBJECT_NAME=?",
                            ps -> ps.setString(1, tableId.table()),
                            rs -> {
                                if (rs.next()) {
                                    return new TableId(tableId.catalog(), rs.getString(1), rs.getString(2));
                                }
                                return tableId;
                            });
                }
            }
            else if (tableId.table().equalsIgnoreCase("UNKNOWN")) {
                // Object has been dropped and purged.
                final TableId resolvedTableId = getSchema().getTableIdByObjectId(event.getObjectId(), event.getDataObjectId());
                if (resolvedTableId != null) {
                    return resolvedTableId;
                }
                throw new DebeziumException("Failed to resolve UNKNOWN table name by object id lookup");
            }
        }
        return tableId;
    }

    private Table dispatchSchemaChangeEventAndGetTableForNewConfiguredTable(TableId tableId) throws SQLException, InterruptedException {
        LOGGER.warn("Obtaining schema for table {}, which should already be loaded.", tableId);
        // Given that the current connection is used for processing the event data, a separate connection is needed
        try (OracleConnection connection = new OracleConnection(getConfig().getJdbcConfig(), false)) {
            connection.setAutoCommit(false);
            if (isUsingPluggableDatabase()) {
                connection.setSessionToPdb(getConfig().getPdbName());
            }

            getBatchMetrics().tableMetadataQueryObserved();
            final String tableDdl = connection.getTableMetadataDdl(tableId);

            final Long objectId = connection.getTableObjectId(tableId);
            final Long dataObjectId = connection.getTableDataObjectId(tableId);

            getEventDispatcher().dispatchSchemaChangeEvent(
                    getPartition(),
                    getOffsetContext(),
                    tableId,
                    new OracleSchemaChangeEventEmitter(
                            getConfig(),
                            getPartition(),
                            getOffsetContext(),
                            tableId,
                            tableId.catalog(),
                            tableId.schema(),
                            objectId,
                            dataObjectId,
                            tableDdl,
                            getSchema(),
                            Instant.now(),
                            getMetrics(),
                            null));

            return getSchema().tableFor(tableId);
        }
        catch (OracleConnection.NonRelationalTableException e) {
            LOGGER.warn("Table {} is not a relational table and will be skipped.", tableId);
            getMetrics().incrementWarningCount();
            return null;
        }
    }

    private void notifyEventProcessingFailure(LogMinerEventRow event) {
        switch (getConfig().getEventProcessingFailureHandlingMode()) {
            case FAIL -> {
                final String message = String.format(
                        "Oracle LogMiner is unable to re-construct the SQL for '%s' event with SCN %s",
                        event.getEventType(),
                        event.getScn());

                Loggings.logErrorAndTraceRecord(LOGGER, event, message);
                throw new DebeziumException(message);
            }
            case WARN -> Loggings.logWarningAndTraceRecord(
                    LOGGER,
                    event,
                    "An {} event with SCN {} cannot be parsed. This event will be ignored and skipped.",
                    event.getEventType(),
                    event.getScn());
            default -> Loggings.logDebugAndTraceRecord(
                    LOGGER,
                    event,
                    "An {} event with SCN {} cannot be parsed. This event will be ignored and skipped.",
                    event.getEventType(),
                    event.getScn());
        }
    }

    private LogMinerDmlEntry parseDmlStatement(LogMinerEventRow event, Table table) {
        final Instant parseStartTime = Instant.now();
        try {
            final LogMinerDmlParser parser;
            if (event.hasErrorStatus() && !Strings.isNullOrBlank(event.getInfo()) && isUsingHybridStrategy()) {
                parser = reconstructColumnDmlParser;
            }
            else {
                parser = dmlParser;
            }

            final LogMinerDmlEntry parsedEvent = parser.parse(event.getRedoSql(), table);

            if (parsedEvent.getOldValues().length == 0) {
                switch (parsedEvent.getEventType()) {
                    case UPDATE, DELETE -> {
                        Loggings.logWarningAndTraceRecord(
                                LOGGER,
                                event,
                                "The DML event in transaction {} at SCN {} contained no before state",
                                event.getTransactionId(),
                                event.getScn());
                        getMetrics().incrementWarningCount();
                    }
                }
            }

            return parsedEvent;
        }
        catch (DmlParserException e) {
            throw new DmlParserException(String.format(
                    "DML statement couldn't be parsed. Please open a Jira issue with the statement '%s'.",
                    event.getRedoSql()),
                    e);
        }
        finally {
            getMetrics().setLastParseTimeDuration(Duration.between(parseStartTime, Instant.now()));
        }
    }

    private boolean isTableKnown(TableId tableId) {
        return !tableId.table().equalsIgnoreCase("UNKNOWN");
    }

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

    private void dispatchSchemaChanges() throws InterruptedException {
        for (LogMinerEventRow event : ddlQueue) {
            final TableId tableId = event.getTableId();

            // This is called from within the scope of handleCommitEvent, so the commit details
            // should have already been set, so only need to set event specifics.
            getOffsetContext().setRedoThread(event.getThread());
            getOffsetContext().setRsId(event.getRsId());
            getOffsetContext().setRowId("");
            getOffsetContext().setTransactionSequence(event.getTransactionSequence());

            getEventDispatcher().dispatchSchemaChangeEvent(
                    getPartition(),
                    getOffsetContext(),
                    tableId,
                    new OracleSchemaChangeEventEmitter(
                            getConfig(),
                            getPartition(),
                            getOffsetContext(),
                            tableId,
                            tableId.catalog(),
                            tableId.schema(),
                            event.getObjectId(),
                            // ALTER TABLE does not populate the data object id, pass object id on purpose
                            event.getObjectId(),
                            event.getRedoSql(),
                            getSchema(),
                            event.getChangeTime(),
                            getMetrics(),
                            () -> handleTruncateEvent(event)));

            if (isUsingHybridStrategy()) {
                // Remove table from the column-based parser cache
                // It will be refreshed on the next DML event that requires special parsing
                reconstructColumnDmlParser.removeTableFromCache(tableId);
            }

            getBatchMetrics().schemaChangeObserved();
        }
        ddlQueue.clear();
    }

}
