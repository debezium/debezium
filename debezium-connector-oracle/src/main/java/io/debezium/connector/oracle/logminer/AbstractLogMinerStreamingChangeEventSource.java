/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import java.math.BigInteger;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.annotation.VisibleForTesting;
import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnection.NonRelationalTableException;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.OraclePartition;
import io.debezium.connector.oracle.OracleSchemaChangeEventEmitter;
import io.debezium.connector.oracle.RedoThreadState;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.LogMinerStreamingChangeEventSourceMetrics.BatchMetrics;
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
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotContext;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.text.ParsingException;
import io.debezium.util.Clock;
import io.debezium.util.Loggings;
import io.debezium.util.Metronome;
import io.debezium.util.Stopwatch;
import io.debezium.util.Strings;

/**
 * An abstract implementation of the {@link StreamingChangeEventSource} for Oracle LogMiner, that is the basis
 * for both the buffered and unbuffered adapter implementations.
 *
 * @author Chris Cranford
 */
public abstract class AbstractLogMinerStreamingChangeEventSource
        implements StreamingChangeEventSource<OraclePartition, OracleOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractLogMinerStreamingChangeEventSource.class);

    private static final String NO_REDO_SQL_FOR_TEMPORARY_TABLES = "/* No SQL_REDO for temporary tables */";

    private static final int MINING_START_RETRIES = 5;
    private static final int MAXIMUM_NAME_LENGTH = 30;
    private static final int MAX_ITERATIONS_BEFORE_OFFSET_STALE = 25;
    private static final Long SMALL_REDO_LOG_WARNING = 524_288_000L;

    private final OracleConnectorConfig connectorConfig;
    private final OracleConnection jdbcConnection;
    private final EventDispatcher<OraclePartition, TableId> dispatcher;
    private final ErrorHandler errorHandler;
    private final Clock clock;
    private final OracleDatabaseSchema schema;
    private final LogMinerStreamingChangeEventSourceMetrics metrics;
    private final JdbcConfiguration jdbcConfiguration;
    private final boolean useContinuousMining;
    private final LogFileCollector logCollector;
    private final LogMinerSessionContext sessionContext;
    private final LogMinerDmlParser dmlParser;
    private final LogMinerColumnResolverDmlParser reconstructColumnDmlParser;
    private final SelectLobParser selectLobParser;
    private final ExtendedStringParser extendedStringParser;
    private final XmlBeginParser xmlBeginParser;
    private final Tables.TableFilter tableFilter;

    private boolean sequenceUnavailable = false;
    private List<LogFile> currentLogFiles;
    private List<BigInteger> currentRedoLogSequences;
    private OracleOffsetContext effectiveOffset;
    private OraclePartition partition;
    private ChangeEventSourceContext context;
    private int currentBatchSize;
    private long currentSleepTime;
    private OffsetActivityMonitor offsetActivityMonitor;

    public AbstractLogMinerStreamingChangeEventSource(OracleConnectorConfig connectorConfig,
                                                      OracleConnection jdbcConnection,
                                                      EventDispatcher<OraclePartition, TableId> dispatcher,
                                                      ErrorHandler errorHandler,
                                                      Clock clock,
                                                      OracleDatabaseSchema schema,
                                                      Configuration jdbcConfig,
                                                      LogMinerStreamingChangeEventSourceMetrics metrics) {
        this.connectorConfig = connectorConfig;
        this.jdbcConnection = jdbcConnection;
        this.dispatcher = dispatcher;
        this.errorHandler = errorHandler;
        this.clock = clock;
        this.schema = schema;
        this.metrics = metrics;
        this.jdbcConfiguration = JdbcConfiguration.adapt(jdbcConfig);
        this.useContinuousMining = connectorConfig.isLogMiningContinuousMining(jdbcConnection.getOracleVersion());
        this.logCollector = new LogFileCollector(connectorConfig, jdbcConnection);
        this.sessionContext = new LogMinerSessionContext(jdbcConnection, useContinuousMining, connectorConfig.getLogMiningStrategy());
        this.dmlParser = new LogMinerDmlParser(connectorConfig);
        this.reconstructColumnDmlParser = new LogMinerColumnResolverDmlParser(connectorConfig);
        this.selectLobParser = new SelectLobParser();
        this.extendedStringParser = new ExtendedStringParser();
        this.xmlBeginParser = new XmlBeginParser();
        this.tableFilter = connectorConfig.getTableFilters().dataCollectionFilter();

        metrics.setBatchSize(connectorConfig.getLogMiningBatchSizeDefault());
        metrics.setSleepTime(connectorConfig.getLogMiningSleepTimeDefault().toMillis());
    }

    @Override
    public void init(OracleOffsetContext offsetContext) throws InterruptedException {
        this.effectiveOffset = offsetContext == null ? emptyContext() : offsetContext;
    }

    @Override
    public OracleOffsetContext getOffsetContext() {
        return effectiveOffset;
    }

    @Override
    public void execute(ChangeEventSourceContext context, OraclePartition partition, OracleOffsetContext offsetContext)
            throws InterruptedException {
        try {
            // Set these first as this state is needed by later code calls during streaming
            this.effectiveOffset = offsetContext;
            this.partition = partition;
            this.context = context;
            this.offsetActivityMonitor = new OffsetActivityMonitor(MAX_ITERATIONS_BEFORE_OFFSET_STALE, getOffsetContext(), getMetrics());

            // perform various pre-streaming initialization steps
            prepareJdbcConnection(false);
            checkDatabaseAndTableState();
            logOnlineRedoLogSizes();

            final Scn offsetScn = getOffsetContext().getScn();
            final Scn snapshotScn = getOffsetContext().getSnapshotScn();
            final Scn firstScn = getFirstScnAvailableInLogs();

            if (offsetScn.compareTo(snapshotScn) == 0) {
                // This is the initial run of the streaming change event source.
                // We need to compute the correct start offset for mining. That is not the snapshot offset,
                // but the start offset of the oldest transaction that was still pending when the snapshot
                // was taken.
                final Scn startScn = computeStartScnForFirstMiningSession(firstScn, offsetScn, snapshotScn);
                getOffsetContext().setScn(startScn);
            }

            // Fail-fast check: makes sure the offset SCN is still available in the logs
            if (!useContinuousMining && offsetScn.compareTo(firstScn.subtract(Scn.ONE)) < 0) {
                // offsetScn is the exclusive lower bound, so must be >= (firstScn - 1)
                throw new DebeziumException("Online REDO LOG files or archive log files do not contain the offset scn " +
                        offsetScn + ". Please perform a new snapshot.");
            }

            if (isArchiveLogOnlyModeAndScnIsNotAvailable(getOffsetContext().getScn())) {
                // This implies that a connector shutdown was requested before the offset's SCN was
                // available in the archive logs to start the mining session.
                return;
            }

            executeLogMiningStreaming();
        }
        catch (Throwable throwable) {
            LOGGER.error("LogMiner session stopped due to an error.", throwable);
            metrics.incrementErrorCount();
            errorHandler.setProducerThrowable(throwable);
        }
        finally {
            LOGGER.info("Streaming metrics at shutdown: {}", metrics);
            LOGGER.info("Offsets as shutdown: {}", offsetContext);
        }
    }

    /**
     * Executes the unique mining logic for the implementation.
     *
     * @throws Exception if an error is thrown
     */
    protected abstract void executeLogMiningStreaming() throws Exception;

    /**
     * Provides an adapter-specific way to process a specific event from the event handlers.
     *
     * @param event the original LogMiner JDBC event row, should not be {@code null}
     * @param dispatchedEvent the constructed event to be dispatched, should not be {@code null}
     * @throws InterruptedException if the thread is interrupted
     */
    protected abstract void enqueueEvent(LogMinerEventRow event, LogMinerEvent dispatchedEvent) throws InterruptedException;

    /**
     * Handles processing {@code DDL} operations that perform {@code TRUNCATE} statements.
     *
     * @param event the event, should not be {@code null}
     * @throws InterruptedException if the thread is interrupted
     */
    protected abstract void handleTruncateEvent(LogMinerEventRow event) throws InterruptedException;

    protected ChangeEventSourceContext getContext() {
        return context;
    }

    protected OraclePartition getPartition() {
        return partition;
    }

    protected EventDispatcher<OraclePartition, TableId> getEventDispatcher() {
        return dispatcher;
    }

    protected OracleDatabaseSchema getSchema() {
        return schema;
    }

    protected Clock getClock() {
        return clock;
    }

    protected OracleConnectorConfig getConfig() {
        return connectorConfig;
    }

    protected JdbcConfiguration getJdbcConfiguration() {
        return jdbcConfiguration;
    }

    protected OracleConnection getConnection() {
        return jdbcConnection;
    }

    protected LogMinerStreamingChangeEventSourceMetrics getMetrics() {
        return metrics;
    }

    protected BatchMetrics getBatchMetrics() {
        return metrics.getBatchMetrics();
    }

    protected boolean isUsingPluggableDatabase() {
        return !Strings.isNullOrBlank(connectorConfig.getPdbName());
    }

    protected boolean isUsingCatalogInRedoStrategy() {
        return OracleConnectorConfig.LogMiningStrategy.CATALOG_IN_REDO.equals(connectorConfig.getLogMiningStrategy());
    }

    protected boolean isUsingHybridStrategy() {
        return OracleConnectorConfig.LogMiningStrategy.HYBRID.equals(connectorConfig.getLogMiningStrategy());
    }

    protected boolean isUsingCommittedDataOnly() {
        return false;
    }

    protected List<LogFile> getCurrentLogFiles() {
        return currentLogFiles;
    }

    protected OffsetActivityMonitor getOffsetActivityMonitor() {
        return offsetActivityMonitor;
    }

    protected void executeBlockingSnapshot() throws InterruptedException {
        LOGGER.info("Streaming will now pause");
        context.streamingPaused();
        context.waitSnapshotCompletion();
        LOGGER.info("Streaming resumed");
    }

    /**
     * Advances the JDBC {@link ResultSet} to the next row, while also tracking the time taken by advancing
     * the result set and placing that detail into the connector's metrics.
     *
     * @param resultSet the result set, should not be {@code null}
     * @return true if there is another row available, false if the result set is exhausted
     * @throws SQLException if a database exception is thrown
     */
    protected boolean hasNextWithMetricsUpdate(ResultSet resultSet) throws SQLException {
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

    /**
     * Executes the prepared statement's query and processes the result set.
     *
     * @param statement the prepared statement to execute, should not be {@code null}
     * @throws SQLException if a database error occurs
     * @throws InterruptedException if the thread is interrupted
     */
    protected void executeAndProcessQuery(PreparedStatement statement) throws SQLException, InterruptedException {
        final Instant queryStartTime = Instant.now();
        try (ResultSet resultSet = statement.executeQuery()) {
            getMetrics().setLastDurationOfFetchQuery(Duration.between(queryStartTime, Instant.now()));

            final Instant startProcessTime = Instant.now();
            final String catalogName = getConfig().getCatalogName();

            while (getContext().isRunning() && hasNextWithMetricsUpdate(resultSet)) {
                getBatchMetrics().rowObserved();

                final LogMinerEventRow event = LogMinerEventRow.fromResultSet(resultSet, catalogName);
                processEvent(event);
            }

            getBatchMetrics().updateStreamingMetrics();

            if (getBatchMetrics().hasProcessedAnyTransactions()) {
                getOffsetActivityMonitor().checkForStaleOffsets();
            }

            LOGGER.debug("{}.", getBatchMetrics());
            LOGGER.debug("Processed in {} ms. Lag {}. Active Transactions: {}. Sleep: {}. Offsets: {}",
                    Duration.between(startProcessTime, Instant.now()),
                    getMetrics().getLagFromSourceInMilliseconds(),
                    getMetrics().getNumberOfActiveTransactions(),
                    getMetrics().getSleepTimeInMilliseconds(),
                    getOffsetContext());
        }
    }

    /**
     * Execute any steps that should occur before dispatching a data change event.
     *
     * @param event the event, should not be {@code null}
     * @throws InterruptedException if the thread is interrupted
     */
    protected void executeDataChangeEventPreDispatchSteps(LogMinerEventRow event) throws InterruptedException {
        // no-op
    }

    /**
     * Checks whether the DML event can be dispatched.
     *
     * @param event the event, should not be {@code null}
     * @return true if the event can be dispatched, false otherwise
     */
    protected boolean isDispatchAllowedForDataChangeEvent(LogMinerEventRow event) {
        return true;
    }

    /**
     * Checks whether the event has previously been processed.
     *
     * @param event the event, should be {@code null}
     * @return true if the event has previously been processed, false otherwise
     */
    protected boolean hasEventBeenProcessed(LogMinerEventRow event) {
        return false;
    }

    /**
     * Check whether the specified event should be skipped.
     *
     * @param event the event, should not be {@code null}
     * @return true if the event should be skipped, false otherwise
     */
    protected boolean isEventSkipped(LogMinerEventRow event) {
        return false;
    }

    /**
     * Process a specific event.
     *
     * @param event the event, should not be {@code null}
     * @throws SQLException if a database exception occurs
     * @throws InterruptedException if the thread is interrupted
     */
    protected void processEvent(LogMinerEventRow event) throws SQLException, InterruptedException {
        if (!hasEventBeenProcessed(event)) {
            if (!isEventSkipped(event)) {
                preProcessEvent(event);

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
     * Provides a hook for an implementation to perform common processing behavior for an event
     * before the event is dispatched to its operation-specific handler.
     *
     * @param event the event, should not be {@code null}
     */
    protected void preProcessEvent(LogMinerEventRow event) {
        getBatchMetrics().rowProcessed();
    }

    /**
     * Handles processing {@code MISSING_SCN} operation events.
     *
     * @param event the event, should not be {@code null}
     */
    protected void handleMissingScnEvent(LogMinerEventRow event) {
        Loggings.logWarningAndTraceRecord(LOGGER, event, "Event with `MISSING_SCN` operation found with SCN {}", event.getScn());
    }

    /**
     * Handles processing {@code START} operation events.
     *
     * @param event the event, should not be {@code null}
     * @throws InterruptedException if the thread is interrupted
     */
    protected abstract void handleStartEvent(LogMinerEventRow event) throws InterruptedException;

    /**
     * Handles processing {@code COMMIT} operation events.
     *
     * @param event the event, should not be {@code null}
     * @throws InterruptedException if the thread is interrupted
     */
    protected abstract void handleCommitEvent(LogMinerEventRow event) throws InterruptedException;

    /**
     * Handles processing {@code ROLLBACK} operation events.
     *
     * @param event the event, should not be {@code null}
     * @throws InterruptedException if the thread is interrupted
     */
    protected abstract void handleRollbackEvent(LogMinerEventRow event) throws InterruptedException;

    /**
     * Handles processing {@code DDL} operation events.
     *
     * @param event the event, should not be {@code null}
     * @throws InterruptedException if the thread is interrupted
     */
    protected abstract void handleSchemaChangeEvent(LogMinerEventRow event) throws InterruptedException;

    /**
     * Handles processing {@code REPLICATION_MARKER} operation events.
     *
     * @param event the event, should not be {@code null}
     * @throws InterruptedException if the thread is interrupted
     */
    protected abstract void handleReplicationMarkerEvent(LogMinerEventRow event) throws InterruptedException;

    /**
     * Handles processing {@code UNSUPPORTED} operation events.
     *
     * @param event the event, should not be {@code null}
     */
    protected void handleUnsupportedEvent(LogMinerEventRow event) {
        if (!Strings.isNullOrEmpty(event.getTableName())) {
            Loggings.logWarningAndTraceRecord(LOGGER, event,
                    "An unsupported operation detected for table '{}' in transaction {} with SCN {} on redo thread {}.",
                    event.getTableId(), event.getTransactionId(), event.getScn(), event.getThread());
        }
    }

    /**
     * Handles processing {@code INSERT}, {@code UPDATE}, and {@code DELETE} operation events.
     *
     * @param event the event, should not be {@code null}
     * @throws SQLException if a database exception is thrown
     * @throws InterruptedException if the thread is interrupted
     */
    protected void handleDataChangeEvent(LogMinerEventRow event) throws SQLException, InterruptedException {
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
                notifyEventProcessingFailure(event, null);
                return;
            }
        }

        // There are some obscure corner cases where Oracle may mistakenly introduce a redo entry provided
        // by LogMiner for temporary tables, which should not happen as they're officially unsupported nor
        // are supported to be tracked by supplemental logging. Should any of these show up in the event
        // stream, they should be gracefully discarded.
        if (isNoSqlRedoForTemporaryTable(event)) {
            Loggings.logDebugAndTraceRecord(LOGGER, event, "Skipped a change for a temporary table.");
            return;
        }

        getBatchMetrics().dataChangeEventObserved(event.getEventType());

        executeDataChangeEventPreDispatchSteps(event);

        final Table table = getTableForDataEvent(event);
        if (table != null) {
            if (isDispatchAllowedForDataChangeEvent(event)) {
                dispatchDataChangeEventInternal(event, table);
            }
        }
    }

    /**
     * Handles processing {@code SELECT_LOB_LOCATOR} operation events.
     *
     * @param event the event, should not be {@code null}
     * @throws InterruptedException if the thread is interrupted
     */
    protected void handleSelectLobLocatorEvent(LogMinerEventRow event) throws InterruptedException {
        if (getConfig().isLobEnabled()) {
            final TableId tableId = event.getTableId();
            final Table table = getSchema().tableFor(tableId);
            if (table == null) {
                return;
            }

            final LogMinerDmlEntry parsedEvent = selectLobParser.parse(event.getRedoSql(), table);
            parsedEvent.setObjectName(event.getTableName());
            parsedEvent.setObjectOwner(event.getTablespaceName());

            enqueueEvent(event, new SelectLobLocatorEvent(
                    event,
                    parsedEvent,
                    selectLobParser.getColumnName(),
                    selectLobParser.isBinary()));

            getMetrics().incrementTotalChangesCount();
        }
    }

    /**
     * Handles processing {@code LOB_WRITE} operation events.
     *
     * @param event the event, should not be {@code null}
     * @throws InterruptedException if the thread is interrupted
     */
    protected void handleLobWriteEvent(LogMinerEventRow event) throws InterruptedException {
        if (getConfig().isLobEnabled() && !Strings.isNullOrEmpty(event.getRedoSql())) {
            final TableId tableId = event.getTableId();
            final Table table = getSchema().tableFor(tableId);
            if (table == null) {
                return;
            }

            final LobWrite parsedEvent = LobWriteParser.parse(event.getRedoSql());
            if (parsedEvent != null) {
                enqueueEvent(event, new LobWriteEvent(event, parsedEvent));
            }
        }
    }

    /**
     * Handles processing {@code LOB_ERASE} operation events.
     *
     * @param event the event, should not be {@code null}
     * @throws InterruptedException if the thread is interrupted
     */
    protected void handleLobEraseEvent(LogMinerEventRow event) throws InterruptedException {
        if (getConfig().isLobEnabled()) {
            final TableId tableId = event.getTableId();
            final Table table = getSchema().tableFor(tableId);
            if (table != null) {
                enqueueEvent(event, new LobEraseEvent(event));
            }
        }
    }

    /**
     * Handles processing {@code 32K_BEGIN} operation events.
     *
     * @param event the event, should not be {@code null}
     * @throws InterruptedException if the thread is interrupted
     */
    protected void handleExtendedStringBeginEvent(LogMinerEventRow event) throws InterruptedException {
        if (getConfig().isLobEnabled()) {
            final TableId tableId = event.getTableId();
            final Table table = getSchema().tableFor(tableId);
            if (table == null) {
                return;
            }

            final LogMinerDmlEntry parsedEvent = extendedStringParser.parse(event.getRedoSql(), table);
            parsedEvent.setObjectName(event.getTableName());
            parsedEvent.setObjectOwner(event.getTablespaceName());

            enqueueEvent(event, new ExtendedStringBeginEvent(event, parsedEvent, extendedStringParser.getColumnName()));

            getMetrics().incrementTotalChangesCount();
        }
    }

    /**
     * Handles processing {@code 32K_WRITE} operation events.
     *
     * @param event the event, should not be {@code null}
     * @throws InterruptedException if the thread is interrupted
     */
    protected void handleExtendedStringWriteEvent(LogMinerEventRow event) throws InterruptedException {
        if (getConfig().isLobEnabled() && !Strings.isNullOrEmpty(event.getRedoSql())) {
            final TableId tableId = event.getTableId();
            final Table table = getSchema().tableFor(tableId);
            if (table == null) {
                return;
            }

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

            enqueueEvent(event, new ExtendedStringWriteEvent(event, data));
        }
    }

    /**
     * Handles processing {@code 32K_END} operation events.
     *
     * @param event the event, should not be {@code null}
     */
    protected void handleExtendedStringEndEvent(LogMinerEventRow event) {
        // no-op
    }

    /**
     * Handles processing {@code XML_BEGIN} operation events.
     *
     * @param event the event, should not be {@code null}
     * @throws InterruptedException if the thread is interrupted
     */
    protected void handleXmlBeginEvent(LogMinerEventRow event) throws InterruptedException {
        if (getConfig().isLobEnabled()) {
            final TableId tableId = event.getTableId();
            final Table table = getSchema().tableFor(tableId);
            if (table == null) {
                return;
            }

            final LogMinerDmlEntry parsedEvent = xmlBeginParser.parse(event.getRedoSql(), table);
            parsedEvent.setObjectName(event.getTableName());
            parsedEvent.setObjectOwner(event.getTablespaceName());

            enqueueEvent(event, new XmlBeginEvent(event, parsedEvent, xmlBeginParser.getColumnName()));
        }
    }

    /**
     * Handles processing {@code XML_WRITE} operation events.
     *
     * @param event the event, should not be {@code null}
     * @throws InterruptedException if the thread is interrupted
     */
    protected void handleXmlWriteEvent(LogMinerEventRow event) throws InterruptedException {
        if (getConfig().isLobEnabled()) {
            final TableId tableId = event.getTableId();
            final Table table = getSchema().tableFor(tableId);
            if (table != null) {
                final XmlWriteParser.XmlWrite parsedEvent = XmlWriteParser.parse(event.getRedoSql());
                enqueueEvent(event, new XmlWriteEvent(event, parsedEvent.data(), parsedEvent.length()));
            }
        }
    }

    /**
     * Handles processing {@code XML_END} operation events.
     *
     * @param event the event, should not be {@code null}
     * @throws InterruptedException if the thread is interrupted
     */
    protected void handleXmlEndEvent(LogMinerEventRow event) throws InterruptedException {
        if (getConfig().isLobEnabled()) {
            final TableId tableId = event.getTableId();
            final Table table = getSchema().tableFor(tableId);
            if (table != null) {
                enqueueEvent(event, new XmlEndEvent(event));
            }
        }
    }

    /**
     * Waits for the range if it's not completely available in the archive logs yet.
     *
     * @param startScn the range starting SCN number
     * @param endScn the range ending SCN number
     * @return {@code true} if the connector loop should break, {@code false} otherwise
     * @throws SQLException if a database exception occurs
     * @throws InterruptedException if the thread is interrupted
     */
    protected boolean waitForRangeAvailabilityInArchiveLogs(Scn startScn, Scn endScn) throws SQLException, InterruptedException {
        if (endScn.isNull()) {
            // There was no prior iteration yet, sanity check to verify starting SCN
            if (isArchiveLogOnlyModeAndScnIsNotAvailable(startScn)) {
                LOGGER.error("Could not find the start SCN {} in the archive logs, stopping connector.", startScn);
                return true;
            }
        }
        else if (isNoDataProcessedInBatchAndAtEndOfArchiveLogs()) {
            if (endScn.compareTo(getMaximumArchiveLogsScn()) == 0) {
                // Prior iteration mined up to the last entry in the archive logs and no data was returned.
                return isArchiveLogOnlyModeAndScnIsNotAvailable(endScn.add(Scn.ONE));
            }
            // The endScn + 1 is now available
        }

        // Connector loop should continue to iterate
        return false;
    }

    /**
     * @return {@code true} if no data was processed, and we've reached end of the archive logs, {@code false} otherwise.
     */
    protected abstract boolean isNoDataProcessedInBatchAndAtEndOfArchiveLogs();

    /**
     * Calculates the mining session's upper boundary based on batch size limits.
     *
     * @param lowerBoundsScn the current lower boundary
     * @param previousUpperBounds the previous upper boundary
     * @param currentScn the database current write position system change number
     * @return the next iterations maximum upper boundary
     * @throws SQLException if a database exception is thrown
     */
    protected Scn calculateUpperBounds(Scn lowerBoundsScn, Scn previousUpperBounds, Scn currentScn) throws SQLException {
        final Scn maximumScn = getConfig().isArchiveLogOnlyMode() ? getMaximumArchiveLogsScn() : currentScn;

        final Scn maximumBatchScn = lowerBoundsScn.add(Scn.valueOf(metrics.getBatchSize()));
        final Scn defaultBatchSizeScn = Scn.valueOf(connectorConfig.getLogMiningBatchSizeDefault());
        final Scn maxBatchSizeScn = Scn.valueOf(connectorConfig.getLogMiningBatchSizeMax());

        // Initially set the upper bounds based on batch size
        // The following logic will alter this value as needed based on specific rules
        Scn result = maximumBatchScn;

        // Check if the batch upper bounds is greater than the current upper bounds
        // If it isn't, there is no need to update the batch size
        boolean batchUpperBoundsScnAfterCurrentScn = false;
        if (maximumBatchScn.subtract(maximumScn).compareTo(defaultBatchSizeScn) > 0) {
            // Don't update the batch size, batch upper bounds currently large enough
            decrementBatchSize();
            batchUpperBoundsScnAfterCurrentScn = true;
        }

        if (maximumScn.subtract(maximumBatchScn).compareTo(defaultBatchSizeScn) > 0) {
            // Update batch size because the database upper position is greater than the batch size
            incrementBatchSize();
        }

        if (maximumScn.compareTo(maximumBatchScn) < 0) {
            if (!batchUpperBoundsScnAfterCurrentScn) {
                incrementSleepTime();
            }
            // Batch upperbounds greater than database max possible read position.
            // Cap it at the max possible database read position
            LOGGER.debug("Batch upper bounds {} exceeds maximum read position, capping to {}.", maximumBatchScn, maximumScn);
            result = maximumScn;
        }
        else {
            if (!previousUpperBounds.isNull() && maximumBatchScn.compareTo(previousUpperBounds) <= 0) {
                // Batch size is too small, make a large leap
                // This will always add the max batch size window rather than smaller increments
                // This fits more closely to the same semantics as maximumScn, but for very large bursts, it
                // keeps the window relatively capped.
                Scn extendedUpperBounds = previousUpperBounds.add(maxBatchSizeScn);
                if (extendedUpperBounds.compareTo(maximumScn) > 0) {
                    extendedUpperBounds = maximumScn;
                }
                LOGGER.debug("Batch size upper bounds {} too small, using maximum read position {} instead.", maximumBatchScn, extendedUpperBounds);
                result = extendedUpperBounds;
            }
            else {
                decrementSleepTime();
                if (maximumBatchScn.compareTo(lowerBoundsScn) < 0) {
                    // Batch SCN calculation resulted in a value before start SCN, fallback to max read position
                    LOGGER.debug("Batch upper bounds {} is before start SCN {}, fallback to maximum read position {}.", maximumBatchScn, lowerBoundsScn, maximumScn);
                    result = maximumScn;
                }
                else if (!previousUpperBounds.isNull()) {
                    final Scn deltaScn = maximumScn.subtract(previousUpperBounds);
                    if (deltaScn.compareTo(Scn.valueOf(connectorConfig.getLogMiningScnGapDetectionGapSizeMin())) > 0) {
                        Optional<Instant> prevEndScnTimestamp = jdbcConnection.getScnToTimestamp(previousUpperBounds);
                        if (prevEndScnTimestamp.isPresent()) {
                            Optional<Instant> upperBoundsScnTimestamp = jdbcConnection.getScnToTimestamp(maximumScn);
                            if (upperBoundsScnTimestamp.isPresent()) {
                                long deltaTime = ChronoUnit.MILLIS.between(prevEndScnTimestamp.get(), upperBoundsScnTimestamp.get());
                                if (deltaTime < connectorConfig.getLogMiningScnGapDetectionTimeIntervalMaxMs()) {
                                    LOGGER.warn(
                                            "Detected possible SCN gap, using upperBounds SCN, startSCN {}, prevEndSCN {}, timestamp {}, upperBounds SCN {} timestamp {}.",
                                            lowerBoundsScn, previousUpperBounds, prevEndScnTimestamp.get(), maximumScn, upperBoundsScnTimestamp.get());
                                    result = maximumScn;
                                }
                            }
                        }
                    }
                }
            }
        }

        // If the connector is configured with maximum SCN deviation, apply the deviation time.
        // This rolls the current maximum read SCN position back based on the deviation duration.
        final Duration deviation = connectorConfig.getLogMiningMaxScnDeviation();
        if (!deviation.isZero()) {
            Optional<Scn> deviatedScn = calculateDeviatedEndScn(lowerBoundsScn, result, deviation);
            if (deviatedScn.isEmpty()) {
                return Scn.NULL;
            }
            LOGGER.debug("Adjusted upper bounds {} based on deviation to {}.", result, deviatedScn.get());
            result = deviatedScn.get();
        }

        // Retrieve the redo thread state and get the minimum flushed SCN across all open redo threads
        Scn minOpenRedoThreadLastScn = jdbcConnection.getRedoThreadState()
                .getThreads()
                .stream()
                .filter(RedoThreadState.RedoThread::isOpen)
                .map(RedoThreadState.RedoThread::getLastRedoScn)
                .min(Scn::compareTo)
                .orElse(Scn.NULL);

        // If there is a minimum flushed SCN across Open redo threads, and it is before the currently
        // assigned maximum read position, we should attempt to cap the maximum read position based
        // on the redo thread data.
        if (!minOpenRedoThreadLastScn.isNull()) {
            // LogMiner takes the range we provide and subtracts 1 from the start and adds 1 to the upper bounds
            // to create a non-inclusive range from our inclusive range. If we supply the last flushed SCN, the
            // non-inclusive range will specify an SCN beyond what is in the logs, leading to LogMiner failure.
            minOpenRedoThreadLastScn = minOpenRedoThreadLastScn.subtract(
                    Scn.valueOf(connectorConfig.getLogMiningRedoThreadScnAdjustment()));

            if (minOpenRedoThreadLastScn.compareTo(result) < 0) {
                // There are situations where on first start-up that the startScn may be higher
                // than the last flushed redo thread SCN, in which case we should delay by one
                // iteration until the startScn is before the minOpenRedoThreadLastScn
                if (minOpenRedoThreadLastScn.compareTo(lowerBoundsScn) < 0) {
                    return Scn.NULL;
                }
                LOGGER.debug("Adjusting upper bounds {} to minimum read thread flush SCN {}.", result, minOpenRedoThreadLastScn);
                result = minOpenRedoThreadLastScn;
            }
        }

        if (result.compareTo(lowerBoundsScn) <= 0) {
            // Final sanity check to prevent ORA-01281: SCN range specified is invalid
            LOGGER.debug("Final upper bounds {} matches start read position, delay required.", result);
            return Scn.NULL;
        }

        LOGGER.debug("Final upper bounds range is {}.", result);
        return result;
    }

    /**
     * Checks whether the connector is operating in archive only mode and waits for the specified scn
     * to be available in the logs.
     *
     * @param scn the system change number to check, should never be {@code null}
     * @return {@code true} if archive log only mode and scn is available, {@code false} if connector is
     *         not in archive log only mode or the connector is requesting to be shutdown
     * @throws SQLException if a database exception occurs
     * @throws InterruptedException if the thread is interrupted
     */
    protected boolean isArchiveLogOnlyModeAndScnIsNotAvailable(Scn scn) throws SQLException, InterruptedException {
        return connectorConfig.isArchiveLogOnlyMode() && !waitForScnInArchiveLogs(scn);
    }

    /**
     * Pauses the connector's iteration by the sleep time.
     *
     * @throws InterruptedException if the thread is interrupted
     */
    protected void pauseBetweenMiningSessions() throws InterruptedException {
        Duration period = Duration.ofMillis(metrics.getSleepTimeInMilliseconds());
        Metronome.sleeper(period, clock).pause();
    }

    /**
     * Prepares the JDBC connection, optionally closing and reconnecting as requested.
     *
     * @param closeAndReconnect specifies whether current connection is closed and reopened
     * @throws SQLException if a database exception occurred
     */
    protected void prepareJdbcConnection(boolean closeAndReconnect) throws SQLException {
        if (closeAndReconnect) {
            // Close and reconnect
            LOGGER.debug("Log switch or maximum session threshold detected, restarting Oracle JDBC connection.");
            jdbcConnection.close();

            if (isUsingPluggableDatabase()) {
                // Guarantee on reconnection that the connection resets to the CDB in case the user
                // configured the database.dbname or database.url to point to the PDB
                jdbcConnection.resetSessionToCdb();
            }
        }

        // Explicitly set auto-commit as disabled
        jdbcConnection.setAutoCommit(false);

        // Prepare the session's NLS configuration for streaming
        // This makes sure that specific LogMiner attributes are serialized in a consistent format
        // to minimize the various permutations needed in the value converters.
        setNlsSessionParameters();
    }

    /**
     * Updates the database time difference in the metrics.
     *
     * @throws SQLException if a database exception occurred
     */
    protected void updateDatabaseTimeDifference() throws SQLException {
        metrics.setDatabaseTimeDifference(jdbcConnection.getDatabaseSystemTime());
    }

    /**
     * Get the database's current maximum system change number.
     *
     * @return the database current maximum system change number
     * @throws SQLException if a database exception occurred
     */
    protected Scn getCurrentScn() throws SQLException {
        return jdbcConnection.getCurrentScn();
    }

    /**
     * Computes the maximum SCN across all currently known logs.
     *
     * @return the maximum SCN, never {@code null}
     */
    protected Scn getMaximumArchiveLogsScn() {
        final List<LogFile> archiveLogs = (currentLogFiles == null)
                ? Collections.emptyList()
                : currentLogFiles.stream().filter(LogFile::isArchive).toList();

        if (archiveLogs.isEmpty()) {
            throw new DebeziumException("Cannot get maximum archive log SCN as no archive logs are present.");
        }

        final Scn maximumScn = archiveLogs.stream().map(LogFile::getNextScn).max(Scn::compareTo).orElseThrow();

        LOGGER.debug("Maximum archive log SCN resolved as {}", maximumScn);
        return maximumScn;
    }

    /**
     * Check whether the mining session should be restarted.
     *
     * @param stopWatch the stop watch tracking the session's lifecycle time
     * @return {@code true} if the mining session has met or exceeded its maximum lifecycle, {@code false} otherwise
     */
    protected boolean isMiningSessionRestartRequired(Stopwatch stopWatch) {
        final Duration maximumSessionDuration = connectorConfig.getLogMiningMaximumSession().orElse(null);
        if (maximumSessionDuration != null) {
            final Duration watchTime = stopWatch.stop().durations().statistics().getTotal();
            if (watchTime.compareTo(maximumSessionDuration) >= 0) {
                LOGGER.info("LogMiner session has exceeded maximum session time of '{}', forcing restart.",
                        maximumSessionDuration);
                return true;
            }

            // Resume the watch
            stopWatch.start();
        }
        return false;
    }

    /**
     * Checks whether a redo log switch has occurred.
     * <p>
     * This method updates the {@link #currentRedoLogSequences} if a log switch has happened, along with
     * the number of log switches in the metrics.
     *
     * @return {@code true} if a log switch has occurred, {@code false} otherwise
     * @throws SQLException if a database exception occurred
     */
    protected boolean checkLogSwitchOccurredAndUpdate() throws SQLException {
        final List<BigInteger> sequences = jdbcConnection.queryAndMap(
                SqlUtils.currentRedoLogSequenceQuery(), rs -> {
                    List<BigInteger> results = new ArrayList<>();
                    while (rs.next()) {
                        results.add(new BigInteger(rs.getString(1)));
                    }
                    return results;
                });

        if (!sequences.equals(currentRedoLogSequences)) {
            LOGGER.debug("Current log sequence(s) is now {}, was {}", sequences, currentRedoLogSequences);
            currentRedoLogSequences = sequences;

            metrics.setSwitchCount(jdbcConnection.queryAndMap(
                    SqlUtils.switchHistoryQuery(connectorConfig.getArchiveLogDestinationName()),
                    rs -> rs.next() ? rs.getInt(2) : 0));

            return true;
        }
        return false;
    }

    /**
     * Adds the logs to the LogMiner session context and updates the metrics and internal state.
     *
     * @param postMiningSessionEnded {@code true} if a prior session just ended
     * @param lowerBoundsScn the lower read system change number boundary, should never be {@code null}
     * @throws SQLException if a database exception occurs
     */
    protected void prepareLogsForMining(boolean postMiningSessionEnded, Scn lowerBoundsScn) throws SQLException {
        if (!useContinuousMining) {
            sessionContext.removeAllLogFilesFromSession();
        }

        if ((!postMiningSessionEnded || !useContinuousMining) && isUsingCatalogInRedoStrategy()) {
            sessionContext.writeDataDictionaryToRedoLogs();
        }

        currentLogFiles = logCollector.getLogs(lowerBoundsScn);

        if (!useContinuousMining) {
            for (LogFile logFile : currentLogFiles) {
                sessionContext.addLogFile(logFile.getFileName());
            }

            currentRedoLogSequences = currentLogFiles.stream()
                    .filter(LogFile::isCurrent)
                    .map(LogFile::getSequence)
                    .toList();

        }

        metrics.setMinedLogFileNames(currentLogFiles.stream()
                .map(LogFile::getFileName)
                .collect(Collectors.toSet()));

        metrics.setCurrentLogFileNames(currentLogFiles.stream()
                .filter(LogFile::isCurrent)
                .map(LogFile::getFileName)
                .collect(Collectors.toSet()));

        LOGGER.trace("Current redo log filenames: {}", String.join(", ", metrics.getCurrentLogFileNames()));

        metrics.setRedoLogStatuses(jdbcConnection.queryAndMap(
                SqlUtils.redoLogStatusQuery(),
                rs -> {
                    final Map<String, String> results = new LinkedHashMap<>();
                    while (rs.next()) {
                        results.put(rs.getString(1), rs.getString(2));
                    }
                    return results;
                }));
    }

    /**
     * Starts a mining session
     *
     * @param startScn starting system change number, may be {@link Scn#NULL} to leave unset
     * @param endScn ending system change number, may be {@link Scn#NULL} to leave unset
     * @param attempts number of attempts at starting the mining session
     * @return {@code true} if the session was started successfully, {@code false} otherwise
     * @throws SQLException if a mining session failed to start
     */
    protected boolean startMiningSession(Scn startScn, Scn endScn, int attempts) throws SQLException {
        try {

            if (connectorConfig.isArchiveLogOnlyMode()) {
                final Scn newEndScn = getMinNextScnAcrossAllThreadMaxNextScnValues();
                if (!newEndScn.equals(endScn)) {
                    LOGGER.debug("Adjusted archive log only mode upper bounds from {} to {}.", endScn, newEndScn);
                    endScn = newEndScn;
                }
            }

            LOGGER.debug("Starting mining session [startScn={}, endScn={}, strategy={}, attempts={}/{}]",
                    startScn, endScn, connectorConfig.getLogMiningStrategy(), attempts, MINING_START_RETRIES);

            sessionContext.startSession(startScn, endScn, isUsingCommittedDataOnly());
            metrics.setLastMiningSessionStartDuration(sessionContext.getLastSessionStartTime());

            return true;
        }
        catch (Exception e) {
            LogMinerDatabaseStateWriter.writeLogMinerStartParameters(jdbcConnection);

            if (e instanceof RetriableLogMinerException) {
                if (attempts <= MINING_START_RETRIES) {
                    LOGGER.warn("Failed to start Oracle LogMiner session, retrying...");
                    return false;
                }

                LOGGER.error("Failed to start Oracle LogMiner after '{}' attempts.", MINING_START_RETRIES, e);
                LogMinerDatabaseStateWriter.writeLogMinerLogFailures(jdbcConnection);
            }

            LOGGER.error("Got exception when starting mining session.", e);
            // Capture the database state before throwing the exception up
            LogMinerDatabaseStateWriter.write(jdbcConnection);

            throw e;
        }
    }

    /**
     * Ends the current mining session. If the current session does not have an active session, a
     * log message will be recorded and the method acts as a no-op.
     *
     * @throws SQLException if the current mining session cannot be ended gracefully
     */
    protected void endMiningSession() throws SQLException {
        sessionContext.endMiningSession();
    }

    /**
     * Captures the Oracle JDBC session's memory statistics and updates the metrics.
     *
     * @throws SQLException if a database exception occurred
     */
    protected void captureJdbcSessionMemoryStatistics() throws SQLException {
        long sessionUserGlobalAreaMemory = jdbcConnection.getSessionStatisticByName("session uga memory");
        long sessionUserGlobalAreaMaxMemory = jdbcConnection.getSessionStatisticByName("session uga memory max");
        metrics.setUserGlobalAreaMemory(sessionUserGlobalAreaMemory, sessionUserGlobalAreaMaxMemory);

        long sessionProcessGlobalAreaMemory = jdbcConnection.getSessionStatisticByName("session pga memory");
        long sessionProcessGlobalAreaMaxMemory = jdbcConnection.getSessionStatisticByName("session pga memory max");
        metrics.setProcessGlobalAreaMemory(sessionProcessGlobalAreaMemory, sessionProcessGlobalAreaMaxMemory);

        if (LOGGER.isDebugEnabled()) {
            final DecimalFormat format = new DecimalFormat("#.##");
            LOGGER.debug("Oracle Session UGA {}MB (max = {}MB), PGA {}MB (max = {}MB)",
                    format.format(sessionUserGlobalAreaMemory / 1024.f / 1024.f),
                    format.format(sessionUserGlobalAreaMaxMemory / 1024.f / 1024.f),
                    format.format(sessionProcessGlobalAreaMemory / 1024.f / 1024.f),
                    format.format(sessionProcessGlobalAreaMaxMemory / 1024.f / 1024.f));
        }
    }

    /**
     * An internal helper method to dispatch schema change events.
     *
     * @param event the event, should not be {@code null}
     * @throws InterruptedException if the thread is interrupted
     */
    protected void dispatchSchemaChangeEventInternal(LogMinerEventRow event) throws InterruptedException {
        final TableId tableId = event.getTableId();

        getOffsetContext().setEventScn(event.getScn()); // todo: check if this breaks unbuffered
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

    /**
     * An internal helper method to dispatch data change events.
     *
     * @param event the event, should not be {@code null}
     * @param table the event's relational table, should not be {@code null}
     * @throws InterruptedException if the thread is interrupted
     */
    protected void dispatchDataChangeEventInternal(LogMinerEventRow event, Table table) throws InterruptedException {
        final LogMinerDmlEntry parsedEvent = parseDmlStatement(event, table);
        if (parsedEvent != null) {
            parsedEvent.setObjectName(event.getTableName());
            parsedEvent.setObjectOwner(event.getTablespaceName());

            enqueueEvent(event, getConfig().isLogMiningIncludeRedoSql()
                    ? new RedoSqlDmlEvent(event, parsedEvent, event.getRedoSql())
                    : new DmlEvent(event, parsedEvent));

            getMetrics().incrementTotalChangesCount();
        }
    }

    /**
     * Check whether the specific event was included as part of the initial snapshot.
     * <p>
     * This check is necessary when users use specific transaction boundary configurations and the changes pulled
     * from LogMiner go back in time to get all in-progress transactions that existed during the boundary where
     * the initial snapshot was taken. This makes sure we don't reemit an event that we already sent.
     *
     * @param event the event, should not be {@code null}
     * @return true if the event was included in the snapshot, false otherwise
     */
    protected boolean isEventIncludedInSnapshot(LogMinerEventRow event) {
        if (event.getScn().compareTo(getOffsetContext().getSnapshotScn()) < 0) {
            final Map<String, Scn> snapshotPendingTrxs = getOffsetContext().getSnapshotPendingTransactions();
            if (snapshotPendingTrxs == null || !snapshotPendingTrxs.containsKey(event.getTransactionId())) {
                LOGGER.info("Skipping event {} (SCN {}) because it is already included by the initial snapshot",
                        event.getEventType(), event.getScn());
                return true;
            }
        }
        return false;
    }

    /**
     * Checks whether any event that isn't a schema change is skipped.
     *
     * @param event the event, should not be {@code null}
     * @return true if the event should be skipped, false otherwise
     */
    protected boolean isNonSchemaChangeEventSkipped(LogMinerEventRow event) {
        final TableId tableId = event.getTableId();
        if (tableId != null && !EventType.DDL.equals(event.getEventType()) && !tableFilter.isIncluded(tableId)) {
            if (isNonIncludedTableSkipped(event)) {
                LOGGER.debug("Skipping change associated with table '{}' which does not match filters.", tableId);
                return true;
            }
        }
        return false;
    }

    /**
     * Checks whether a schema change event is skipped.
     *
     * @param event the event, should not be {@code null}
     * @return true if the event should be skipped, false otherwise
     */
    protected boolean isSchemaChangeEventSkipped(LogMinerEventRow event) {
        final TableId tableId = event.getTableId();
        final boolean skipEvent = getConfig().getLogMiningSchemaChangesUsernameExcludes().stream()
                .anyMatch(name -> name.equalsIgnoreCase(event.getUserName()));

        if (skipEvent) {
            Loggings.logDebugAndTraceRecord(LOGGER, event,
                    "User '{}' is in schema change exclusions, DDL skipped.", event.getUserName());
            return true;
        }
        else if (!Strings.isNullOrEmpty(event.getInfo()) && event.getInfo().startsWith("INTERNAL DDL")) {
            // Internal DDL operations are skipped.
            Loggings.logDebugAndTraceRecord(LOGGER, event, "Internal DDL skipped.");
            return true;
        }
        else if (tableId != null && getSchema().storeOnlyCapturedTables() && !tableFilter.isIncluded(tableId)) {
            Loggings.logDebugAndTraceRecord(LOGGER, event,
                    "Skipped DDL associated with table '{}' because schema history only stores included tables.", tableId);
            return true;
        }
        else if (getOffsetContext().getCommitScn().hasEventScnBeenHandled(event)) {
            final Scn commitScn = getOffsetContext().getCommitScn().getCommitScnForRedoThread(event.getThread());
            LOGGER.trace("DDL skipped with SCN {} <= Commit SCN {} for thread {}: {}",
                    event.getScn(), commitScn, event.getRowId(), Loggings.maybeRedactSensitiveData(event));
            return true;
        }

        return tableId == null;
    }

    /**
     * Check whether the given table identifier refers to an {@code UNKNOWN} table, which happens
     * when a redo entry's object identifier matches but its version does not match the version
     * in the Oracle data dictionary.
     *
     * @param tableId table identifier, should not be {@code null}
     * @return true if the table is unknown, false otherwise
     */
    protected boolean isTableKnown(TableId tableId) {
        return !tableId.table().equalsIgnoreCase("UNKNOWN");
    }

    /**
     * Parses a DML event, which represents an insert, update, or delete for a given table.
     *
     * @param event the event, should not be {@code null}
     * @param table the table the event is for, should not be {@code null}
     * @return the parsed entry or {@code null} if the parse fails
     */
    protected LogMinerDmlEntry parseDmlStatement(LogMinerEventRow event, Table table) {
        final Instant parseStartTime = Instant.now();
        try {
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
        catch (DmlParserException e) {
            notifyEventProcessingFailure(event, e);
            return null;
        }
    }

    /**
     * Parse a TRUNCATE event.
     *
     * @param event the event, should not be {@code null}
     * @return the parsed entry, never {@code null}
     */
    protected LogMinerDmlEntry parseTruncateEvent(LogMinerEventRow event) {
        final LogMinerDmlEntry parsedEvent = LogMinerDmlEntryImpl.forValuelessDdl();
        parsedEvent.setObjectName(event.getTableName());
        parsedEvent.setObjectOwner(event.getTablespaceName());

        return parsedEvent;
    }

    /**
     * Handle common notification behavior when an event processing failure occurs.
     *
     * @param event the event, should not be {@code null}
     * @param cause the exception that triggers the notification handler, can be {@code null}
     */
    protected void notifyEventProcessingFailure(LogMinerEventRow event, RuntimeException cause) {
        switch (getConfig().getEventProcessingFailureHandlingMode()) {
            case FAIL -> {
                final String message = String.format(
                        "Oracle LogMiner is unable to re-construct the SQL for '%s' event with SCN %s",
                        event.getEventType(),
                        event.getScn());

                Loggings.logErrorAndTraceRecord(LOGGER, event, message);
                throw (cause != null ? cause : new DebeziumException(message));
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

    /**
     * Resolve the relational table for a DML data event.
     *
     * @param event the event, should not be {@code null}
     * @return the resolved relational table, can be {@code null}
     * @throws SQLException if a database exception occurred
     * @throws InterruptedException if the thread is interrupted while dispatching a schema change event
     */
    protected Table getTableForDataEvent(LogMinerEventRow event) throws SQLException, InterruptedException {
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

    /**
     * Resolves the relational table identifier for a DML data event.
     *
     * @param event the event, should not be {@code null}
     * @return the resolved relational table identifier, can be {@code null}
     * @throws SQLException if a database exception occurred
     */
    protected TableId getTableIdForDataEvent(LogMinerEventRow event) throws SQLException {
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
            else if (!isTableKnown(tableId)) {
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

    /**
     * Check whether the event's non-included table should be skipped.
     *
     * @param event the event, should not be {@code null}
     * @return true if the event should be skipped, false otherwise
     */
    protected boolean isNonIncludedTableSkipped(LogMinerEventRow event) {
        if (isUsingHybridStrategy()) {
            if (isTableLookupByObjectIdRequired(event)) {
                // Special use case where the table has been dropped and purged, and we are processing an
                // old event for the table that comes prior to the drop.
                LOGGER.trace("Found DML for dropped table in history with object-id based table name {}.", event.getTableId().table());
                final TableId tableId = getSchema().getTableIdByObjectId(event.getObjectId(), null);
                if (tableId != null) {
                    event.setTableId(tableId);
                }
                return !tableFilter.isIncluded(event.getTableId());
            }
        }
        return true;
    }

    /**
     * Check whether an event with the given username should be skipped.
     *
     * @param userName the username, can be {@code null} or {@code empty}
     * @return true to skip, false otherwise
     */
    protected boolean isUserNameSkipped(String userName) {
        if (!Strings.isNullOrEmpty(userName)) {
            final Set<String> userNameExcludes = connectorConfig.getLogMiningUsernameExcludes();
            final Set<String> userNameIncludes = connectorConfig.getLogMiningUsernameIncludes();
            if (userNameExcludes.contains(userName)) {
                LOGGER.debug("Skipped transaction with excluded username {}", userName);
                return true;
            }
            else if (!userNameIncludes.isEmpty() && !userNameIncludes.contains(userName)) {
                LOGGER.debug("Skipped transaction with username {}", userName);
                return true;
            }
        }
        return false;
    }

    /**
     * Check whether an event with the given client identifier should be skipped.
     *
     * @param clientId the client identifier, can be {@code null} or {@code empty}
     * @return true to skip, false otherwise
     */
    protected boolean isClientIdSkipped(String clientId) {
        if (!Strings.isNullOrEmpty(clientId)) {
            final Set<String> clientIdExcludes = connectorConfig.getLogMiningClientIdExcludes();
            final Set<String> clientIdIncludes = connectorConfig.getLogMiningClientIdIncludes();
            if (clientIdExcludes.contains(clientId)) {
                LOGGER.debug("Skipped transaction with excluded client id {}", clientId);
                return true;
            }
            else if (!clientIdIncludes.isEmpty() && !clientIdIncludes.contains(clientId)) {
                LOGGER.debug("Skipped transaction with client id {}", clientId);
                return true;
            }
        }
        return false;
    }

    /**
     * Update common metrics on {@code COMMIT} events.
     *
     * @param event the event, should not be {@code null}
     * @param commitDuration the duration of the commit operation, should not be {@code null}
     */
    protected void updateCommitMetrics(LogMinerEventRow event, Duration commitDuration) {
        getMetrics().incrementCommittedTransactionCount();
        getMetrics().setCommitScn(event.getScn());
        getMetrics().setOffsetScn(getOffsetContext().getScn());
        getMetrics().setLastCommitDuration(commitDuration);
    }

    /**
     * Check whether the event's table represents a dropped table that has been purged or is in the database's
     * object recycle-bin.
     *
     * @param event the event, should not be {@code null}
     * @return true if the table lookup should be done by object identifier, false otherwise
     */
    private boolean isTableLookupByObjectIdRequired(LogMinerEventRow event) {
        final String tableName = event.getTableId().table();
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

    /**
     * Dispatches a schema change event for a newly observed table that matches the filter conditions but
     * does not yet have a relational table in the internal schema topic.
     *
     * @param tableId the table identifier, should not be {@code null}
     * @return the relational table model, may be {@code null}
     * @throws SQLException if a database exception is thrown
     * @throws InterruptedException if the thread is interrupted
     */
    @VisibleForTesting
    protected Table dispatchSchemaChangeEventAndGetTableForNewConfiguredTable(TableId tableId) throws SQLException, InterruptedException {
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
        catch (NonRelationalTableException e) {
            LOGGER.warn("{} The event will be skipped.", e.getMessage());
            getMetrics().incrementWarningCount();
            return null;
        }
    }

    /**
     * Sets the NLS parameters for the mining session.
     *
     * @throws SQLException if a database exception occurred
     */
    private void setNlsSessionParameters() throws SQLException {
        final String NLS_SESSION_PARAMETERS = "ALTER SESSION SET "
                + "  NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS'"
                + "  NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF9'"
                + "  NLS_TIMESTAMP_TZ_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF9 TZH:TZM'"
                + "  NLS_NUMERIC_CHARACTERS = '.,'";
        jdbcConnection.executeWithoutCommitting(NLS_SESSION_PARAMETERS);

        // This is necessary so that TIMESTAMP WITH LOCAL TIME ZONE is returned in UTC
        jdbcConnection.executeWithoutCommitting("ALTER SESSION SET TIME_ZONE = '00:00'");
    }

    /**
     * Checks and validates the database's supplemental logging configuration as well as the lengths of the
     * table and column names that are part of the database schema.
     *
     * @throws SQLException if a database exception occurred
     */
    private void checkDatabaseAndTableState() throws SQLException {
        final Instant start = Instant.now();
        LOGGER.trace("Checking database and table state, this may take time depending on the size of your schema.");
        try {
            if (isUsingPluggableDatabase()) {
                jdbcConnection.setSessionToPdb(connectorConfig.getPdbName());
            }

            // Check if ALL supplemental logging is enabled at the database
            if (!isDatabaseAllSupplementalLoggingEnabled()) {
                // Check if MIN supplemental logging is enabled at the database
                if (!isDatabaseMinSupplementalLoggingEnabled()) {
                    throw new DebeziumException("Supplemental logging not properly configured. "
                            + "Use: ALTER DATABASE ADD SUPPLEMENTAL LOG DATA");
                }

                // Check if ALL COLUMNS supplemental logging is enabled for each captured table
                for (TableId tableId : schema.tableIds()) {
                    if (!jdbcConnection.isTableExists(tableId)) {
                        LOGGER.warn("Database table '{}' no longer exists, supplemental log check skipped", tableId);
                    }
                    else if (!isTableAllColumnsSupplementalLoggingEnabled(tableId)) {
                        LOGGER.warn("Database table '{}' not configured with supplemental logging \"(ALL) COLUMNS\"; " +
                                "only explicitly changed columns will be captured. " +
                                "Use: ALTER TABLE {}.{} ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS", tableId, tableId.schema(), tableId.table());
                    }
                    final Table table = schema.tableFor(tableId);
                    if (table == null) {
                        // This should never happen; however in the event something would cause it we can
                        // at least get the table identifier thrown in the error to debug from rather
                        // than an erroneous NPE
                        throw new DebeziumException("Unable to find table in relational model: " + tableId);
                    }
                    checkTableColumnNameLengths(table);
                }
            }
            else {
                // ALL supplemental logging is enabled, now check table/column lengths
                for (TableId tableId : schema.tableIds()) {
                    final Table table = schema.tableFor(tableId);
                    if (table == null) {
                        // This should never happen; however in the event something would cause it we can
                        // at least get the table identifier thrown in the error to debug from rather
                        // than an erroneous NPE
                        throw new DebeziumException("Unable to find table in relational model: " + tableId);
                    }
                    checkTableColumnNameLengths(table);
                }
            }
        }
        finally {
            if (isUsingPluggableDatabase()) {
                jdbcConnection.resetSessionToCdb();
            }
        }
        LOGGER.trace("Database and table state check finished after {} ms", Duration.between(start, Instant.now()).toMillis());
    }

    /**
     * Returns whether the database is configured with ALL supplemental logging.
     *
     * @return true if all supplemental logging is enabled, false otherwise
     * @throws SQLException if a database exception occurred
     */
    private boolean isDatabaseAllSupplementalLoggingEnabled() throws SQLException {
        return jdbcConnection.queryAndMap(SqlUtils.databaseSupplementalLoggingAllCheckQuery(), rs -> {
            while (rs.next()) {
                if ("YES".equalsIgnoreCase(rs.getString(2))) {
                    return true;
                }
            }
            return false;
        });
    }

    /**
     * Returns whether the database is configured with MIN supplemental logging.
     *
     * @return true if min supplemental logging is enabled, false otherwise
     * @throws SQLException if a database exception occurred
     */
    private boolean isDatabaseMinSupplementalLoggingEnabled() throws SQLException {
        return jdbcConnection.queryAndMap(SqlUtils.databaseSupplementalLoggingMinCheckQuery(), rs -> {
            while (rs.next()) {
                if ("YES".equalsIgnoreCase(rs.getString(2))) {
                    return true;
                }
            }
            return false;
        });
    }

    /**
     * Return whether the table is configured with ALL COLUMN supplemental logging.
     *
     * @param tableId table identifier, must not be {@code null}
     * @return true if all column supplemental logging is enabled, false otherwise
     * @throws SQLException if a database exception occurred
     */
    private boolean isTableAllColumnsSupplementalLoggingEnabled(TableId tableId) throws SQLException {
        // A table can be defined with multiple logging groups, hence why this check needs to iterate
        // multiple returned rows to see whether ALL_COLUMN_LOGGING is part of the set.
        return jdbcConnection.prepareQueryAndMap(SqlUtils.tableSupplementalLoggingCheckQuery(),
                ps -> {
                    ps.setString(1, tableId.schema());
                    ps.setString(2, tableId.table());
                }, rs -> {
                    while (rs.next()) {
                        if ("ALL COLUMN LOGGING".equals(rs.getString(2))) {
                            return true;
                        }
                    }
                    return false;
                });
    }

    /**
     * Examines the table and column names and logs a warning if any name exceeds {@link #MAXIMUM_NAME_LENGTH}.
     *
     * @param table the table, should not be {@code null}
     */
    private void checkTableColumnNameLengths(Table table) {
        if (table.id().table().length() > MAXIMUM_NAME_LENGTH) {
            LOGGER.warn("Table '{}' won't be captured by Oracle LogMiner because its name exceeds {} characters.",
                    table.id().table(), MAXIMUM_NAME_LENGTH);
        }
        for (Column column : table.columns()) {
            if (column.name().length() > MAXIMUM_NAME_LENGTH) {
                LOGGER.warn("Table '{}' won't be captured by Oracle LogMiner because column '{}' exceeds {} characters.",
                        table.id().table(), column.name(), MAXIMUM_NAME_LENGTH);
            }
        }
    }

    /**
     * Logs the online redo log groups and sizes to the connector logs at streaming startup.
     *
     * @throws SQLException if a database exception occurs
     */
    private void logOnlineRedoLogSizes() throws SQLException {
        jdbcConnection.query("SELECT GROUP#, BYTES FROM V$LOG ORDER BY 1", rs -> {
            LOGGER.info("Redo Log Group Sizes:");
            boolean potentiallySmallLogs = false;
            while (rs.next()) {
                long logSize = rs.getLong(2);
                if (logSize < SMALL_REDO_LOG_WARNING) {
                    potentiallySmallLogs = true;
                }
                LOGGER.info("\tGroup #{}: {} bytes", rs.getInt(1), logSize);
            }

            if (isUsingCatalogInRedoStrategy() && potentiallySmallLogs) {
                LOGGER.warn("Redo logs may be sized too small using the default mining strategy, " +
                        "consider increasing redo log sizes to a minimum of 500MB.");
            }
        });
    }

    /**
     * Computes the start SCN for the first mining session.
     * <p>
     * Normally, this would be the snapshot SCN, but if there were pending transactions at the time
     * the snapshot was taken, we'd miss the events in those transactions that have an SCN smaller
     * than the snapshot SCN.
     *
     * @param firstScn the oldest SCN still available in the REDO logs
     * @param offsetScn the SCN from the offsets
     * @param snapshotScn the SCN used to take the snapshot
     */
    private Scn computeStartScnForFirstMiningSession(Scn firstScn, Scn offsetScn, Scn snapshotScn) {
        // This is the initial run of the streaming change event source.
        // We need to compute the correct start offset for mining. That is not the snapshot offset,
        // but the start offset of the oldest transaction that was still pending when the snapshot
        // was taken.
        Map<String, Scn> snapshotPendingTransactions = getOffsetContext().getSnapshotPendingTransactions();
        if (snapshotPendingTransactions == null || snapshotPendingTransactions.isEmpty()) {
            // no pending transactions, we can start mining from the snapshot SCN
            return snapshotScn;
        }
        else {
            // find the oldest transaction we can still fully process, and start from there.
            Scn minScn = snapshotScn;
            for (Map.Entry<String, Scn> entry : snapshotPendingTransactions.entrySet()) {
                String transactionId = entry.getKey();
                Scn scn = entry.getValue();

                LOGGER.info("Transaction {} was pending across snapshot boundary. Start SCN = {}, snapshot SCN = {}",
                        transactionId, scn, offsetScn);

                if (scn.compareTo(firstScn) < 0) {
                    LOGGER.warn("Transaction {} was still ongoing while snapshot was taken, but is no longer completely " +
                            "recorded in the archive logs. Events will be lost. Oldest SCN in logs = {}, TX start SCN = {}",
                            transactionId, firstScn, scn);
                    minScn = firstScn;
                }
                else if (scn.compareTo(minScn) < 0) {
                    minScn = scn;
                }
            }

            // Make sure the commit SCN is at least the snapshot SCN - 1.
            // This ensures we'll never emit events for transactions that were complete before the snapshot was
            // taken.
            if (getOffsetContext().getCommitScn().compareTo(snapshotScn) < 0) {
                LOGGER.info("Setting commit SCN to {} (snapshot SCN - 1) to ensure we don't double-emit events from pre-snapshot transactions.",
                        snapshotScn.subtract(Scn.ONE));
                getOffsetContext().getCommitScn().setCommitScnOnAllThreads(snapshotScn.subtract(Scn.ONE));
            }

            // set start SCN to minScn
            if (minScn.compareTo(offsetScn) <= 0) {
                LOGGER.info("Resetting start SCN from {} (snapshot SCN) to {} (start of oldest complete pending transaction)", offsetScn, minScn);
                return minScn.subtract(Scn.ONE);
            }
        }
        return offsetScn;
    }

    /**
     * Get the first system change number in all available database transaction logs.
     *
     * @return the first system change number, never {@code null}
     * @throws SQLException if no system change number was found
     */
    private Scn getFirstScnAvailableInLogs() throws SQLException {
        return jdbcConnection.getFirstScnInLogs(
                connectorConfig.getArchiveLogRetention(),
                connectorConfig.getArchiveLogDestinationName())
                .orElseThrow(() -> new DebeziumException("Failed to calculate oldest SCN available in logs"));
    }

    /**
     * Waits for the system change number to exist in the archive logs.
     *
     * @param scn the system change number
     * @return {@code true} if the code should continue, {@code false} if the code should end.
     * @throws SQLException if a database exception occurred
     * @throws InterruptedException if the pause between checks is interrupted
     */
    private boolean waitForScnInArchiveLogs(Scn scn) throws SQLException, InterruptedException {
        boolean showMessage = true;
        while (context.isRunning() && !isScnInArchiveLogs(scn)) {
            if (showMessage) {
                LOGGER.warn("SCN {} is not yet in archive logs, waiting for log switch.", scn);
                showMessage = false;
            }
            Metronome.sleeper(connectorConfig.getArchiveLogOnlyScnPollTime(), getClock()).pause();
        }

        // If the loop broke because the context is no longer running, shutdown is requested
        if (!context.isRunning()) {
            return false;
        }

        if (!showMessage) {
            LOGGER.info("SCN {} is now available in archive logs, log mining session resumed.", scn);
        }

        return true;
    }

    /**
     * Returns whether the system change number is in the archive logs.
     *
     * @param scn the system change number to check, should not be {@code null}
     * @return {@code true} if the starting system change number is in the archive logs; {@code false} otherwise.
     * @throws SQLException if a database exception occurred
     */
    private boolean isScnInArchiveLogs(Scn scn) throws SQLException {
        try {
            // Purposely use getLogsForOffsetScn as we want to skip consistency here
            return logCollector.getLogsForOffsetScn(scn).stream()
                    .anyMatch(log -> log.isScnInLogFileRange(scn) && log.isArchive());
        }
        catch (LogFileNotFoundException e) {
            // It is safe to ignore this error.
            // This identifies that the check should simply be re-evaluated after the pause.
            return false;
        }
    }

    /**
     * Calculates the deviated end scn based on the scn range and deviation.
     *
     * @param lowerboundsScn the mining range's lower bounds
     * @param upperboundsScn the mining range's upper bounds
     * @param deviation the time deviation
     * @return an optional that contains the deviated scn or empty if the operation should be performed again
     */
    private Optional<Scn> calculateDeviatedEndScn(Scn lowerboundsScn, Scn upperboundsScn, Duration deviation) {
        if (connectorConfig.isArchiveLogOnlyMode()) {
            // When archive-only mode is enabled, deviation should be ignored, even when enabled.
            return Optional.of(upperboundsScn);
        }

        final Optional<Scn> calculatedDeviatedEndScn = getDeviatedMaxScn(upperboundsScn, deviation);
        if (calculatedDeviatedEndScn.isEmpty() || calculatedDeviatedEndScn.get().isNull()) {
            // This happens only if the deviation calculation is outside the flashback/undo area or an exception was thrown.
            // In this case we have no choice but to use the upper bounds as a fallback.
            LOGGER.warn("Mining session end SCN deviation calculation is outside undo space, using upperbounds {}. If this continues, " +
                    "consider lowering the value of the '{}' configuration property.", upperboundsScn,
                    OracleConnectorConfig.LOG_MINING_MAX_SCN_DEVIATION_MS.name());
            return Optional.of(upperboundsScn);
        }
        else if (calculatedDeviatedEndScn.get().compareTo(lowerboundsScn) <= 0) {
            // This should also force the outer loop to recall this method again.
            LOGGER.debug("Mining session end SCN deviation as {}, outside of mining range, recalculating.", calculatedDeviatedEndScn.get());
            return Optional.empty();
        }
        else {
            // Calculated SCN is after lower bounds and within flashback/undo area, safe to return.
            return calculatedDeviatedEndScn;
        }
    }

    /**
     * Uses the provided Upperbound SCN and deviation to calculate an SCN that happened in the past at a
     * time based on Oracle's {@code TIMESTAMP_TO_SCN} and {@code SCN_TO_TIMESTAMP} functions.
     *
     * @param upperboundsScn the upper bound system change number, should not be {@code null}
     * @param deviation the time deviation to be applied, should not be {@code null}
     * @return the newly calculated Scn
     */
    private Optional<Scn> getDeviatedMaxScn(Scn upperboundsScn, Duration deviation) {
        try {
            final Scn currentScn = jdbcConnection.getCurrentScn();
            final Optional<Instant> currentInstant = jdbcConnection.getScnToTimestamp(currentScn);
            final Optional<Instant> upperInstant = jdbcConnection.getScnToTimestamp(upperboundsScn);
            if (currentInstant.isPresent() && upperInstant.isPresent()) {
                // If the upper bounds satisfies the deviation time
                if (Duration.between(upperInstant.get(), currentInstant.get()).compareTo(deviation) >= 0) {
                    LOGGER.trace("Upper bounds {} is within deviation period, using it.", upperboundsScn);
                    return Optional.of(upperboundsScn);
                }
            }
            return Optional.of(jdbcConnection.getScnAdjustedByTime(upperboundsScn, deviation));
        }
        catch (SQLException e) {
            LOGGER.warn("Failed to calculate deviated max SCN value from {}.", upperboundsScn);
            return Optional.empty();
        }
    }

    /**
     * Increments the mining batch size.
     */
    private void incrementBatchSize() {
        int batchSizeMax = connectorConfig.getLogMiningBatchSizeMax();
        int batchSizeIncrement = connectorConfig.getLogMiningBatchSizeIncrement();
        if (currentBatchSize < batchSizeMax) {
            final int previousBatchSize = currentBatchSize;
            currentBatchSize = Math.min(currentBatchSize + batchSizeIncrement, batchSizeMax);
            metrics.setBatchSize(currentBatchSize);
            if (previousBatchSize != currentBatchSize && currentBatchSize == batchSizeMax) {
                LOGGER.debug("The connector is now using the maximum batch size {}.", currentBatchSize);
            }
            else if (previousBatchSize != currentBatchSize) {
                LOGGER.debug("Updated batch size window, using batch size {}", currentBatchSize);
            }
        }
    }

    /**
     * Increments the sleep time to wait in between mining iterations.
     */
    private void incrementSleepTime() {
        long sleepTimeMax = connectorConfig.getLogMiningSleepTimeMax().toMillis();
        long sleepTimeIncrement = connectorConfig.getLogMiningSleepTimeIncrement().toMillis();
        if (currentSleepTime < sleepTimeMax) {
            final long previousSleepTime = currentSleepTime;
            currentSleepTime = Math.min(currentSleepTime + sleepTimeIncrement, sleepTimeMax);
            metrics.setSleepTime(currentSleepTime);
            if (previousSleepTime != currentSleepTime) {
                if (currentSleepTime == sleepTimeMax) {
                    LOGGER.debug("The connector is now using the maximum sleep time {}.", currentSleepTime);
                }
                else {
                    LOGGER.debug("Update sleep time, using {}", currentBatchSize);
                }
            }
        }
    }

    /**
     * Decrements the mining batch size.
     */
    private void decrementBatchSize() {
        int batchSizeMin = connectorConfig.getLogMiningBatchSizeMin();
        int batchSizeIncrement = connectorConfig.getLogMiningBatchSizeIncrement();
        if (currentBatchSize > batchSizeMin) {
            final int previousBatchSize = currentBatchSize;
            currentBatchSize = Math.max(currentBatchSize - batchSizeIncrement, batchSizeMin);
            metrics.setBatchSize(currentBatchSize);
            if (previousBatchSize != currentBatchSize && currentBatchSize == batchSizeMin) {
                LOGGER.debug("The connector is now using the minimum batch size {}.", currentBatchSize);
            }
            else if (previousBatchSize != currentBatchSize) {
                LOGGER.debug("Updated batch size window, using batch size {}", currentBatchSize);
            }
        }
    }

    /**
     * Decrements the sleep time to wait in between mining iterations.
     */
    private void decrementSleepTime() {
        long sleepTimeMin = connectorConfig.getLogMiningSleepTimeMin().toMillis();
        long sleepTimeIncrement = connectorConfig.getLogMiningSleepTimeIncrement().toMillis();
        if (currentSleepTime > sleepTimeMin) {
            final long previousSleepTime = currentSleepTime;
            currentSleepTime = Math.max(currentSleepTime - sleepTimeIncrement, sleepTimeMin);
            metrics.setSleepTime(currentSleepTime);
            if (previousSleepTime != currentSleepTime) {
                if (currentSleepTime == sleepTimeMin) {
                    LOGGER.debug("The connector is now using the minimum sleep time {}.", currentSleepTime);
                }
                else {
                    LOGGER.debug("Update sleep time, using {}", currentBatchSize);
                }
            }
        }
    }

    private boolean isNoSqlRedoForTemporaryTable(LogMinerEventRow event) {
        return NO_REDO_SQL_FOR_TEMPORARY_TABLES.equals(event.getRedoSql());
    }

    private OracleOffsetContext emptyContext() {
        return OracleOffsetContext.create().logicalName(connectorConfig)
                .snapshotPendingTransactions(Collections.emptyMap())
                .transactionContext(new TransactionContext())
                .incrementalSnapshotContext(new SignalBasedIncrementalSnapshotContext<>()).build();
    }

    private Scn getMinNextScnAcrossAllThreadMaxNextScnValues() {
        return getCurrentLogFiles().stream()
                .filter(LogFile::isArchive)
                .collect(Collectors.groupingBy(
                        LogFile::getThread,
                        Collectors.mapping(LogFile::getNextScn, Collectors.maxBy(Scn::compareTo))))
                .values()
                .stream()
                .flatMap(Optional::stream)
                .min(Scn::compareTo)
                .orElseThrow(() -> new DebeziumException("Failed to resolve archive logs upper bounds"));
    }
}
