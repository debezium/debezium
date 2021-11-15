/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.processor;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.OraclePartition;
import io.debezium.connector.oracle.OracleSchemaChangeEventEmitter;
import io.debezium.connector.oracle.OracleStreamingChangeEventSourceMetrics;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.events.DmlEvent;
import io.debezium.connector.oracle.logminer.events.EventType;
import io.debezium.connector.oracle.logminer.events.LobEraseEvent;
import io.debezium.connector.oracle.logminer.events.LobWriteEvent;
import io.debezium.connector.oracle.logminer.events.LogMinerEvent;
import io.debezium.connector.oracle.logminer.events.LogMinerEventRow;
import io.debezium.connector.oracle.logminer.events.SelectLobLocatorEvent;
import io.debezium.connector.oracle.logminer.parser.DmlParserException;
import io.debezium.connector.oracle.logminer.parser.LogMinerDmlEntry;
import io.debezium.connector.oracle.logminer.parser.LogMinerDmlParser;
import io.debezium.connector.oracle.logminer.parser.SelectLobParser;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;

/**
 * An abstract implementation of {@link LogMinerEventProcessor} that all processors should extend.
 *
 * @author Chris Cranford
 */
public abstract class AbstractLogMinerEventProcessor<T extends AbstractTransaction> implements LogMinerEventProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractLogMinerEventProcessor.class);

    private final ChangeEventSourceContext context;
    private final OracleConnectorConfig connectorConfig;
    private final OracleDatabaseSchema schema;
    private final OraclePartition partition;
    private final OracleOffsetContext offsetContext;
    private final EventDispatcher<TableId> dispatcher;
    private final OracleStreamingChangeEventSourceMetrics metrics;
    private final LogMinerDmlParser dmlParser;
    private final SelectLobParser selectLobParser;

    protected final Counters counters;

    private Scn lastProcessedScn = Scn.NULL;
    private boolean sequenceUnavailable = false;

    public AbstractLogMinerEventProcessor(ChangeEventSourceContext context,
                                          OracleConnectorConfig connectorConfig,
                                          OracleDatabaseSchema schema,
                                          OraclePartition partition,
                                          OracleOffsetContext offsetContext,
                                          EventDispatcher<TableId> dispatcher,
                                          OracleStreamingChangeEventSourceMetrics metrics) {
        this.context = context;
        this.connectorConfig = connectorConfig;
        this.schema = schema;
        this.partition = partition;
        this.offsetContext = offsetContext;
        this.dispatcher = dispatcher;
        this.metrics = metrics;
        this.counters = new Counters();
        this.dmlParser = new LogMinerDmlParser();
        this.selectLobParser = new SelectLobParser();
    }

    protected OracleConnectorConfig getConfig() {
        return connectorConfig;
    }

    protected OracleDatabaseSchema getSchema() {
        return schema;
    }

    /**
     * Check whether a transaction has been recently processed through either a commit or rollback.
     *
     * @param transactionId the unique transaction id
     * @return true if the transaction has been recently processed, false otherwise
     */
    protected boolean isRecentlyProcessed(String transactionId) {
        return false;
    }

    /**
     * Checks whether the LogMinerEvent row for a schema change can be emitted.
     *
     * @param row the result set row
     * @return true if the schema change has been seen, false otherwise.
     */
    protected boolean hasSchemaChangeBeenSeen(LogMinerEventRow row) {
        return false;
    }

    /**
     * Return the last processed system change number handled by the processor.
     *
     * @return the last processed system change number, never {@code null}.
     */
    protected Scn getLastProcessedScn() {
        return lastProcessedScn;
    }

    /**
     * Returns the {@code TransactionCache} implementation.
     * @return the transaction cache, never {@code null}
     */
    protected abstract Map<String, T> getTransactionCache();

    /**
     * Creates a new transaction based on the supplied {@code START} event.
     *
     * @param row the event row, must not be {@code null}
     * @return the implementation-specific {@link Transaction} instance
     */
    protected abstract T createTransaction(LogMinerEventRow row);

    /**
     * Removes a specific transaction event by database row identifier.
     *
     * @param row the event row that contains the row identifier, must not be {@code null}
     */
    protected abstract void removeEventWithRowId(LogMinerEventRow row);

    /**
     * Returns the number of events associated with the specified transaction.
     *
     * @param transaction the transaction, must not be {@code null}
     * @return the number of events in the transaction
     */
    protected abstract int getTransactionEventCount(T transaction);

    // todo: can this be removed in favor of a single implementation?
    protected boolean isTrxIdRawValue() {
        return true;
    }

    /**
     * Processes the LogMiner results.
     *
     * @param resultSet the result set from a LogMiner query
     * @throws SQLException if a database exception occurred
     * @throws InterruptedException if the dispatcher was interrupted sending an event
     */
    protected void processResults(ResultSet resultSet) throws SQLException, InterruptedException {
        while (context.isRunning() && hasNextWithMetricsUpdate(resultSet)) {
            counters.rows++;
            processRow(LogMinerEventRow.fromResultSet(resultSet, getConfig().getCatalogName(), isTrxIdRawValue()));
        }
    }

    /**
     * Processes a single LogMinerEventRow.
     *
     * @param row the event row, must not be {@code null}
     * @throws SQLException if a database exception occurred
     * @throws InterruptedException if the dispatcher was interrupted sending an event
     */
    protected void processRow(LogMinerEventRow row) throws SQLException, InterruptedException {
        if (!row.getEventType().equals(EventType.MISSING_SCN)) {
            lastProcessedScn = row.getScn();
        }
        switch (row.getEventType()) {
            case MISSING_SCN:
                handleMissingScn(row);
            case START:
                handleStart(row);
                break;
            case COMMIT:
                handleCommit(row);
                break;
            case ROLLBACK:
                handleRollback(row);
                break;
            case DDL:
                handleSchemaChange(row);
                break;
            case SELECT_LOB_LOCATOR:
                handleSelectLobLocator(row);
                break;
            case LOB_WRITE:
                handleLobWrite(row);
                break;
            case LOB_ERASE:
                handleLobErase(row);
                break;
            case INSERT:
            case UPDATE:
            case DELETE:
                handleDataEvent(row);
                break;
        }
    }

    /**
     * Handle processing a LogMinerEventRow for a {@code MISSING_SCN} event.
     *
     * @param row the result set row
     */
    protected void handleMissingScn(LogMinerEventRow row) {
        LOGGER.warn("Missing SCN detected. {}", row);
    }

    /**
     * Handle processing a LogMinerEventRow for a {@code START} event.
     *
     * @param row the result set row
     */
    protected void handleStart(LogMinerEventRow row) {
        final String transactionId = row.getTransactionId();
        final AbstractTransaction transaction = getTransactionCache().get(transactionId);
        if (transaction == null && !isRecentlyProcessed(transactionId)) {
            getTransactionCache().put(transactionId, createTransaction(row));
            metrics.setActiveTransactions(getTransactionCache().size());
        }
        else if (transaction != null && !isRecentlyProcessed(transactionId)) {
            LOGGER.trace("Transaction {} is not yet committed and START event detected.", transactionId);
            transaction.start();
        }
    }

    /**
     * Handle processing a LogMinerEventRow for a {@code COMMIT} event.
     *
     * @param row the result set row
     * @throws InterruptedException if the event dispatcher was interrupted sending events
     */
    protected abstract void handleCommit(LogMinerEventRow row) throws InterruptedException;

    /**
     * Handle processing a LogMinerEventRow for a {@code ROLLBACK} event.
     * The default implementation is a no-op.
     *
     * @param row the result set row
     */
    protected void handleRollback(LogMinerEventRow row) {
        // no-op
    }

    /**
     * Handle processing a LogMinerEventRow for a {@code DDL} event.
     *
     * @param row the result set row
     * @throws InterruptedException if the event dispatcher is interrupted sending the event
     */
    protected void handleSchemaChange(LogMinerEventRow row) throws InterruptedException {
        if (hasSchemaChangeBeenSeen(row)) {
            LOGGER.trace("DDL: Scn {}, SQL '{}' has already been processed, skipped.", row.getScn(), row.getRedoSql());
            return;
        }

        LOGGER.trace("DDL: '{}' {}", row.getRedoSql(), row);
        if (row.getTableName() != null) {
            counters.ddlCount++;
            final TableId tableId = row.getTableId();
            dispatcher.dispatchSchemaChangeEvent(tableId,
                    new OracleSchemaChangeEventEmitter(
                            getConfig(),
                            partition,
                            offsetContext,
                            tableId,
                            tableId.catalog(),
                            tableId.schema(),
                            row.getRedoSql(),
                            getSchema(),
                            row.getChangeTime(),
                            metrics));
        }
    }

    /**
     * Handle processing a LogMinerEventRow for a {@code SEL_LOB_LOCATOR} event.
     *
     * @param row the result set row
     */
    protected void handleSelectLobLocator(LogMinerEventRow row) {
        if (!getConfig().isLobEnabled()) {
            LOGGER.trace("LOB support is disabled, SEL_LOB_LOCATOR '{}' skipped.", row.getRedoSql());
            return;
        }

        LOGGER.trace("SEL_LOB_LOCATOR: {}", row);
        final TableId tableId = row.getTableId();
        final Table table = getSchema().tableFor(tableId);
        if (table == null) {
            LOGGER.warn("SEL_LOB_LOCATOR for table '{}' is not known, skipped.", tableId);
            return;
        }

        addToTransaction(row.getTransactionId(),
                row,
                () -> {
                    final LogMinerDmlEntry dmlEntry = selectLobParser.parse(row.getRedoSql(), table);
                    dmlEntry.setObjectName(row.getTableName());
                    dmlEntry.setObjectOwner(row.getTablespaceName());

                    return new SelectLobLocatorEvent(row,
                            dmlEntry,
                            selectLobParser.getColumnName(),
                            selectLobParser.isBinary());
                });

        metrics.incrementRegisteredDmlCount();
    }

    /**
     * Handle processing a LogMinerEventRow for a {@code LOB_WRITE} event.
     *
     * @param row the result set row
     */
    protected void handleLobWrite(LogMinerEventRow row) {
        if (!getConfig().isLobEnabled()) {
            LOGGER.trace("LOB support is disabled, LOB_WRITE scn={}, tableId={} skipped", row.getScn(), row.getTableId());
            return;
        }

        LOGGER.trace("LOB_WRITE: scn={}, tableId={}, changeTime={}, transactionId={}",
                row.getScn(), row.getTableId(), row.getChangeTime(), row.getTransactionId());

        final TableId tableId = row.getTableId();
        final Table table = getSchema().tableFor(tableId);
        if (table == null) {
            LOGGER.warn("LOB_WRITE for table '{}' is not known, skipped", tableId);
            return;
        }

        if (row.getRedoSql() != null) {
            addToTransaction(row.getTransactionId(), row, () -> new LobWriteEvent(row, parseLobWriteSql(row.getRedoSql())));
        }
    }

    /**
     * Handle processing a LogMinerEventRow for a {@code LOB_ERASE} event.
     *
     * @param row the result set row
     */
    private void handleLobErase(LogMinerEventRow row) {
        if (!getConfig().isLobEnabled()) {
            LOGGER.trace("LOB support is disabled, LOB_ERASE '{}' skipped", row);
            return;
        }

        LOGGER.trace("LOB_ERASE: {}", row);
        final TableId tableId = row.getTableId();
        final Table table = getSchema().tableFor(tableId);
        if (table == null) {
            LOGGER.warn("LOB_ERASE for table '{}' is not known, skipped", tableId);
            return;
        }

        addToTransaction(row.getTransactionId(), row, () -> new LobEraseEvent(row));
    }

    /**
     * Handle processing a LogMinerEventRow for a {@code INSERT}, {@code UPDATE}, or {@code DELETE} event.
     *
     * @param row the result set row
     * @throws SQLException if a database exception occurs
     * @throws InterruptedException if the dispatch of an event is interrupted
     */
    protected void handleDataEvent(LogMinerEventRow row) throws SQLException, InterruptedException {
        if (row.getRedoSql() == null) {
            return;
        }

        LOGGER.trace("DML: {}", row);
        LOGGER.trace("\t{}", row.getRedoSql());

        counters.dmlCount++;
        switch (row.getEventType()) {
            case INSERT:
                counters.insertCount++;
                break;
            case UPDATE:
                counters.updateCount++;
                break;
            case DELETE:
                counters.deleteCount++;
                break;
        }

        final Table table = getTableForDataEvent(row);
        if (table == null) {
            return;
        }

        if (row.isRollbackFlag()) {
            // There is a use case where a constraint violation will result in a DML event being
            // written to the redo log subsequently followed by another DML event that is marked
            // with a rollback flag to indicate that the prior event should be omitted. In this
            // use case, the transaction can still be committed, so we need to manually rollback
            // the previous DML event when this use case occurs.
            removeEventWithRowId(row);
            return;
        }

        addToTransaction(row.getTransactionId(), row, () -> {
            final LogMinerDmlEntry dmlEntry = parseDmlStatement(row.getRedoSql(), table, row.getTransactionId());
            dmlEntry.setObjectName(row.getTableName());
            dmlEntry.setObjectOwner(row.getTablespaceName());
            return new DmlEvent(row, dmlEntry);
        });

        metrics.incrementRegisteredDmlCount();
    }

    /**
     * Checks to see whether the offset's {@code scn} is remaining the same across multiple mining sessions
     * while the offset's {@code commit_scn} is changing between sessions.
     *
     * @param previousOffsetScn the previous offset system change number
     * @param previousOffsetCommitScn the previous offset commit system change number
     */
    protected void warnPotentiallyStuckScn(Scn previousOffsetScn, Scn previousOffsetCommitScn) {
        if (offsetContext != null && offsetContext.getCommitScn() != null) {
            final Scn scn = offsetContext.getScn();
            final Scn commitScn = offsetContext.getCommitScn();
            if (previousOffsetScn.equals(scn) && !previousOffsetCommitScn.equals(commitScn)) {
                counters.stuckCount++;
                if (counters.stuckCount == 25) {
                    LOGGER.warn("Offset SCN {} has not changed in 25 mining session iterations. " +
                            "This indicates long running transaction(s) are active.  Commit SCN {}.",
                            previousOffsetScn,
                            previousOffsetCommitScn);
                    metrics.incrementScnFreezeCount();
                }
            }
            else {
                counters.stuckCount = 0;
            }
        }
    }

    private Table getTableForDataEvent(LogMinerEventRow row) throws SQLException, InterruptedException {
        final TableId tableId = row.getTableId();
        Table table = getSchema().tableFor(tableId);
        if (table == null) {
            if (!getConfig().getTableFilters().dataCollectionFilter().isIncluded(tableId)) {
                return null;
            }
            table = dispatchSchemaChangeEventAndGetTableForNewCapturedTable(tableId, offsetContext, dispatcher);
        }
        return table;
    }

    /**
     * Checks whether the result-set has any more data available.
     * When a new row is available, the streaming metrics is updated with the fetch timings.
     *
     * @param resultSet the result set to check if any more rows exist
     * @return true if another row exists, false otherwise
     * @throws SQLException if there was a database exception
     */
    private boolean hasNextWithMetricsUpdate(ResultSet resultSet) throws SQLException {
        Instant start = Instant.now();
        boolean result = false;
        try {
            if (resultSet.next()) {
                metrics.addCurrentResultSetNext(Duration.between(start, Instant.now()));
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
     * Add a transaction to the transaction map if allowed.
     *
     * @param transactionId the unqiue transaction id
     * @param row the LogMiner event row
     * @param eventSupplier the supplier of the event to create if the event is allowed to be added
     */
    protected abstract void addToTransaction(String transactionId, LogMinerEventRow row, Supplier<LogMinerEvent> eventSupplier);

    /**
     * Dispatch a schema change event for a new table and get the newly created relational table model.
     *
     * @param tableId the unique table identifier, must not be {@code null}
     * @param offsetContext the offset context
     * @param dispatcher the event dispatcher
     * @return the relational table model
     * @throws SQLException if a database exception occurred
     * @throws InterruptedException if the event dispatch was interrupted
     */
    private Table dispatchSchemaChangeEventAndGetTableForNewCapturedTable(TableId tableId,
                                                                          OracleOffsetContext offsetContext,
                                                                          EventDispatcher<TableId> dispatcher)
            throws SQLException, InterruptedException {
        LOGGER.info("Table '{}' is new and will now be captured.", tableId);
        offsetContext.event(tableId, Instant.now());
        dispatcher.dispatchSchemaChangeEvent(tableId,
                new OracleSchemaChangeEventEmitter(connectorConfig,
                        partition,
                        offsetContext,
                        tableId,
                        tableId.catalog(),
                        tableId.schema(),
                        getTableMetadataDdl(tableId),
                        getSchema(),
                        Instant.now(),
                        metrics));

        return getSchema().tableFor(tableId);
    }

    /**
     * Get the specified table's create DDL statement.
     *
     * @param tableId the table identifier, must not be {@code null}
     * @return the table's create DDL statement, never {@code null}
     * @throws SQLException if an exception occurred obtaining the DDL statement
     */
    private String getTableMetadataDdl(TableId tableId) throws SQLException {
        counters.tableMetadataCount++;
        LOGGER.info("Getting database metadata for table '{}'", tableId);
        // A separate connection must be used for this out-of-bands query while processing LogMiner results.
        // This should have negligible overhead since this use case should happen rarely.
        try (OracleConnection connection = new OracleConnection(connectorConfig.getJdbcConfig(), () -> getClass().getClassLoader())) {
            final String pdbName = getConfig().getPdbName();
            if (pdbName != null) {
                connection.setSessionToPdb(pdbName);
            }
            return connection.getTableMetadataDdl(tableId);
        }
    }

    /**
     * Parse a DML redo SQL statement.
     *
     * @param redoSql the redo SQL statement
     * @param table the table the SQL statement is for
     * @param transactionId the associated transaction id for the SQL statement
     * @return a parse object for the redo SQL statement
     */
    private LogMinerDmlEntry parseDmlStatement(String redoSql, Table table, String transactionId) {
        LogMinerDmlEntry dmlEntry;
        try {
            Instant parseStart = Instant.now();
            dmlEntry = dmlParser.parse(redoSql, table);
            metrics.addCurrentParseTime(Duration.between(parseStart, Instant.now()));
        }
        catch (DmlParserException e) {
            String message = "DML statement couldn't be parsed." +
                    " Please open a Jira issue with the statement '" + redoSql + "'.";
            throw new DmlParserException(message, e);
        }

        if (dmlEntry.getOldValues().length == 0) {
            if (EventType.UPDATE == dmlEntry.getEventType() || EventType.DELETE == dmlEntry.getEventType()) {
                LOGGER.warn("The DML event '{}' contained no before state.", redoSql);
                metrics.incrementWarningCount();
            }
        }

        return dmlEntry;
    }

    /**
     * Parses a {@code LOB_WRITE} operation SQL fragment.
     *
     * @param sql sql statement
     * @return the parsed statement
     * @throws DebeziumException if an unexpected SQL fragment is provided that cannot be parsed
     */
    private String parseLobWriteSql(String sql) {
        if (sql == null) {
            return null;
        }

        int start = sql.indexOf(":= '");
        if (start != -1) {
            // LOB_WRITE SQL is for a CLOB field
            int end = sql.lastIndexOf("'");
            return sql.substring(start + 4, end);
        }

        start = sql.indexOf(":= HEXTORAW");
        if (start != -1) {
            // LOB_WRITE SQL is for a BLOB field
            int end = sql.lastIndexOf("'") + 2;
            return sql.substring(start + 3, end);
        }

        throw new DebeziumException("Unable to parse unsupported LOB_WRITE SQL: " + sql);
    }

    /**
     * Gets the minimum system change number stored in the transaction cache.
     * @return the minimum system change number, never {@code null} but could be {@link Scn#NULL}.
     */
    protected abstract Scn getTransactionCacheMinimumScn();

    /**
     * Wrapper for all counter variables
     *
     */
    protected class Counters {
        public int stuckCount;
        public int dmlCount;
        public int ddlCount;
        public int insertCount;
        public int updateCount;
        public int deleteCount;
        public int commitCount;
        public int rollbackCount;
        public int tableMetadataCount;
        public long rows;

        public void reset() {
            stuckCount = 0;
            dmlCount = 0;
            ddlCount = 0;
            insertCount = 0;
            updateCount = 0;
            deleteCount = 0;
            commitCount = 0;
            rollbackCount = 0;
            tableMetadataCount = 0;
            rows = 0;
        }

        @Override
        public String toString() {
            return "Counters{" +
                    "rows=" + rows +
                    ", stuckCount=" + stuckCount +
                    ", dmlCount=" + dmlCount +
                    ", ddlCount=" + ddlCount +
                    ", insertCount=" + insertCount +
                    ", updateCount=" + updateCount +
                    ", deleteCount=" + deleteCount +
                    ", commitCount=" + commitCount +
                    ", rollbackCount=" + rollbackCount +
                    ", tableMetadataCount=" + tableMetadataCount +
                    '}';
        }
    }
}
