/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.Notification;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.ChangeTable;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.schema.SchemaChangeEvent.SchemaChangeEventType;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.util.Clock;
import io.debezium.util.ElapsedTimeStrategy;

/**
 * <p>A {@link StreamingChangeEventSource} based on SQL Server change data capture functionality.
 * A main loop polls database DDL change and change data tables and turns them into change events.</p>
 *
 * <p>The connector uses CDC functionality of SQL Server that is implemented as as a process that monitors
 * source table and write changes from the table into the change table.</p>
 *
 * <p>The main loop keeps a pointer to the LSN of changes that were already processed. It queries all change
 * tables and get result set of changes. It always finds the smallest LSN across all tables and the change
 * is converted into the event message and sent downstream. The process repeats until all result sets are
 * empty. The LSN is marked and the procedure repeats.</p>
 *
 * <p>The schema changes detection follows the procedure recommended by SQL Server CDC documentation.
 * The database operator should create one more capture process (and table) when a table schema is updated.
 * The code detects presence of two change tables for a single source table. It decides which table is the new one
 * depending on LSNs stored in them. The loop streams changes from the older table till there are events in new
 * table with the LSN larger than in the old one. Then the change table is switched and streaming is executed
 * from the new one.</p>
 *
 * @author Jiri Pechanec
 */
public class SqlServerStreamingChangeEventSource implements StreamingChangeEventSource<SqlServerPartition, SqlServerOffsetContext> {

    private static final Pattern MISSING_CDC_FUNCTION_CHANGES_ERROR = Pattern.compile("Invalid object name '(.*)\\.cdc.fn_cdc_get_all_changes_(.*)'\\.");

    private static final Logger LOGGER = LoggerFactory.getLogger(SqlServerStreamingChangeEventSource.class);

    private static final Duration DEFAULT_INTERVAL_BETWEEN_COMMITS = Duration.ofMinutes(1);
    private static final int INTERVAL_BETWEEN_COMMITS_BASED_ON_POLL_FACTOR = 3;

    /**
     * Connection used for reading CDC tables.
     */
    private final SqlServerConnection dataConnection;

    /**
     * A separate connection for retrieving details of the schema changes; without it, adaptive buffering will not work.
     *
     * @link https://docs.microsoft.com/en-us/sql/connect/jdbc/using-adaptive-buffering?view=sql-server-2017#guidelines-for-using-adaptive-buffering
     */
    private final SqlServerConnection metadataConnection;

    private final EventDispatcher<SqlServerPartition, TableId> dispatcher;
    private final ErrorHandler errorHandler;
    private final Clock clock;
    private final SqlServerDatabaseSchema schema;
    private final Duration pollInterval;
    private final SnapshotterService snapshotterService;
    private final SqlServerConnectorConfig connectorConfig;

    private final ElapsedTimeStrategy pauseBetweenCommits;
    private final Map<SqlServerPartition, SqlServerStreamingExecutionContext> streamingExecutionContexts;
    private final Map<SqlServerPartition, Set<SqlServerChangeTable>> changeTablesWithKnownStopLsn = new HashMap<>();

    private boolean checkAgent;
    private SqlServerOffsetContext effectiveOffset;
    private final NotificationService<SqlServerPartition, SqlServerOffsetContext> notificationService;

    public SqlServerStreamingChangeEventSource(SqlServerConnectorConfig connectorConfig, SqlServerConnection dataConnection,
                                               SqlServerConnection metadataConnection,
                                               EventDispatcher<SqlServerPartition, TableId> dispatcher,
                                               ErrorHandler errorHandler, Clock clock, SqlServerDatabaseSchema schema,
                                               NotificationService<SqlServerPartition, SqlServerOffsetContext> notificationService,
                                               SnapshotterService snapshotterService) {
        this.connectorConfig = connectorConfig;
        this.dataConnection = dataConnection;
        this.metadataConnection = metadataConnection;
        this.dispatcher = dispatcher;
        this.errorHandler = errorHandler;
        this.clock = clock;
        this.schema = schema;
        this.notificationService = notificationService;
        this.pollInterval = connectorConfig.getPollInterval();
        this.snapshotterService = snapshotterService;
        final Duration intervalBetweenCommitsBasedOnPoll = this.pollInterval.multipliedBy(INTERVAL_BETWEEN_COMMITS_BASED_ON_POLL_FACTOR);
        this.pauseBetweenCommits = ElapsedTimeStrategy.constant(clock,
                DEFAULT_INTERVAL_BETWEEN_COMMITS.compareTo(intervalBetweenCommitsBasedOnPoll) > 0
                        ? DEFAULT_INTERVAL_BETWEEN_COMMITS.toMillis()
                        : intervalBetweenCommitsBasedOnPoll.toMillis());
        this.streamingExecutionContexts = new HashMap<>();
        this.checkAgent = true;
    }

    @Override
    public void init(SqlServerOffsetContext offsetContext) throws InterruptedException {
        this.effectiveOffset = offsetContext == null ? new SqlServerOffsetContext(connectorConfig, TxLogPosition.NULL, false, false) : offsetContext;
    }

    @Override
    public void execute(ChangeEventSourceContext context, SqlServerPartition partition, SqlServerOffsetContext offsetContext) throws InterruptedException {
        throw new UnsupportedOperationException("Currently unsupported by the SQL Server connector");
    }

    @Override
    public boolean executeIteration(ChangeEventSourceContext context, SqlServerPartition partition, SqlServerOffsetContext offsetContext) {

        if (!snapshotterService.getSnapshotter().shouldStream()) {
            LOGGER.info("Streaming is disabled for snapshot mode {}", snapshotterService.getSnapshotter().name());
            return false;
        }

        final String databaseName = partition.getDatabaseName();

        this.effectiveOffset = offsetContext;

        try {
            final SqlServerStreamingExecutionContext streamingExecutionContext = streamingExecutionContexts.getOrDefault(partition,
                    new SqlServerStreamingExecutionContext(
                            new PriorityQueue<>(Comparator.comparing(SqlServerChangeTable::getStopLsn)),
                            new AtomicReference<>(),
                            offsetContext.getChangePosition(),
                            new AtomicBoolean(false),
                            // LSN should be increased for the first run only immediately after snapshot completion
                            // otherwise we might skip an incomplete transaction after restart
                            offsetContext.isSnapshotCompleted()));

            if (!streamingExecutionContexts.containsKey(partition)) {
                streamingExecutionContexts.put(partition, streamingExecutionContext);
                LOGGER.info("Last position recorded in offsets is {}[{}]", offsetContext.getChangePosition(), offsetContext.getEventSerialNo());
            }

            final Queue<SqlServerChangeTable> schemaChangeCheckpoints = streamingExecutionContext.getSchemaChangeCheckpoints();
            final AtomicReference<SqlServerChangeTable[]> tablesSlot = streamingExecutionContext.getTablesSlot();
            final TxLogPosition lastProcessedPositionOnStart = offsetContext.getChangePosition();
            final long lastProcessedEventSerialNoOnStart = offsetContext.getEventSerialNo();
            final AtomicBoolean changesStoppedBeingMonotonic = streamingExecutionContext.getChangesStoppedBeingMonotonic();
            final int maxTransactionsPerIteration = connectorConfig.getMaxTransactionsPerIteration();

            TxLogPosition lastProcessedPosition = streamingExecutionContext.getLastProcessedPosition();

            if (context.isRunning()) {
                commitTransaction();
                final Lsn toLsn = getToLsn(dataConnection, databaseName, lastProcessedPosition, maxTransactionsPerIteration);

                // Shouldn't happen if the agent is running, but it is better to guard against such situation
                if (!toLsn.isAvailable()) {
                    if (checkAgent) {
                        try {
                            if (!dataConnection.isAgentRunning(databaseName)) {
                                LOGGER.error("No maximum LSN recorded in the database; SQL Server Agent is not running");
                            }
                        }
                        catch (SQLException e) {
                            LOGGER.warn("No maximum LSN recorded in the database; this may happen if there are no changes recorded in the change table yet or " +
                                    "low activity database where the cdc clean up job periodically clears entries from the cdc tables. " +
                                    "Otherwise, this may be an indication that the SQL Server Agent is not running. " +
                                    "You should follow the documentation on how to configure SQL Server Agent running status query.");
                            LOGGER.warn("Cannot query the status of the SQL Server Agent", e);
                        }
                        checkAgent = false;
                    }
                    return false;
                }
                else if (!checkAgent) {
                    checkAgent = true;
                }
                // There is no change in the database
                if (toLsn.compareTo(lastProcessedPosition.getCommitLsn()) <= 0 && streamingExecutionContext.getShouldIncreaseFromLsn()) {
                    LOGGER.debug("No change in the database");
                    dispatcher.dispatchHeartbeatEvent(partition, offsetContext);
                    return false;
                }

                // Reading interval is inclusive so we need to move LSN forward but not for first
                // run as TX might not be streamed completely
                final Lsn fromLsn = lastProcessedPosition.getCommitLsn().isAvailable() && streamingExecutionContext.getShouldIncreaseFromLsn()
                        ? dataConnection.incrementLsn(databaseName, lastProcessedPosition.getCommitLsn())
                        : lastProcessedPosition.getCommitLsn();
                streamingExecutionContext.setShouldIncreaseFromLsn(true);

                while (!schemaChangeCheckpoints.isEmpty()) {
                    migrateTable(partition, schemaChangeCheckpoints, offsetContext);
                }
                if (!dataConnection.getNewChangeTables(databaseName, fromLsn, toLsn).isEmpty()) {
                    final SqlServerChangeTable[] tables = getChangeTablesToQuery(partition, offsetContext, toLsn);
                    tablesSlot.set(tables);
                    for (SqlServerChangeTable table : tables) {
                        if (table.getStartLsn().isBetween(fromLsn, toLsn)) {
                            LOGGER.info("Schema will be changed for {}", table);
                            schemaChangeCheckpoints.add(table);
                        }
                    }
                    collectChangeTablesWithKnownStopLsn(partition, tables);
                }
                if (tablesSlot.get() == null) {
                    tablesSlot.set(getChangeTablesToQuery(partition, offsetContext, toLsn));
                    collectChangeTablesWithKnownStopLsn(partition, tablesSlot.get());
                }
                try {
                    dataConnection.getChangesForTables(databaseName, tablesSlot.get(), fromLsn, toLsn, resultSets -> {

                        long eventSerialNoInInitialTx = 1;
                        final int tableCount = resultSets.length;
                        final SqlServerChangeTablePointer[] changeTables = new SqlServerChangeTablePointer[tableCount];
                        final SqlServerChangeTable[] tables = tablesSlot.get();

                        for (int i = 0; i < tableCount; i++) {
                            changeTables[i] = new SqlServerChangeTablePointer(tables[i], resultSets[i]);
                            changeTables[i].next();
                        }

                        for (;;) {
                            SqlServerChangeTablePointer tableWithSmallestLsn = null;
                            for (SqlServerChangeTablePointer changeTable : changeTables) {
                                if (changeTable.isCompleted()) {
                                    continue;
                                }
                                if (tableWithSmallestLsn == null || changeTable.compareTo(tableWithSmallestLsn) < 0) {
                                    tableWithSmallestLsn = changeTable;
                                }
                            }
                            if (tableWithSmallestLsn == null) {
                                // No more LSNs available
                                break;
                            }

                            if (!(tableWithSmallestLsn.getChangePosition().isAvailable() && tableWithSmallestLsn.getChangePosition().getInTxLsn().isAvailable())) {
                                LOGGER.error("Skipping change {} as its LSN is NULL which is not expected", tableWithSmallestLsn);
                                tableWithSmallestLsn.next();
                                continue;
                            }

                            if (tableWithSmallestLsn.isNewTransaction() && changesStoppedBeingMonotonic.get()) {
                                LOGGER.info("Resetting changesStoppedBeingMonotonic as transaction changes");
                                changesStoppedBeingMonotonic.set(false);
                            }

                            // After restart for changes that are not monotonic to avoid data loss
                            if (tableWithSmallestLsn.isCurrentPositionSmallerThanPreviousPosition()) {
                                LOGGER.info("Disabling skipping changes due to not monotonic order of changes");
                                changesStoppedBeingMonotonic.set(true);
                            }

                            // After restart for changes that were executed before the last committed offset
                            if (!changesStoppedBeingMonotonic.get() &&
                                    tableWithSmallestLsn.getChangePosition().compareTo(lastProcessedPositionOnStart) < 0) {
                                LOGGER.info("Skipping change {} as its position is smaller than the last recorded position {}", tableWithSmallestLsn,
                                        lastProcessedPositionOnStart);
                                tableWithSmallestLsn.next();
                                continue;
                            }
                            // After restart for change that was the last committed and operations in it before the last committed offset
                            if (!changesStoppedBeingMonotonic.get() && tableWithSmallestLsn.getChangePosition().compareTo(lastProcessedPositionOnStart) == 0
                                    && eventSerialNoInInitialTx <= lastProcessedEventSerialNoOnStart) {
                                LOGGER.info("Skipping change {} as its order in the transaction {} is smaller than or equal to the last recorded operation {}[{}]",
                                        tableWithSmallestLsn, eventSerialNoInInitialTx, lastProcessedPositionOnStart, lastProcessedEventSerialNoOnStart);
                                eventSerialNoInInitialTx++;
                                tableWithSmallestLsn.next();
                                continue;
                            }
                            if (tableWithSmallestLsn.getChangeTable().getStopLsn().isAvailable() &&
                                    tableWithSmallestLsn.getChangeTable().getStopLsn().compareTo(tableWithSmallestLsn.getChangePosition().getCommitLsn()) <= 0) {
                                LOGGER.debug("Skipping table change {} as its stop LSN is smaller than the last recorded LSN {}", tableWithSmallestLsn,
                                        tableWithSmallestLsn.getChangePosition());
                                tableWithSmallestLsn.next();
                                continue;
                            }
                            LOGGER.trace("Processing change {}", tableWithSmallestLsn);
                            LOGGER.trace("Schema change checkpoints {}", schemaChangeCheckpoints);
                            if (!schemaChangeCheckpoints.isEmpty()) {
                                if (tableWithSmallestLsn.getChangePosition().getCommitLsn().compareTo(schemaChangeCheckpoints.peek().getStartLsn()) >= 0) {
                                    migrateTable(partition, schemaChangeCheckpoints, offsetContext);
                                }
                            }
                            final TableId tableId = tableWithSmallestLsn.getChangeTable().getSourceTableId();
                            final int operation = tableWithSmallestLsn.getOperation();
                            final Object[] data = tableWithSmallestLsn.getData();

                            // UPDATE consists of two consecutive events, first event contains
                            // the row before it was updated and the second the row after
                            // it was updated
                            int eventCount = 1;
                            if (operation == SqlServerChangeRecordEmitter.OP_UPDATE_BEFORE) {
                                if (!tableWithSmallestLsn.next() || tableWithSmallestLsn.getOperation() != SqlServerChangeRecordEmitter.OP_UPDATE_AFTER) {
                                    throw new IllegalStateException("The update before event at " + tableWithSmallestLsn.getChangePosition() + " for table " + tableId
                                            + " was not followed by after event.\n Please report this as a bug together with a events around given LSN.");
                                }
                                eventCount = 2;
                            }
                            final Object[] dataNext = (operation == SqlServerChangeRecordEmitter.OP_UPDATE_BEFORE) ? tableWithSmallestLsn.getData() : null;

                            final ResultSet resultSet = tableWithSmallestLsn.getResultSet();
                            offsetContext.setChangePosition(tableWithSmallestLsn.getChangePosition(), eventCount);
                            offsetContext.event(
                                    tableWithSmallestLsn.getChangeTable().getSourceTableId(),
                                    resultSet.getTimestamp(resultSet.getMetaData().getColumnCount()).toInstant());

                            dispatcher
                                    .dispatchDataChangeEvent(
                                            partition,
                                            tableId,
                                            new SqlServerChangeRecordEmitter(
                                                    partition,
                                                    offsetContext,
                                                    operation,
                                                    data,
                                                    dataNext,
                                                    clock,
                                                    connectorConfig));
                            tableWithSmallestLsn.next();
                        }
                    });
                    streamingExecutionContext.setLastProcessedPosition(TxLogPosition.valueOf(toLsn));
                    // Terminate the transaction otherwise CDC could not be disabled for tables
                    dataConnection.rollback();
                }
                catch (SQLException e) {
                    tablesSlot.set(processErrorFromChangeTableQuery(databaseName, e, tablesSlot.get()));
                }
            }
        }
        catch (Exception e) {
            errorHandler.setProducerThrowable(e);
        }

        return true;
    }

    @Override
    public SqlServerOffsetContext getOffsetContext() {
        return effectiveOffset;
    }

    private void collectChangeTablesWithKnownStopLsn(SqlServerPartition partition, SqlServerChangeTable[] tables) {
        for (SqlServerChangeTable table : tables) {
            if (table.getStopLsn().isAvailable()) {
                LOGGER.info("The stop lsn of {} change table became known", table);
                changeTablesWithKnownStopLsn.computeIfAbsent(partition, x -> new HashSet<>()).add(table);
            }
        }
    }

    private void commitTransaction() throws SQLException {
        // When reading from read-only Always On replica the default and only transaction isolation
        // is snapshot. This means that CDC metadata are not visible for long-running transactions.
        // It is thus necessary to restart the transaction before every read.
        // For R/W database it is important to execute regular commits to maintain the size of TempDB
        if (connectorConfig.isReadOnlyDatabaseConnection() || pauseBetweenCommits.hasElapsed()) {
            dataConnection.commit();
            metadataConnection.commit();
        }
    }

    private void migrateTable(SqlServerPartition partition, final Queue<SqlServerChangeTable> schemaChangeCheckpoints, SqlServerOffsetContext offsetContext)
            throws InterruptedException, SQLException {
        final SqlServerChangeTable newTable = schemaChangeCheckpoints.poll();
        LOGGER.info("Migrating schema to {}", newTable);
        Table oldTableSchema = schema.tableFor(newTable.getSourceTableId());
        Table tableSchema = metadataConnection.getTableSchemaFromTable(partition.getDatabaseName(), newTable);
        if (oldTableSchema.equals(tableSchema)) {
            LOGGER.info("Migration skipped, no table schema changes detected.");
            return;
        }
        dispatcher.dispatchSchemaChangeEvent(partition, offsetContext, newTable.getSourceTableId(),
                new SqlServerSchemaChangeEventEmitter(partition, offsetContext, newTable, tableSchema, schema,
                        SchemaChangeEventType.ALTER));
        newTable.setSourceTable(tableSchema);
    }

    private SqlServerChangeTable[] processErrorFromChangeTableQuery(String databaseName, SQLException exception,
                                                                    SqlServerChangeTable[] currentChangeTables)
            throws Exception {
        final Matcher m = MISSING_CDC_FUNCTION_CHANGES_ERROR.matcher(exception.getMessage());
        if (m.matches() && m.group(1).equals(databaseName)) {
            final String captureName = m.group(2);
            LOGGER.info("Table is no longer captured with capture instance {}", captureName);
            return Arrays.stream(currentChangeTables)
                    .filter(x -> !x.getCaptureInstance().equals(captureName))
                    .toArray(SqlServerChangeTable[]::new);
        }
        throw exception;
    }

    private SqlServerChangeTable[] getChangeTablesToQuery(SqlServerPartition partition, SqlServerOffsetContext offsetContext,
                                                          Lsn toLsn)
            throws SQLException, InterruptedException {
        final String databaseName = partition.getDatabaseName();
        final List<SqlServerChangeTable> changeTables = dataConnection.getChangeTables(databaseName, toLsn);
        if (changeTables.isEmpty()) {
            LOGGER.warn("No table has enabled CDC or security constraints prevents getting the list of change tables");
        }

        final Map<TableId, List<SqlServerChangeTable>> includeListChangeTables = changeTables.stream()
                .filter(changeTable -> {
                    if (connectorConfig.getTableFilters().dataCollectionFilter().isIncluded(changeTable.getSourceTableId())) {
                        return true;
                    }
                    else {
                        LOGGER.info("CDC is enabled for table {} but the table is not on connector's table include list", changeTable);
                        return false;
                    }
                })
                .collect(Collectors.groupingBy(ChangeTable::getSourceTableId));

        if (includeListChangeTables.isEmpty()) {
            LOGGER.warn(
                    "No table on connector's include list has enabled CDC, tables on include list do not contain any table with CDC enabled or no table match the include/exclude filter(s)");
        }

        final List<SqlServerChangeTable> tables = new ArrayList<>();
        for (List<SqlServerChangeTable> captures : includeListChangeTables.values()) {
            SqlServerChangeTable currentTable = captures.get(0);

            // Get changes from table with multiple CDC instances,
            // and it must be table that have already been captured schema.
            if (captures.size() > 1 && schema.tableFor(currentTable.getSourceTableId()) != null) {
                SqlServerChangeTable futureTable;
                if (captures.get(0).getStartLsn().compareTo(captures.get(1).getStartLsn()) < 0) {
                    futureTable = captures.get(1);
                }
                else {
                    currentTable = captures.get(1);
                    futureTable = captures.get(0);
                }
                currentTable.setStopLsn(futureTable.getStartLsn());
                futureTable.setSourceTable(dataConnection.getTableSchemaFromTable(databaseName, futureTable));
                tables.add(futureTable);
                LOGGER.info("Multiple capture instances present for the same table: {} and {}", currentTable, futureTable);
            }

            // Check whether the table has been captured schema or not.
            // We only need to get changes for table that have already been captured schema.
            if (schema.tableFor(currentTable.getSourceTableId()) != null) {
                currentTable.setSourceTable(schema.tableFor(currentTable.getSourceTableId()));
                tables.add(currentTable);
            }
        }

        return tables.toArray(new SqlServerChangeTable[tables.size()]);
    }

    /**
     * @return the log sequence number up until which the connector should query changes from the database.
     */
    private Lsn getToLsn(SqlServerConnection connection, String databaseName, TxLogPosition lastProcessedPosition,
                         int maxTransactionsPerIteration)
            throws SQLException {

        if (maxTransactionsPerIteration == 0) {
            return connection.getMaxTransactionLsn(databaseName);
        }

        final Lsn fromLsn = lastProcessedPosition.getCommitLsn();

        if (!fromLsn.isAvailable()) {
            return connection.getNthTransactionLsnFromBeginning(databaseName, maxTransactionsPerIteration);
        }

        return connection.getNthTransactionLsnFromLast(databaseName, fromLsn, maxTransactionsPerIteration);
    }

    @Override
    public void commitOffset(Map<String, ?> sourcePartition, Map<String, ?> offset) {
        Lsn commitLsn = Lsn.valueOf((String) offset.get("commit_lsn"));
        synchronized (changeTablesWithKnownStopLsn) {
            Optional<SqlServerPartition> optionalPartition = changeTablesWithKnownStopLsn.keySet().stream()
                    .filter(p -> p.getSourcePartition().equals(sourcePartition))
                    .findFirst();

            if (optionalPartition.isEmpty()) {
                return;
            }

            SqlServerPartition partition = optionalPartition.get();
            Set<SqlServerChangeTable> partitionTables = changeTablesWithKnownStopLsn.get(partition);

            List<SqlServerChangeTable> changeTablesToCompleteReadingFrom = partitionTables.stream()
                    .filter(t -> t.getStopLsn().compareTo(commitLsn) < 0)
                    .collect(Collectors.toList());

            for (SqlServerChangeTable table : changeTablesToCompleteReadingFrom) {
                Map<String, String> additionalData = new HashMap<>();
                additionalData.put("connector_name", connectorConfig.getLogicalName());
                additionalData.put("capture_instance", table.getCaptureInstance());
                additionalData.put("start_lsn", table.getStartLsn().toString());
                additionalData.put("stop_lsn", table.getStopLsn().toString());
                additionalData.put("commit_lsn", commitLsn.toString());
                additionalData.putAll(partition.getSourcePartition());

                notificationService.notify(Notification.Builder.builder()
                        .withId(UUID.randomUUID().toString())
                        .withAggregateType("Capture Instance")
                        .withType("COMPLETED")
                        .withAdditionalData(additionalData)
                        .withTimestamp(clock.currentTimeInMillis())
                        .build());

                partitionTables.remove(table);

                LOGGER.info(
                        "Complete reading from change table {} as the committed change lsn ({}) is greater than the table's stop lsn ({})",
                        table, offset, table.getStopLsn().toString());
            }
        }
    }
}