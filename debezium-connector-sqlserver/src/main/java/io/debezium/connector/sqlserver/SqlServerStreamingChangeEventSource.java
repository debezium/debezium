/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.sqlserver.SqlServerConnectorConfig.SnapshotMode;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.schema.SchemaChangeEvent.SchemaChangeEventType;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;

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
public class SqlServerStreamingChangeEventSource implements StreamingChangeEventSource<SqlServerTaskPartition, SqlServerOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SqlServerStreamingChangeEventSource.class);

    /**
     * Connection used for reading CDC tables.
     */
    private final SqlServerConnection dataConnection;

    /**
     * A separate connection for retrieving timestamps; without it, adaptive
     * buffering will not work.
     *
     * @see https://docs.microsoft.com/en-us/sql/connect/jdbc/using-adaptive-buffering?view=sql-server-2017#guidelines-for-using-adaptive-buffering
     */
    private final SqlServerConnection metadataConnection;

    private final EventDispatcher<SqlServerTaskPartition, SqlServerOffsetContext, TableId> dispatcher;
    private final ErrorHandler errorHandler;
    private final Clock clock;
    private final SqlServerDatabaseSchema schema;
    private final Duration pollInterval;
    private final SqlServerConnectorConfig connectorConfig;

    public SqlServerStreamingChangeEventSource(SqlServerConnectorConfig connectorConfig, SqlServerConnection dataConnection,
                                               SqlServerConnection metadataConnection,
                                               EventDispatcher<SqlServerTaskPartition, SqlServerOffsetContext, TableId> dispatcher, ErrorHandler errorHandler,
                                               Clock clock,
                                               SqlServerDatabaseSchema schema) {
        this.connectorConfig = connectorConfig;
        this.dataConnection = dataConnection;
        this.metadataConnection = metadataConnection;
        this.dispatcher = dispatcher;
        this.errorHandler = errorHandler;
        this.clock = clock;
        this.schema = schema;
        this.pollInterval = connectorConfig.getPollInterval();
    }

    @Override
    public void execute(ChangeEventSourceContext context, SqlServerTaskPartition partition, SqlServerOffsetContext offsetContext) throws InterruptedException {
        if (connectorConfig.getSnapshotMode().equals(SnapshotMode.INITIAL_ONLY)) {
            LOGGER.info("Streaming is not enabled in current configuration");
            return;
        }

        final Metronome metronome = Metronome.sleeper(pollInterval, clock);
        final Queue<SqlServerChangeTable> schemaChangeCheckpoints = new PriorityQueue<>((x, y) -> x.getStopLsn().compareTo(y.getStopLsn()));
        try {
            final AtomicReference<SqlServerChangeTable[]> tablesSlot = new AtomicReference<SqlServerChangeTable[]>(getCdcTablesToQuery(offsetContext, partition));

            final TxLogPosition lastProcessedPositionOnStart = offsetContext.getChangePosition();
            final long lastProcessedEventSerialNoOnStart = offsetContext.getEventSerialNo();
            LOGGER.info("Last position recorded in offsets is {}[{}]", lastProcessedPositionOnStart, lastProcessedEventSerialNoOnStart);
            final AtomicBoolean changesStoppedBeingMonotonic = new AtomicBoolean(false);

            TxLogPosition lastProcessedPosition = lastProcessedPositionOnStart;

            // LSN should be increased for the first run only immediately after snapshot completion
            // otherwise we might skip an incomplete transaction after restart
            boolean shouldIncreaseFromLsn = offsetContext.isSnapshotCompleted();
            while (context.isRunning()) {
                // When reading from read-only Always On replica the default and only transaction isolation
                // is snapshot. This means that CDC metadata are not visible for long-running transactions.
                // It is thus necessary to restart the transaction before every read.
                if (connectorConfig.isReadOnlyDatabaseConnection()) {
                    dataConnection.commit();
                }
                final MaxLsnResult maxLsnResult = dataConnection.getMaxLsnResult(connectorConfig.isSkipLowActivityLsnsEnabled(), partition.getDatabaseName());

                // Shouldn't happen if the agent is running, but it is better to guard against such situation
                if (!maxLsnResult.getMaxLsn().isAvailable() || !maxLsnResult.getMaxTransactionalLsn().isAvailable()) {
                    LOGGER.warn("No maximum LSN recorded in the database; please ensure that the SQL Server Agent is running");
                    metronome.pause();
                    continue;
                }
                // There is no change in the database
                if (maxLsnResult.getMaxTransactionalLsn().compareTo(lastProcessedPosition.getCommitLsn()) <= 0 && shouldIncreaseFromLsn) {
                    LOGGER.debug("No change in the database");
                    metronome.pause();
                    continue;
                }

                // Reading interval is inclusive so we need to move LSN forward but not for first
                // run as TX might not be streamed completely
                final Lsn fromLsn = lastProcessedPosition.getCommitLsn().isAvailable() && shouldIncreaseFromLsn
                        ? dataConnection.incrementLsn(lastProcessedPosition.getCommitLsn(), partition.getDatabaseName())
                        : lastProcessedPosition.getCommitLsn();
                shouldIncreaseFromLsn = true;

                while (!schemaChangeCheckpoints.isEmpty()) {
                    migrateTable(schemaChangeCheckpoints, offsetContext, partition);
                }
                if (!dataConnection.listOfNewChangeTables(fromLsn, maxLsnResult.getMaxLsn(), partition.getDatabaseName()).isEmpty()) {
                    final SqlServerChangeTable[] tables = getCdcTablesToQuery(offsetContext, partition);
                    tablesSlot.set(tables);
                    for (SqlServerChangeTable table : tables) {
                        if (table.getStartLsn().isBetween(fromLsn, maxLsnResult.getMaxLsn())) {
                            LOGGER.info("Schema will be changed for {}", table);
                            schemaChangeCheckpoints.add(table);
                        }
                    }
                }
                try {
                    dataConnection.getChangesForTables(tablesSlot.get(), fromLsn, maxLsnResult.getMaxLsn(), resultSets -> {

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
                                    migrateTable(schemaChangeCheckpoints, offsetContext, partition);
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

                            offsetContext.setChangePosition(tableWithSmallestLsn.getChangePosition(), eventCount);
                            offsetContext.event(
                                    tableWithSmallestLsn.getChangeTable().getSourceTableId(),
                                    metadataConnection.timestampOfLsn(tableWithSmallestLsn.getChangePosition().getCommitLsn(), partition.getDatabaseName()));

                            dispatcher
                                    .dispatchDataChangeEvent(
                                            tableId,
                                            new SqlServerChangeRecordEmitter(
                                                    offsetContext,
                                                    operation,
                                                    data,
                                                    dataNext,
                                                    clock));
                            tableWithSmallestLsn.next();
                        }
                    }, partition.getDatabaseName());
                    lastProcessedPosition = TxLogPosition.valueOf(maxLsnResult.getMaxLsn());
                    // Terminate the transaction otherwise CDC could not be disabled for tables
                    dataConnection.rollback();
                }
                catch (SQLException e) {
                    tablesSlot.set(processErrorFromChangeTableQuery(e, tablesSlot.get(), partition));
                }
            }
        }
        catch (Exception e) {
            errorHandler.setProducerThrowable(e);
        }
    }

    private void migrateTable(final Queue<SqlServerChangeTable> schemaChangeCheckpoints, SqlServerOffsetContext offsetContext, SqlServerTaskPartition partition)
            throws InterruptedException, SQLException {
        final SqlServerChangeTable newTable = schemaChangeCheckpoints.poll();
        LOGGER.info("Migrating schema to {}", newTable);
        Table tableSchema = metadataConnection.getTableSchemaFromTable(newTable, partition.getDatabaseName());
        dispatcher.dispatchSchemaChangeEvent(newTable.getSourceTableId(),
                new SqlServerSchemaChangeEventEmitter(offsetContext, newTable, tableSchema, SchemaChangeEventType.ALTER));
        newTable.setSourceTable(tableSchema);
    }

    private SqlServerChangeTable[] processErrorFromChangeTableQuery(SQLException exception, SqlServerChangeTable[] currentChangeTables,
                                                                    SqlServerTaskPartition partition)
            throws Exception {
        final String pattern = "Invalid object name '[#db].cdc.fn_cdc_get_all_changes_(.*)'\\."
                .replace("[#db]", partition.getDatabaseName());
        final Matcher m = Pattern.compile(pattern).matcher(exception.getMessage());
        if (m.matches()) {
            final String captureName = m.group(1);
            LOGGER.info("Table is no longer captured with capture instance {}", captureName);
            return Arrays.asList(currentChangeTables).stream()
                    .filter(x -> !x.getCaptureInstance().equals(captureName))
                    .collect(Collectors.toList()).toArray(new SqlServerChangeTable[0]);
        }
        throw exception;
    }

    private SqlServerChangeTable[] getCdcTablesToQuery(SqlServerOffsetContext offsetContext, SqlServerTaskPartition partition) throws SQLException, InterruptedException {
        final Set<SqlServerChangeTable> cdcEnabledTables = dataConnection.listOfChangeTables(partition.getDatabaseName());
        if (cdcEnabledTables.isEmpty()) {
            LOGGER.warn("No table has enabled CDC or security constraints prevents getting the list of change tables");
        }

        final Map<TableId, List<SqlServerChangeTable>> includeListCdcEnabledTables = cdcEnabledTables.stream()
                .filter(changeTable -> {
                    if (connectorConfig.getTableFilters().dataCollectionFilter().isIncluded(changeTable.getSourceTableId())) {
                        return true;
                    }
                    else {
                        LOGGER.info("CDC is enabled for table {} but the table is not whitelisted by connector", changeTable);
                        return false;
                    }
                })
                .collect(Collectors.groupingBy(x -> x.getSourceTableId()));

        if (includeListCdcEnabledTables.isEmpty()) {
            LOGGER.warn(
                    "No whitelisted table has enabled CDC, whitelisted table list does not contain any table with CDC enabled or no table match the white/blacklist filter(s)");
        }

        final List<SqlServerChangeTable> tables = new ArrayList<>();
        for (List<SqlServerChangeTable> captures : includeListCdcEnabledTables.values()) {
            SqlServerChangeTable currentTable = captures.get(0);
            if (captures.size() > 1) {
                SqlServerChangeTable futureTable;
                if (captures.get(0).getStartLsn().compareTo(captures.get(1).getStartLsn()) < 0) {
                    futureTable = captures.get(1);
                }
                else {
                    currentTable = captures.get(1);
                    futureTable = captures.get(0);
                }
                currentTable.setStopLsn(futureTable.getStartLsn());
                futureTable.setSourceTable(dataConnection.getTableSchemaFromTable(futureTable, partition.getDatabaseName()));
                tables.add(futureTable);
                LOGGER.info("Multiple capture instances present for the same table: {} and {}", currentTable, futureTable);
            }
            if (schema.tableFor(currentTable.getSourceTableId()) == null) {
                LOGGER.info("Table {} is new to be monitored by capture instance {}", currentTable.getSourceTableId(), currentTable.getCaptureInstance());
                // We need to read the source table schema - nullability information cannot be obtained from change table
                // There might be no start LSN in the new change table at this time so current timestamp is used
                offsetContext.event(
                        currentTable.getSourceTableId(),
                        Instant.now());
                dispatcher.dispatchSchemaChangeEvent(
                        currentTable.getSourceTableId(),
                        new SqlServerSchemaChangeEventEmitter(
                                offsetContext,
                                currentTable,
                                dataConnection.getTableSchemaFromTable(currentTable, partition.getDatabaseName()),
                                SchemaChangeEventType.CREATE));
            }

            // If a column was renamed, then the old capture instance had been dropped and a new one
            // created. In consequence, a table with out-dated schema might be assigned here.
            // A proper value will be set when migration happens.
            currentTable.setSourceTable(schema.tableFor(currentTable.getSourceTableId()));
            tables.add(currentTable);
        }

        return tables.toArray(new SqlServerChangeTable[tables.size()]);
    }

}
