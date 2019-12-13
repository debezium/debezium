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
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
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
public class SqlServerStreamingChangeEventSource implements StreamingChangeEventSource {

    private static final int COL_COMMIT_LSN = 1;
    private static final int COL_ROW_LSN = 2;
    private static final int COL_OPERATION = 3;
    private static final int COL_DATA = 5;

    private static final Pattern MISSING_CDC_FUNCTION_CHANGES_ERROR = Pattern.compile("Invalid object name 'cdc.fn_cdc_get_all_changes_(.*)'\\.");

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

    private final EventDispatcher<TableId> dispatcher;
    private final ErrorHandler errorHandler;
    private final Clock clock;
    private final SqlServerDatabaseSchema schema;
    private final SqlServerOffsetContext offsetContext;
    private final Duration pollInterval;
    private final SqlServerConnectorConfig connectorConfig;

    public SqlServerStreamingChangeEventSource(SqlServerConnectorConfig connectorConfig, SqlServerOffsetContext offsetContext, SqlServerConnection dataConnection,
                                               SqlServerConnection metadataConnection, EventDispatcher<TableId> dispatcher, ErrorHandler errorHandler, Clock clock,
                                               SqlServerDatabaseSchema schema) {
        this.connectorConfig = connectorConfig;
        this.dataConnection = dataConnection;
        this.metadataConnection = metadataConnection;
        this.dispatcher = dispatcher;
        this.errorHandler = errorHandler;
        this.clock = clock;
        this.schema = schema;
        this.offsetContext = offsetContext;
        this.pollInterval = connectorConfig.getPollInterval();
    }

    @Override
    public void execute(ChangeEventSourceContext context) throws InterruptedException {
        final Metronome metronome = Metronome.sleeper(pollInterval, clock);
        final Queue<ChangeTable> schemaChangeCheckpoints = new PriorityQueue<>((x, y) -> x.getStopLsn().compareTo(y.getStopLsn()));
        try {
            final AtomicReference<ChangeTable[]> tablesSlot = new AtomicReference<ChangeTable[]>(getCdcTablesToQuery());

            final TxLogPosition lastProcessedPositionOnStart = offsetContext.getChangePosition();
            final long lastProcessedEventSerialNoOnStart = offsetContext.getEventSerialNo();
            LOGGER.info("Last position recorded in offsets is {}[{}]", lastProcessedPositionOnStart, lastProcessedEventSerialNoOnStart);

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
                final Lsn currentMaxLsn = dataConnection.getMaxLsn();

                // Shouldn't happen if the agent is running, but it is better to guard against such situation
                if (!currentMaxLsn.isAvailable()) {
                    LOGGER.warn("No maximum LSN recorded in the database; please ensure that the SQL Server Agent is running");
                    metronome.pause();
                    continue;
                }
                // There is no change in the database
                if (currentMaxLsn.equals(lastProcessedPosition.getCommitLsn()) && shouldIncreaseFromLsn) {
                    LOGGER.debug("No change in the database");
                    metronome.pause();
                    continue;
                }

                // Reading interval is inclusive so we need to move LSN forward but not for first
                // run as TX might not be streamed completely
                final Lsn fromLsn = lastProcessedPosition.getCommitLsn().isAvailable() && shouldIncreaseFromLsn
                        ? dataConnection.incrementLsn(lastProcessedPosition.getCommitLsn())
                        : lastProcessedPosition.getCommitLsn();
                shouldIncreaseFromLsn = true;

                while (!schemaChangeCheckpoints.isEmpty()) {
                    migrateTable(schemaChangeCheckpoints);
                }
                if (!dataConnection.listOfNewChangeTables(fromLsn, currentMaxLsn).isEmpty()) {
                    final ChangeTable[] tables = getCdcTablesToQuery();
                    tablesSlot.set(tables);
                    for (ChangeTable table : tables) {
                        if (table.getStartLsn().isBetween(fromLsn, currentMaxLsn)) {
                            LOGGER.info("Schema will be changed for {}", table);
                            schemaChangeCheckpoints.add(table);
                        }
                    }
                }
                try {
                    dataConnection.getChangesForTables(tablesSlot.get(), fromLsn, currentMaxLsn, resultSets -> {

                        long eventSerialNoInInitialTx = 1;
                        final int tableCount = resultSets.length;
                        final ChangeTablePointer[] changeTables = new ChangeTablePointer[tableCount];
                        final ChangeTable[] tables = tablesSlot.get();

                        for (int i = 0; i < tableCount; i++) {
                            changeTables[i] = new ChangeTablePointer(tables[i], resultSets[i]);
                            changeTables[i].next();
                        }

                        for (;;) {
                            ChangeTablePointer tableWithSmallestLsn = null;
                            for (ChangeTablePointer changeTable : changeTables) {
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
                            // After restart for changes that were executed before the last committed offset
                            if (tableWithSmallestLsn.getChangePosition().compareTo(lastProcessedPositionOnStart) < 0) {
                                LOGGER.info("Skipping change {} as its position is smaller than the last recorded position {}", tableWithSmallestLsn,
                                        lastProcessedPositionOnStart);
                                tableWithSmallestLsn.next();
                                continue;
                            }
                            // After restart for change that was the last committed and operations in it before the last committed offset
                            if (tableWithSmallestLsn.getChangePosition().compareTo(lastProcessedPositionOnStart) == 0
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
                            if (!schemaChangeCheckpoints.isEmpty()) {
                                if (tableWithSmallestLsn.getChangePosition().getCommitLsn().compareTo(schemaChangeCheckpoints.peek().getStopLsn()) >= 0) {
                                    migrateTable(schemaChangeCheckpoints);
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
                            offsetContext.event(tableWithSmallestLsn.getChangeTable().getSourceTableId(),
                                    metadataConnection.timestampOfLsn(tableWithSmallestLsn.getChangePosition().getCommitLsn()));

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
                    });
                    lastProcessedPosition = TxLogPosition.valueOf(currentMaxLsn);
                    // Terminate the transaction otherwise CDC could not be disabled for tables
                    dataConnection.rollback();
                }
                catch (SQLException e) {
                    tablesSlot.set(processErrorFromChangeTableQuery(e, tablesSlot.get()));
                }
            }
        }
        catch (Exception e) {
            errorHandler.setProducerThrowable(e);
        }
    }

    private void migrateTable(final Queue<ChangeTable> schemaChangeCheckpoints)
            throws InterruptedException, SQLException {
        final ChangeTable newTable = schemaChangeCheckpoints.poll();
        LOGGER.info("Migrating schema to {}", newTable);
        dispatcher.dispatchSchemaChangeEvent(newTable.getSourceTableId(),
                new SqlServerSchemaChangeEventEmitter(offsetContext, newTable, metadataConnection.getTableSchemaFromTable(newTable), SchemaChangeEventType.ALTER));
    }

    private ChangeTable[] processErrorFromChangeTableQuery(SQLException exception, ChangeTable[] currentChangeTables) throws Exception {
        final Matcher m = MISSING_CDC_FUNCTION_CHANGES_ERROR.matcher(exception.getMessage());
        if (m.matches()) {
            final String captureName = m.group(1);
            LOGGER.info("Table is no longer captured with capture instance {}", captureName);
            return Arrays.asList(currentChangeTables).stream()
                    .filter(x -> !x.getCaptureInstance().equals(captureName))
                    .collect(Collectors.toList()).toArray(new ChangeTable[0]);
        }
        throw exception;
    }

    private ChangeTable[] getCdcTablesToQuery() throws SQLException, InterruptedException {
        final Set<ChangeTable> cdcEnabledTables = dataConnection.listOfChangeTables();
        if (cdcEnabledTables.isEmpty()) {
            LOGGER.warn("No table has enabled CDC or security constraints prevents getting the list of change tables");
        }

        final Map<TableId, List<ChangeTable>> whitelistedCdcEnabledTables = cdcEnabledTables.stream()
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

        if (whitelistedCdcEnabledTables.isEmpty()) {
            LOGGER.warn(
                    "No whitelisted table has enabled CDC, whitelisted table list does not contain any table with CDC enabled or no table match the white/blacklist filter(s)");
        }

        final List<ChangeTable> tables = new ArrayList<>();
        for (List<ChangeTable> captures : whitelistedCdcEnabledTables.values()) {
            ChangeTable currentTable = captures.get(0);
            if (captures.size() > 1) {
                ChangeTable futureTable;
                if (captures.get(0).getStartLsn().compareTo(captures.get(1).getStartLsn()) < 0) {
                    futureTable = captures.get(1);
                }
                else {
                    currentTable = captures.get(1);
                    futureTable = captures.get(0);
                }
                currentTable.setStopLsn(futureTable.getStartLsn());
                tables.add(futureTable);
                LOGGER.info("Multiple capture instances present for the same table: {} and {}", currentTable, futureTable);
            }
            if (schema.tableFor(currentTable.getSourceTableId()) == null) {
                LOGGER.info("Table {} is new to be monitored by capture instance {}", currentTable.getSourceTableId(), currentTable.getCaptureInstance());
                // We need to read the source table schema - nullability information cannot be obtained from change table
                dispatcher.dispatchSchemaChangeEvent(
                        currentTable.getSourceTableId(),
                        new SqlServerSchemaChangeEventEmitter(
                                offsetContext,
                                currentTable,
                                dataConnection.getTableSchemaFromTable(currentTable),
                                SchemaChangeEventType.CREATE));
            }
            tables.add(currentTable);
        }

        return tables.toArray(new ChangeTable[tables.size()]);
    }

    @Override
    public void commitOffset(Map<String, ?> offset) {
    }

    /**
     * The logical representation of a position for the change in the transaction log.
     * During each sourcing cycle it is necessary to query all change tables and then
     * make a total order of changes across all tables.<br>
     * This class represents an open database cursor over the change table that is
     * able to move the cursor forward and report the LSN for the change to which the cursor
     * now points.
     *
     * @author Jiri Pechanec
     *
     */
    private static class ChangeTablePointer {

        private final ChangeTable changeTable;
        private final ResultSet resultSet;
        private boolean completed = false;
        private TxLogPosition currentChangePosition;

        public ChangeTablePointer(ChangeTable changeTable, ResultSet resultSet) {
            this.changeTable = changeTable;
            this.resultSet = resultSet;
        }

        public ChangeTable getChangeTable() {
            return changeTable;
        }

        public TxLogPosition getChangePosition() throws SQLException {
            return currentChangePosition;
        }

        public int getOperation() throws SQLException {
            return resultSet.getInt(COL_OPERATION);
        }

        public Object[] getData() throws SQLException {
            final int dataColumnCount = resultSet.getMetaData().getColumnCount() - (COL_DATA - 1);
            final Object[] data = new Object[dataColumnCount];
            for (int i = 0; i < dataColumnCount; i++) {
                data[i] = resultSet.getObject(COL_DATA + i);
            }
            return data;
        }

        public boolean next() throws SQLException {
            completed = !resultSet.next();
            currentChangePosition = completed ? TxLogPosition.NULL
                    : TxLogPosition.valueOf(Lsn.valueOf(resultSet.getBytes(COL_COMMIT_LSN)), Lsn.valueOf(resultSet.getBytes(COL_ROW_LSN)));
            if (completed) {
                LOGGER.trace("Closing result set of change tables for table {}", changeTable);
                resultSet.close();
            }
            return !completed;
        }

        public boolean isCompleted() {
            return completed;
        }

        public int compareTo(ChangeTablePointer o) throws SQLException {
            return getChangePosition().compareTo(o.getChangePosition());
        }

        @Override
        public String toString() {
            return "ChangeTablePointer [changeTable=" + changeTable + ", resultSet=" + resultSet + ", completed="
                    + completed + ", currentChangePosition=" + currentChangePosition + "]";
        }
    }
}
