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
import io.debezium.util.Clock;
import io.debezium.util.Metronome;

/**
 * A {@link StreamingChangeEventSource} based on SQL Server change data capture functionality.
 * A main polls database DDL change and change data tables and turns them into change events.
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

    private final SqlServerConnection connection;
    private final EventDispatcher<TableId> dispatcher;
    private final ErrorHandler errorHandler;
    private final Clock clock;
    private final SqlServerDatabaseSchema schema;
    private final SqlServerOffsetContext offsetContext;
    private final Duration pollInterval;
    private final SqlServerConnectorConfig connectorConfig;

    public SqlServerStreamingChangeEventSource(SqlServerConnectorConfig connectorConfig, SqlServerOffsetContext offsetContext, SqlServerConnection connection, EventDispatcher<TableId> dispatcher, ErrorHandler errorHandler, Clock clock, SqlServerDatabaseSchema schema) {
        this.connectorConfig = connectorConfig;
        this.connection = connection;
        this.dispatcher = dispatcher;
        this.errorHandler = errorHandler;
        this.clock = clock;
        this.schema = schema;
        this.offsetContext = offsetContext;
        this.pollInterval = Duration.ofSeconds(1);
    }

    @Override
    public void execute(ChangeEventSourceContext context) throws InterruptedException {
        final Metronome metronome = Metronome.sleeper(pollInterval, clock);
        final Queue<ChangeTable> schemaChangeCheckpoints = new PriorityQueue<>((x, y) -> x.getStopLsn().compareTo(y.getStopLsn()));
        try {
            final AtomicReference<ChangeTable[]> tablesSlot = new AtomicReference<ChangeTable[]>(getCdcTablesToQuery());

            final Lsn lastProcessedLsnOnStart = offsetContext.getChangeLsn();
            LOGGER.info("Last LSN recorded in offsets is {}", lastProcessedLsnOnStart);

            Lsn lastProcessedLsn = offsetContext.getChangeLsn();
            while (context.isRunning()) {
                final Lsn currentMaxLsn = connection.getMaxLsn();

                // Probably cannot happen but it is better to guard against such
                // situation
                if (!currentMaxLsn.isAvailable()) {
                    LOGGER.debug("No maximum LSN recorded in the database");
                    metronome.pause();
                    continue;
                }
                // There is no change in the database
                if (currentMaxLsn.equals(lastProcessedLsn)) {
                    LOGGER.debug("No change in the database");
                    metronome.pause();
                    continue;
                }

                // Reading interval is inclusive so we need to move LSN forward
                final Lsn fromLsn = lastProcessedLsn.isAvailable() ? connection.incrementLsn(lastProcessedLsn)
                        : lastProcessedLsn;

                schemaChangeCheckpoints.clear();
                if (!connection.listOfNewChangeTables(fromLsn, currentMaxLsn).isEmpty()) {
                    final ChangeTable[] tables = getCdcTablesToQuery();
                    tablesSlot.set(tables);
                    for (ChangeTable table: tables) {
                        if (table.getStopLsn().isBetween(fromLsn, currentMaxLsn)) {
                            LOGGER.info("Schema will be changed for {}", table);
                            schemaChangeCheckpoints.add(table);
                        }
                    }
                }
                try {
                    connection.getChangesForTables(tablesSlot.get(), fromLsn, currentMaxLsn, resultSets -> {

                        final int tableCount = resultSets.length;
                        final ChangeTablePointer[] changeTables = new ChangeTablePointer[tableCount];
                        final ChangeTable[] tables = tablesSlot.get();

                        for (int i = 0; i < tableCount; i++) {
                            changeTables[i] = new ChangeTablePointer(tables[i], resultSets[i]);
                            changeTables[i].next();
                        }

                        for (;;) {
                            ChangeTablePointer tableSmallestLsn = null;
                            for (ChangeTablePointer changeTable: changeTables) {
                                if (changeTable.isCompleted()) {
                                    continue;
                                }
                                if (tableSmallestLsn == null || changeTable.compareTo(tableSmallestLsn) < 0) {
                                    tableSmallestLsn = changeTable;
                                }
                            }
                            if (tableSmallestLsn == null) {
                                // No more LSNs available
                                break;
                            }

                            if (tableSmallestLsn.getRowLsn().compareTo(lastProcessedLsnOnStart) <= 0) {
                                LOGGER.info("Skipping change {} as its LSN is smaller than the last recorded LSN {}", tableSmallestLsn, lastProcessedLsnOnStart);
                                tableSmallestLsn.next();
                                continue;
                            }
                            if (tableSmallestLsn.getChangeTable().getStopLsn().isAvailable() &&
                                    tableSmallestLsn.getChangeTable().getStopLsn().compareTo(tableSmallestLsn.getRowLsn()) <= 0) {
                                LOGGER.debug("Skipping table change {} as its stop LSN is smaller than the last recorded LSN {}", tableSmallestLsn, tableSmallestLsn.getRowLsn());
                                tableSmallestLsn.next();
                                continue;
                            }
                            LOGGER.trace("Processing change {}", tableSmallestLsn);
                            if (!schemaChangeCheckpoints.isEmpty()) {
                                if (tableSmallestLsn.getRowLsn().compareTo(schemaChangeCheckpoints.peek().getStopLsn()) > 0) {
                                    final ChangeTable oldTable = schemaChangeCheckpoints.poll();
                                    final ChangeTable newTable = oldTable.getNextVersionOfTable();
                                    LOGGER.info("Migrating schema from {} to {}", oldTable, newTable);
                                    dispatcher.dispatchSchemaChangeEvent(oldTable.getSourceTableId(), new SqlServerSchemaChangeEventEmitter(offsetContext, newTable, connection.getTableSchemaFromChangeTable(newTable)));
                                }
                            }
                            final TableId tableId = tableSmallestLsn.getChangeTable().getSourceTableId();
                            final Lsn commitLsn = tableSmallestLsn.getCommitLsn();
                            final Lsn rowLsn = tableSmallestLsn.getRowLsn();
                            final int operation = tableSmallestLsn.getOperation();
                            final Object[] data = tableSmallestLsn.getData();

                            // UPDATE consists of two consecutive events, first event contains
                            // the row before it was updated and the second the row after
                            // it was updated
                            if (operation == SqlServerChangeRecordEmitter.OP_UPDATE_BEFORE) {
                                if (!tableSmallestLsn.next() || tableSmallestLsn.getOperation() != SqlServerChangeRecordEmitter.OP_UPDATE_AFTER) {
                                    throw new IllegalStateException("The update before event at " + rowLsn + " for table " + tableId + " was not followed by after event.\n Please report this as a bug together with a events around given LSN.");
                                }
                            }
                            final Object[] dataNext = (operation == SqlServerChangeRecordEmitter.OP_UPDATE_BEFORE) ? tableSmallestLsn.getData() : null;

                            offsetContext.setChangeLsn(rowLsn);
                            offsetContext.setCommitLsn(commitLsn);
                            offsetContext.setSourceTime(connection.timestampOfLsn(commitLsn));

                            dispatcher
                                    .dispatchDataChangeEvent(
                                            tableId,
                                            new SqlServerChangeRecordEmitter(
                                                    offsetContext,
                                                    operation,
                                                    data,
                                                    dataNext,
                                                    schema.tableFor(tableId),
                                                    clock
                                            )
                                    );
                            tableSmallestLsn.next();
                        }
                    });
                    lastProcessedLsn = currentMaxLsn;
                    // Terminate the transaction otherwise CDC could not be disabled for tables 
                    connection.rollback();
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
        final Set<ChangeTable> cdcEnabledTables = connection.listOfChangeTables();

        final Map<TableId, List<ChangeTable>> whitelistedCdcEnabledTables = cdcEnabledTables.stream()
                .filter(changeTable -> {
                    if (connectorConfig.getTableFilters().dataCollectionFilter().isIncluded(changeTable.getSourceTableId())) {
                        return true;
                    }
                    else {
                        LOGGER.info("CDC is enabled for table {} but the table is not whitelisted by connector");
                        return false;
                    }
                })
                .collect(Collectors.groupingBy(x -> x.getSourceTableId()));

        final List<ChangeTable> tables = new ArrayList<>();
        for (List<ChangeTable> captures: whitelistedCdcEnabledTables.values()) {
            ChangeTable changeTable = captures.get(0);
            if (captures.size() > 1) {
                if (captures.get(0).getStartLsn().compareTo(captures.get(1).getStartLsn()) < 0) {
                    captures.get(0).setStopLsn(captures.get(1).getStartLsn());
                    captures.get(0).setNextVersionOfTable(captures.get(1));
                    tables.add(captures.get(1));
                }
                else {
                    captures.get(1).setStopLsn(captures.get(0).getStartLsn());
                    captures.get(1).setNextVersionOfTable(captures.get(0));
                    changeTable = captures.get(1);
                    tables.add(captures.get(0));
                }
                LOGGER.info("Multiple capture instances {} and {} present for the same table", changeTable, captures.get(1));
            }
            if (schema.tableFor(changeTable.getSourceTableId()) == null) {
                LOGGER.info("Table {} is new to be monitored by capture instance {}", changeTable.getSourceTableId(), changeTable.getCaptureInstance());
                // We need to read the source table schema - primary key information cannot be obtained from change table
                dispatcher.dispatchSchemaChangeEvent(changeTable.getSourceTableId(), new SqlServerSchemaChangeEventEmitter(offsetContext, changeTable, connection.getTableSchemaFromTable(changeTable)));
            }
            tables.add(changeTable);
        }

        return tables.toArray(new ChangeTable[tables.size()]);
    }

    @Override
    public void commitOffset(Map<String, ?> offset) {
    }

    private static class ChangeTablePointer {

        private final ChangeTable changeTable;
        private final ResultSet resultSet;
        private boolean completed = false;
        private Lsn currentChangeLsn;

        public ChangeTablePointer(ChangeTable changeTable, ResultSet resultSet) {
            this.changeTable = changeTable;
            this.resultSet = resultSet;
        }

        public ChangeTable getChangeTable() {
            return changeTable;
        }

        public Lsn getCommitLsn() throws SQLException {
            return Lsn.valueOf(resultSet.getBytes(COL_COMMIT_LSN));
        }

        public Lsn getRowLsn() throws SQLException {
            return currentChangeLsn;
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
            currentChangeLsn = completed ? Lsn.NULL : Lsn.valueOf(resultSet.getBytes(COL_ROW_LSN));
            if (completed) {
                LOGGER.trace("Closing result set of change table for table {}", changeTable);
                resultSet.close();
            }
            return !completed;
        }

        public boolean isCompleted() {
            return completed;
        }

        public int compareTo(ChangeTablePointer o) throws SQLException {
            return getRowLsn().compareTo(o.getRowLsn());
        }

        @Override
        public String toString() {
            return "ChangeTable [changeTable=" + changeTable + ", resultSet=" + resultSet + ", completed=" + completed + "]";
        }
    }
}
