/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Map;

import org.apache.kafka.connect.errors.ConnectException;
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

    private static final Logger LOGGER = LoggerFactory.getLogger(SqlServerStreamingChangeEventSource.class);

    private final SqlServerConnection connection;
    private final EventDispatcher<TableId> dispatcher;
    private final ErrorHandler errorHandler;
    private final Clock clock;
    private final SqlServerDatabaseSchema schema;
    private final SqlServerOffsetContext offsetContext;
    private final Duration pollInterval;

    public SqlServerStreamingChangeEventSource(SqlServerConnectorConfig connectorConfig, SqlServerOffsetContext offsetContext, SqlServerConnection connection, EventDispatcher<TableId> dispatcher, ErrorHandler errorHandler, Clock clock, SqlServerDatabaseSchema schema) {
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
        try {
            final TableId[] tables = schema.getCapturedTables().toArray(new TableId[schema.getCapturedTables().size()]);
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

                connection.getChangesForTables(tables, fromLsn, currentMaxLsn, resultSets -> {
                    final int tableCount = resultSets.length;
                    final ChangeTable[] changeTables = new ChangeTable[tableCount];

                    for (int i = 0; i < tableCount; i++) {
                        changeTables[i] = new ChangeTable(tables[i], resultSets[i]);
                        changeTables[i].next();
                    }

                    for (;;) {
                        ChangeTable tableSmallestLsn = null;
                        for (int i = 0; i < tableCount; i++) {
                            final ChangeTable changeTable = changeTables[i];
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

                        LOGGER.trace("Processing change {}", tableSmallestLsn);
                        final TableId tableId = tableSmallestLsn.getTableId();
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

                        try {
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
                        }
                        catch (InterruptedException e) {
                            break;
                        }
                        tableSmallestLsn.next();
                    }
                });
                lastProcessedLsn = currentMaxLsn;
            }
        }
        catch (Exception e) {
            throw new ConnectException(e);
        }
    }

    @Override
    public void commitOffset(Map<String, ?> offset) {
    }

    private static class ChangeTable {

        private final TableId tableId;
        private final ResultSet resultSet;
        private boolean completed = false;
        private Lsn currentChangeLsn;

        public ChangeTable(TableId tableId, ResultSet resultSet) {
            this.tableId = tableId;
            this.resultSet = resultSet;
        }

        public TableId getTableId() {
            return tableId;
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
            return !completed;
        }

        public boolean isCompleted() {
            return completed;
        }

        public int compareTo(ChangeTable o) throws SQLException {
            return getRowLsn().compareTo(o.getRowLsn());
        }

        @Override
        public String toString() {
            return "ChangeTable [tableId=" + tableId + ", resultSet=" + resultSet + ", completed=" + completed + "]";
        }
    }

}
