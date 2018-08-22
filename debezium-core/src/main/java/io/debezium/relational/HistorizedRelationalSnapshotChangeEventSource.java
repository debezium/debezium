/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;

import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.EventDispatcher.SnapshotReceiver;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.pipeline.spi.ChangeRecordEmitter;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.SnapshotResult;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.util.Clock;
import io.debezium.util.Strings;
import io.debezium.util.Threads;
import io.debezium.util.Threads.Timer;

/**
 * Base class for {@link SnapshotChangeEventSource} for relational databases with a schema history.
 * <p>
 * A transaction is managed by this base class, sub-classes shouldn't rollback or commit this transaction. They are free
 * to use nested transactions or savepoints, though.
 *
 * @author Gunnar Morling
 */
// TODO Mostly, this should be usable for Postgres as well; only the aspect of managing the schema history will have to
// be made optional based on the connector
public abstract class HistorizedRelationalSnapshotChangeEventSource implements SnapshotChangeEventSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(HistorizedRelationalSnapshotChangeEventSource.class);

    /**
     * Interval for showing a log statement with the progress while scanning a single table.
     */
    private static final Duration LOG_INTERVAL = Duration.ofMillis(10_000);

    private final RelationalDatabaseConnectorConfig connectorConfig;
    private final OffsetContext previousOffset;
    private final JdbcConnection jdbcConnection;
    private final HistorizedRelationalDatabaseSchema schema;
    private final EventDispatcher<TableId> dispatcher;
    private final Clock clock;

    public HistorizedRelationalSnapshotChangeEventSource(RelationalDatabaseConnectorConfig connectorConfig,
            OffsetContext previousOffset, JdbcConnection jdbcConnection, HistorizedRelationalDatabaseSchema schema,
            EventDispatcher<TableId> dispatcher, Clock clock) {
        this.connectorConfig = connectorConfig;
        this.previousOffset = previousOffset;
        this.jdbcConnection = jdbcConnection;
        this.schema = schema;
        this.dispatcher = dispatcher;
        this.clock = clock;
    }

    @Override
    public SnapshotResult execute(ChangeEventSourceContext context) throws InterruptedException {
        SnapshottingTask snapshottingTask = getSnapshottingTask(previousOffset);

        // Neither schema nor data require snapshotting
        if (!snapshottingTask.snapshotSchema() && !snapshottingTask.snapshotData()) {
            LOGGER.debug("Skipping snapshotting");
            return SnapshotResult.completed(previousOffset);
        }

        Connection connection = null;

        try (SnapshotContext ctx = prepare(context)) {
            LOGGER.info("Snapshot step 1 - Preparing");

            if (previousOffset != null && previousOffset.isSnapshotRunning()) {
                LOGGER.info("Previous snapshot was cancelled before completion; a new snapshot will be taken.");
            }

            connection = jdbcConnection.connection();
            connection.setAutoCommit(false);

            LOGGER.info("Snapshot step 2 - Determining captured tables");

            // Note that there's a minor race condition here: a new table matching the filters could be created between
            // this call and the determination of the initial snapshot position below; this seems acceptable, though
            determineCapturedTables(ctx);

            LOGGER.info("Snapshot step 3 - Locking captured tables");

            if (snapshottingTask.snapshotSchema()) {
                lockTablesForSchemaSnapshot(context, ctx);
            }

            LOGGER.info("Snapshot step 4 - Determining snapshot offset");
            determineSnapshotOffset(ctx);

            LOGGER.info("Snapshot step 5 - Reading structure of captured tables");
            readTableStructure(context, ctx);

            if (snapshottingTask.snapshotSchema()) {
                LOGGER.info("Snapshot step 6 - Persisting schema history");

                createSchemaChangeEventsForTables(context, ctx);

                // if we've been interrupted before, the TX rollback will cause any locks to be released
                releaseSchemaSnapshotLocks(ctx);
            }
            else {
                LOGGER.info("Snapshot step 6 - Skipping persisting of schema history");
            }

            if (snapshottingTask.snapshotData()) {
                LOGGER.info("Snapshot step 7 - Snapshotting data");
                createDataEvents(context, ctx);
            }
            else {
                LOGGER.info("Snapshot step 7 - Skipping snapshotting of data");
                ctx.offset.preSnapshotCompletion();
                ctx.offset.postSnapshotCompletion();
            }

            dispatcher.dispatchHeartbeatEvent(ctx.offset);
            return SnapshotResult.completed(ctx.offset);
        }
        catch(InterruptedException e) {
            LOGGER.warn("Snapshot was interrupted before completion");
            throw e;
        }
        catch(RuntimeException e) {
            throw e;
        }
        catch(Exception e) {
            throw new RuntimeException(e);
        }
        finally {
            rollbackTransaction(connection);

            LOGGER.info("Snapshot step 8 - Finalizing");

            complete();
        }
    }

    /**
     * Returns the snapshotting task based on the previous offset (if available) and the connector's snapshotting mode.
     */
    protected abstract SnapshottingTask getSnapshottingTask(OffsetContext previousOffset);

    /**
     * Prepares the taking of a snapshot and returns an initial {@link SnapshotContext}.
     */
    protected abstract SnapshotContext prepare(ChangeEventSourceContext changeEventSourceContext) throws Exception;

    private void determineCapturedTables(SnapshotContext ctx) throws Exception {
        Set<TableId> allTableIds = getAllTableIds(ctx);

        Set<TableId> capturedTables = new HashSet<>();

        for (TableId tableId : allTableIds) {
            if (connectorConfig.getTableFilters().dataCollectionFilter().isIncluded(tableId)) {
                LOGGER.trace("Adding table {} to the list of captured tables", tableId);
                capturedTables.add(tableId);
            }
            else {
                LOGGER.trace("Ignoring table {} as it's not included in the filter configuration", tableId);
            }
        }

        ctx.capturedTables = capturedTables;
    }

    /**
     * Returns all candidate tables; the current filter configuration will be applied to the result set, resulting in
     * the effective set of captured tables.
     */
    protected abstract Set<TableId> getAllTableIds(SnapshotContext snapshotContext) throws Exception;

    /**
     * Locks all tables to be captured, so that no concurrent schema changes can be applied to them.
     */
    protected abstract void lockTablesForSchemaSnapshot(ChangeEventSourceContext sourceContext, SnapshotContext snapshotContext) throws Exception;

    /**
     * Determines the current offset (MySQL binlog position, Oracle SCN etc.), storing it into the passed context
     * object. Subsequently, the DB's schema (and data) will be be read at this position. Once the snapshot is
     * completed, a {@link StreamingChangeEventSource} will be set up with this initial position to continue with stream
     * reading from there.
     */
    protected abstract void determineSnapshotOffset(SnapshotContext snapshotContext) throws Exception;

    /**
     * Reads the structure of all the captured tables, writing it to {@link SnapshotContext#tables}.
     */
    protected abstract void readTableStructure(ChangeEventSourceContext sourceContext, SnapshotContext snapshotContext) throws Exception;

    /**
     * Releases all locks established in order to create a consistent schema snapshot.
     */
    protected abstract void releaseSchemaSnapshotLocks(SnapshotContext snapshotContext) throws Exception;

    private void createSchemaChangeEventsForTables(ChangeEventSourceContext sourceContext, SnapshotContext snapshotContext) throws Exception {
        for (TableId tableId : snapshotContext.capturedTables) {
            if (!sourceContext.isRunning()) {
                throw new InterruptedException("Interrupted while capturing schema of table " + tableId);
            }

            LOGGER.debug("Capturing structure of table {}", tableId);

            Table table = snapshotContext.tables.forTable(tableId);

            schema.applySchemaChange(getCreateTableEvent(snapshotContext, table));
        }
    }

    /**
     * Creates a {@link SchemaChangeEvent} representing the creation of the given table.
     */
    protected abstract SchemaChangeEvent getCreateTableEvent(SnapshotContext snapshotContext, Table table) throws Exception;

    private void createDataEvents(ChangeEventSourceContext sourceContext, SnapshotContext snapshotContext) throws InterruptedException{
        SnapshotReceiver snapshotReceiver = dispatcher.getSnapshotChangeEventReceiver();
        snapshotContext.offset.preSnapshotStart();

        for (TableId tableId : snapshotContext.capturedTables) {
            if (!sourceContext.isRunning()) {
                throw new InterruptedException("Interrupted while snapshotting table " + tableId);
            }

            LOGGER.debug("Snapshotting table {}", tableId);

            createDataEventsForTable(sourceContext, snapshotContext, snapshotReceiver, snapshotContext.tables.forTable(tableId));
        }

        snapshotContext.offset.preSnapshotCompletion();
        snapshotReceiver.completeSnapshot();
        snapshotContext.offset.postSnapshotCompletion();
    }

    /**
     * Dispatches the data change events for the records of a single table.
     */
    private void createDataEventsForTable(ChangeEventSourceContext sourceContext, SnapshotContext snapshotContext, SnapshotReceiver snapshotReceiver,
            Table table) throws InterruptedException {

        long exportStart = clock.currentTimeInMillis();
        LOGGER.info("\t Exporting data from table '{}'", table.id());

        final String selectStatement = getSnapshotSelect(snapshotContext, table.id());
        LOGGER.info("\t For table '{}' using select statement: '{}'", table.id(), selectStatement);

        try (Statement statement = readTableStatement();
                ResultSet rs = statement.executeQuery(selectStatement)) {

            Column[] columns = getColumnsForResultSet(table, rs);
            final int numColumns = table.columns().size();
            int rows = 0;
            Timer logTimer = getTableScanLogTimer();

            while (rs.next()) {
                if (!sourceContext.isRunning()) {
                    throw new InterruptedException("Interrupted while snapshotting table " + table.id());
                }

                rows++;
                final Object[] row = new Object[numColumns];
                for (int i = 0; i < numColumns; i++) {
                    row[i] = getColumnValue(rs, i + 1, columns[i]);
                }

                if (logTimer.expired()) {
                    long stop = clock.currentTimeInMillis();
                    LOGGER.info("\t Exported {} records for table '{}' after {}", rows, table.id(),
                            Strings.duration(stop - exportStart));
                    logTimer = getTableScanLogTimer();
                }

                dispatcher.dispatchSnapshotEvent(table.id(), getChangeRecordEmitter(snapshotContext, row),
                        snapshotReceiver);
            }

            LOGGER.info("\t Finished exporting {} records for table '{}'; total duration '{}'", rows,
                    table.id(), Strings.duration(clock.currentTimeInMillis() - exportStart));
        }
        catch(SQLException e) {
            throw new ConnectException("Snapshotting of table " + table.id() + " failed", e);
        }
    }

    private Timer getTableScanLogTimer() {
        return Threads.timer(clock, LOG_INTERVAL);
    }

    /**
     * Returns a {@link ChangeRecordEmitter} producing the change records for the given table row.
     */
    protected abstract ChangeRecordEmitter getChangeRecordEmitter(SnapshotContext snapshotContext, Object[] row);

    /**
     * Returns the SELECT statement to be used for scanning the given table
     */
    // TODO Should it be Statement or similar?
    // TODO Handle override option generically; a problem will be how to handle the dynamic part (Oracle's "... as of
    // scn xyz")
    protected abstract String getSnapshotSelect(SnapshotContext snapshotContext, TableId tableId);

    private Column[] getColumnsForResultSet(Table table, ResultSet rs) throws SQLException {
        ResultSetMetaData metaData = rs.getMetaData();
        Column[] columns = new Column[metaData.getColumnCount()];

        for(int i = 0; i < columns.length; i++) {
            columns[i] = table.columnWithName(metaData.getColumnName(i + 1));
        }

        return columns;
    }

    private Object getColumnValue(ResultSet rs, int columnIndex, Column column) throws SQLException {
        return rs.getObject(columnIndex);
    }

    private Statement readTableStatement() throws SQLException {
        // TODO read option
        int rowsFetchSize = 2000;
        Statement statement = jdbcConnection.connection().createStatement(); // the default cursor is FORWARD_ONLY
        statement.setFetchSize(rowsFetchSize);
        return statement;
    }

    /**
     * Completes the snapshot, doing any required clean-up (resource disposal etc.).
     */
    protected abstract void complete();

    private void rollbackTransaction(Connection connection) {
        if(connection != null) {
            try {
                connection.rollback();
            }
            catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Mutable context which is populated in the course of snapshotting.
     */
    public static class SnapshotContext implements AutoCloseable {

        public final String catalogName;
        public final Tables tables;

        public Set<TableId> capturedTables;
        public OffsetContext offset;

        public SnapshotContext(String catalogName) throws SQLException {
            this.catalogName = catalogName;
            this.tables = new Tables();
        }

        @Override
        public void close() throws Exception {
        }
    }

    /**
     * A configuration describing the task to be performed during snapshotting.
     */
    public static class SnapshottingTask {

        private final boolean snapshotSchema;
        private final boolean snapshotData;

        public SnapshottingTask(boolean snapshotSchema, boolean snapshotData) {
            this.snapshotSchema = snapshotSchema;
            this.snapshotData = snapshotData;
        }

        /**
         * Whether data (rows in captured tables) should be snapshotted.
         */
        public boolean snapshotData() {
            return snapshotData;
        }

        /**
         * Whether the schema of captured tables should be snapshotted.
         */
        public boolean snapshotSchema() {
            return snapshotSchema;
        }

        @Override
        public String toString() {
            return "SnapshottingTask [snapshotSchema=" + snapshotSchema + ", snapshotData=" + snapshotData + "]";
        }
    }

    protected Clock getClock() {
        return clock;
    }
}
