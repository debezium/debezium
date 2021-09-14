/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.EventDispatcher.SnapshotReceiver;
import io.debezium.pipeline.source.AbstractSnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.pipeline.spi.ChangeRecordEmitter;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;
import io.debezium.pipeline.spi.SnapshotResult;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.util.Clock;
import io.debezium.util.ColumnUtils;
import io.debezium.util.Strings;
import io.debezium.util.Threads;
import io.debezium.util.Threads.Timer;

/**
 * Base class for {@link SnapshotChangeEventSource} for relational databases with or without a schema history.
 * <p>
 * A transaction is managed by this base class, sub-classes shouldn't rollback or commit this transaction. They are free
 * to use nested transactions or savepoints, though.
 *
 * @author Gunnar Morling
 */
public abstract class RelationalSnapshotChangeEventSource<P extends Partition, O extends OffsetContext> extends AbstractSnapshotChangeEventSource<P, O> {

    private static final Logger LOGGER = LoggerFactory.getLogger(RelationalSnapshotChangeEventSource.class);

    /**
     * Interval for showing a log statement with the progress while scanning a single table.
     */
    public static final Duration LOG_INTERVAL = Duration.ofMillis(10_000);

    public static final Pattern SELECT_ALL_PATTERN = Pattern.compile("\\*");

    private final RelationalDatabaseConnectorConfig connectorConfig;
    private final JdbcConnection jdbcConnection;
    private final RelationalDatabaseSchema schema;
    protected final EventDispatcher<TableId> dispatcher;
    protected final Clock clock;
    private final SnapshotProgressListener snapshotProgressListener;

    public RelationalSnapshotChangeEventSource(RelationalDatabaseConnectorConfig connectorConfig,
                                               JdbcConnection jdbcConnection, RelationalDatabaseSchema schema,
                                               EventDispatcher<TableId> dispatcher, Clock clock, SnapshotProgressListener snapshotProgressListener) {
        super(connectorConfig, snapshotProgressListener);
        this.connectorConfig = connectorConfig;
        this.jdbcConnection = jdbcConnection;
        this.schema = schema;
        this.dispatcher = dispatcher;
        this.clock = clock;
        this.snapshotProgressListener = snapshotProgressListener;
    }

    @Override
    public SnapshotResult<O> doExecute(ChangeEventSourceContext context, O previousOffset,
                                       SnapshotContext<P, O> snapshotContext, SnapshottingTask snapshottingTask)
            throws Exception {
        final RelationalSnapshotContext<P, O> ctx = (RelationalSnapshotContext<P, O>) snapshotContext;

        Connection connection = null;
        try {
            LOGGER.info("Snapshot step 1 - Preparing");

            if (previousOffset != null && previousOffset.isSnapshotRunning()) {
                LOGGER.info("Previous snapshot was cancelled before completion; a new snapshot will be taken.");
            }

            connection = createSnapshotConnection();
            connectionCreated(ctx);

            LOGGER.info("Snapshot step 2 - Determining captured tables");

            // Note that there's a minor race condition here: a new table matching the filters could be created between
            // this call and the determination of the initial snapshot position below; this seems acceptable, though
            determineCapturedTables(ctx);
            snapshotProgressListener.monitoredDataCollectionsDetermined(ctx.capturedTables);

            LOGGER.info("Snapshot step 3 - Locking captured tables {}", ctx.capturedTables);

            if (snapshottingTask.snapshotSchema()) {
                lockTablesForSchemaSnapshot(context, ctx);
            }

            LOGGER.info("Snapshot step 4 - Determining snapshot offset");
            determineSnapshotOffset(ctx, previousOffset);

            LOGGER.info("Snapshot step 5 - Reading structure of captured tables");
            readTableStructure(context, ctx, previousOffset);

            if (snapshottingTask.snapshotSchema()) {
                LOGGER.info("Snapshot step 6 - Persisting schema history");

                createSchemaChangeEventsForTables(context, ctx, snapshottingTask);

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
                releaseDataSnapshotLocks(ctx);
                ctx.offset.preSnapshotCompletion();
                ctx.offset.postSnapshotCompletion();
            }

            postSnapshot();
            dispatcher.alwaysDispatchHeartbeatEvent(ctx.partition, ctx.offset);
            return SnapshotResult.completed(ctx.offset);
        }
        finally {
            rollbackTransaction(connection);
        }
    }

    public Connection createSnapshotConnection() throws SQLException {
        Connection connection = jdbcConnection.connection();
        connection.setAutoCommit(false);
        return connection;
    }

    /**
     * Executes steps which have to be taken just after the database connection is created.
     */
    protected void connectionCreated(RelationalSnapshotContext<P, O> snapshotContext) throws Exception {
    }

    private Stream<TableId> toTableIds(Set<TableId> tableIds, Pattern pattern) {
        return tableIds
                .stream()
                .filter(tid -> pattern.asPredicate().test(connectorConfig.getTableIdMapper().toString(tid)))
                .sorted();
    }

    private Set<TableId> sort(Set<TableId> capturedTables) throws Exception {
        String tableIncludeList = connectorConfig.tableIncludeList();
        if (tableIncludeList != null) {
            return Strings.listOfRegex(tableIncludeList, Pattern.CASE_INSENSITIVE)
                    .stream()
                    .flatMap(pattern -> toTableIds(capturedTables, pattern))
                    .collect(Collectors.toCollection(LinkedHashSet::new));
        }
        return capturedTables
                .stream()
                .sorted()
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    private void determineCapturedTables(RelationalSnapshotContext<P, O> ctx) throws Exception {
        Set<TableId> allTableIds = determineDataCollectionsToBeSnapshotted(getAllTableIds(ctx)).collect(Collectors.toSet());

        Set<TableId> capturedTables = new HashSet<>();
        Set<TableId> capturedSchemaTables = new HashSet<>();

        for (TableId tableId : allTableIds) {
            if (connectorConfig.getTableFilters().eligibleDataCollectionFilter().isIncluded(tableId)) {
                LOGGER.trace("Adding table {} to the list of capture schema tables", tableId);
                capturedSchemaTables.add(tableId);
            }
            if (connectorConfig.getTableFilters().dataCollectionFilter().isIncluded(tableId)) {
                LOGGER.trace("Adding table {} to the list of captured tables", tableId);
                capturedTables.add(tableId);
            }
            else {
                LOGGER.trace("Ignoring table {} as it's not included in the filter configuration", tableId);
            }
        }

        ctx.capturedTables = sort(capturedTables);
        ctx.capturedSchemaTables = capturedSchemaTables
                .stream()
                .sorted()
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    /**
     * Returns all candidate tables; the current filter configuration will be applied to the result set, resulting in
     * the effective set of captured tables.
     */
    protected abstract Set<TableId> getAllTableIds(RelationalSnapshotContext<P, O> snapshotContext) throws Exception;

    /**
     * Locks all tables to be captured, so that no concurrent schema changes can be applied to them.
     */
    protected abstract void lockTablesForSchemaSnapshot(ChangeEventSourceContext sourceContext,
                                                        RelationalSnapshotContext<P, O> snapshotContext)
            throws Exception;

    /**
     * Determines the current offset (MySQL binlog position, Oracle SCN etc.), storing it into the passed context
     * object. Subsequently, the DB's schema (and data) will be be read at this position. Once the snapshot is
     * completed, a {@link StreamingChangeEventSource} will be set up with this initial position to continue with stream
     * reading from there.
     */
    protected abstract void determineSnapshotOffset(RelationalSnapshotContext<P, O> snapshotContext, O previousOffset)
            throws Exception;

    /**
     * Reads the structure of all the captured tables, writing it to {@link RelationalSnapshotContext#tables}.
     */
    protected abstract void readTableStructure(ChangeEventSourceContext sourceContext,
                                               RelationalSnapshotContext<P, O> snapshotContext, O offsetContext)
            throws Exception;

    /**
     * Releases all locks established in order to create a consistent schema snapshot.
     */
    protected abstract void releaseSchemaSnapshotLocks(RelationalSnapshotContext<P, O> snapshotContext)
            throws Exception;

    /**
     * Releases all locks established in order to create a consistent data snapshot.
     */
    protected void releaseDataSnapshotLocks(RelationalSnapshotContext<P, O> snapshotContext) throws Exception {
    }

    protected void createSchemaChangeEventsForTables(ChangeEventSourceContext sourceContext,
                                                     RelationalSnapshotContext<P, O> snapshotContext,
                                                     SnapshottingTask snapshottingTask)
            throws Exception {
        tryStartingSnapshot(snapshotContext);
        for (Iterator<TableId> iterator = snapshotContext.capturedTables.iterator(); iterator.hasNext();) {
            final TableId tableId = iterator.next();
            if (!sourceContext.isRunning()) {
                throw new InterruptedException("Interrupted while capturing schema of table " + tableId);
            }

            LOGGER.debug("Capturing structure of table {}", tableId);

            Table table = snapshotContext.tables.forTable(tableId);

            if (schema.isHistorized()) {
                snapshotContext.offset.event(tableId, getClock().currentTime());

                // If data are not snapshotted then the last schema change must set last snapshot flag
                if (!snapshottingTask.snapshotData() && !iterator.hasNext()) {
                    lastSnapshotRecord(snapshotContext);
                }

                dispatcher.dispatchSchemaChangeEvent(table.id(), (receiver) -> {
                    try {
                        receiver.schemaChangeEvent(getCreateTableEvent(snapshotContext, table));
                    }
                    catch (Exception e) {
                        throw new DebeziumException(e);
                    }
                });
            }
        }
    }

    /**
     * Creates a {@link SchemaChangeEvent} representing the creation of the given table.
     */
    protected abstract SchemaChangeEvent getCreateTableEvent(RelationalSnapshotContext<P, O> snapshotContext,
                                                             Table table)
            throws Exception;

    private void createDataEvents(ChangeEventSourceContext sourceContext,
                                  RelationalSnapshotContext<P, O> snapshotContext)
            throws Exception {
        SnapshotReceiver snapshotReceiver = dispatcher.getSnapshotChangeEventReceiver();
        tryStartingSnapshot(snapshotContext);

        final int tableCount = snapshotContext.capturedTables.size();
        int tableOrder = 1;
        LOGGER.info("Snapshotting contents of {} tables while still in transaction", tableCount);
        for (Iterator<TableId> tableIdIterator = snapshotContext.capturedTables.iterator(); tableIdIterator.hasNext();) {
            final TableId tableId = tableIdIterator.next();
            snapshotContext.lastTable = !tableIdIterator.hasNext();

            if (!sourceContext.isRunning()) {
                throw new InterruptedException("Interrupted while snapshotting table " + tableId);
            }

            LOGGER.debug("Snapshotting table {}", tableId);

            createDataEventsForTable(sourceContext, snapshotContext, snapshotReceiver, snapshotContext.tables.forTable(tableId), tableOrder++, tableCount);
        }

        releaseDataSnapshotLocks(snapshotContext);
        snapshotContext.offset.preSnapshotCompletion();
        snapshotReceiver.completeSnapshot();
        snapshotContext.offset.postSnapshotCompletion();
    }

    protected void tryStartingSnapshot(RelationalSnapshotContext<P, O> snapshotContext) {
        if (!snapshotContext.offset.isSnapshotRunning()) {
            snapshotContext.offset.preSnapshotStart();
        }
    }

    /**
     * Dispatches the data change events for the records of a single table.
     */
    private void createDataEventsForTable(ChangeEventSourceContext sourceContext,
                                          RelationalSnapshotContext<P, O> snapshotContext,
                                          SnapshotReceiver snapshotReceiver, Table table, int tableOrder,
                                          int tableCount)
            throws InterruptedException {

        long exportStart = clock.currentTimeInMillis();
        LOGGER.info("Exporting data from table '{}' ({} of {} tables)", table.id(), tableOrder, tableCount);

        final Optional<String> selectStatement = determineSnapshotSelect(snapshotContext, table.id());
        if (!selectStatement.isPresent()) {
            LOGGER.warn("For table '{}' the select statement was not provided, skipping table", table.id());
            snapshotProgressListener.dataCollectionSnapshotCompleted(table.id(), 0);
            return;
        }
        LOGGER.info("\t For table '{}' using select statement: '{}'", table.id(), selectStatement.get());
        final OptionalLong rowCount = rowCountForTable(table.id());

        try (Statement statement = readTableStatement(rowCount);
                ResultSet rs = statement.executeQuery(selectStatement.get())) {

            ColumnUtils.ColumnArray columnArray = ColumnUtils.toArray(rs, table);
            long rows = 0;
            Timer logTimer = getTableScanLogTimer();
            snapshotContext.lastRecordInTable = false;

            if (rs.next()) {
                while (!snapshotContext.lastRecordInTable) {
                    if (!sourceContext.isRunning()) {
                        throw new InterruptedException("Interrupted while snapshotting table " + table.id());
                    }

                    rows++;
                    final Object[] row = jdbcConnection.rowToArray(table, schema(), rs, columnArray);

                    snapshotContext.lastRecordInTable = !rs.next();
                    if (logTimer.expired()) {
                        long stop = clock.currentTimeInMillis();
                        if (rowCount.isPresent()) {
                            LOGGER.info("\t Exported {} of {} records for table '{}' after {}", rows, rowCount.getAsLong(),
                                    table.id(), Strings.duration(stop - exportStart));
                        }
                        else {
                            LOGGER.info("\t Exported {} records for table '{}' after {}", rows, table.id(),
                                    Strings.duration(stop - exportStart));
                        }
                        snapshotProgressListener.rowsScanned(table.id(), rows);
                        logTimer = getTableScanLogTimer();
                    }

                    if (snapshotContext.lastTable && snapshotContext.lastRecordInTable) {
                        lastSnapshotRecord(snapshotContext);
                    }
                    dispatcher.dispatchSnapshotEvent(table.id(), getChangeRecordEmitter(snapshotContext, table.id(), row), snapshotReceiver);
                }
            }
            else if (snapshotContext.lastTable) {
                lastSnapshotRecord(snapshotContext);
            }

            LOGGER.info("\t Finished exporting {} records for table '{}'; total duration '{}'", rows,
                    table.id(), Strings.duration(clock.currentTimeInMillis() - exportStart));
            snapshotProgressListener.dataCollectionSnapshotCompleted(table.id(), rows);
        }
        catch (SQLException e) {
            throw new ConnectException("Snapshotting of table " + table.id() + " failed", e);
        }
    }

    protected void lastSnapshotRecord(RelationalSnapshotContext<P, O> snapshotContext) {
        snapshotContext.offset.markLastSnapshotRecord();
    }

    /**
     * If connector is able to provide statistics-based number of records per table.
     */
    protected OptionalLong rowCountForTable(TableId tableId) {
        return OptionalLong.empty();
    }

    private Timer getTableScanLogTimer() {
        return Threads.timer(clock, LOG_INTERVAL);
    }

    /**
     * Returns a {@link ChangeRecordEmitter} producing the change records for the given table row.
     */
    protected ChangeRecordEmitter getChangeRecordEmitter(SnapshotContext<P, O> snapshotContext, TableId tableId,
                                                         Object[] row) {
        snapshotContext.offset.event(tableId, getClock().currentTime());
        return new SnapshotChangeRecordEmitter(snapshotContext.partition, snapshotContext.offset, row, getClock());
    }

    /**
     * Returns a valid query string for the specified table, either given by the user via snapshot select overrides or
     * defaulting to a statement provided by the DB-specific change event source.
     *
     * @param tableId the table to generate a query for
     * @return a valid query string or empty if table will not be snapshotted
     */
    private Optional<String> determineSnapshotSelect(RelationalSnapshotContext<P, O> snapshotContext, TableId tableId) {
        String overriddenSelect = connectorConfig.getSnapshotSelectOverridesByTable().get(tableId);

        // try without catalog id, as this might or might not be populated based on the given connector
        if (overriddenSelect == null) {
            overriddenSelect = connectorConfig.getSnapshotSelectOverridesByTable().get(new TableId(null, tableId.schema(), tableId.table()));
        }

        if (overriddenSelect != null) {
            return Optional.of(enhanceOverriddenSelect(snapshotContext, overriddenSelect, tableId));
        }

        List<String> columns = getPreparedColumnNames(schema.tableFor(tableId));

        return getSnapshotSelect(snapshotContext, tableId, columns);
    }

    /**
     * Prepares a list of columns to be used in the snapshot select.
     * The selected columns are based on the column include/exclude filters and if all columns are excluded,
     * the list will contain all the primary key columns.
     *
     * @return list of snapshot select columns
     */
    protected List<String> getPreparedColumnNames(Table table) {
        List<String> columnNames = table.retrieveColumnNames()
                .stream()
                .filter(columnName -> additionalColumnFilter(table.id(), columnName))
                .filter(columnName -> connectorConfig.getColumnFilter().matches(table.id().catalog(), table.id().schema(), table.id().table(), columnName))
                .map(columnName -> jdbcConnection.quotedColumnIdString(columnName))
                .collect(Collectors.toList());

        if (columnNames.isEmpty()) {
            LOGGER.info("\t All columns in table {} were excluded due to include/exclude lists, defaulting to selecting all columns", table.id());

            columnNames = table.retrieveColumnNames()
                    .stream()
                    .map(columnName -> jdbcConnection.quotedColumnIdString(columnName))
                    .collect(Collectors.toList());
        }

        return columnNames;
    }

    /**
     * Additional filter handling for preparing column names for snapshot select
     */
    protected boolean additionalColumnFilter(TableId tableId, String columnName) {
        return true;
    }

    /**
     * This method is overridden for Oracle to implement "as of SCN" predicate
     * @param snapshotContext snapshot context, used for getting offset SCN
     * @param overriddenSelect conditional snapshot select
     * @return enhanced select statement. By default it just returns original select statements.
     */
    protected String enhanceOverriddenSelect(RelationalSnapshotContext<P, O> snapshotContext, String overriddenSelect,
                                             TableId tableId) {
        return overriddenSelect;
    }

    /**
     * Returns the SELECT statement to be used for scanning the given table or empty value if
     * the table will be streamed from but not snapshotted
     */
    // TODO Should it be Statement or similar?
    // TODO Handle override option generically; a problem will be how to handle the dynamic part (Oracle's "... as of
    // scn xyz")
    protected abstract Optional<String> getSnapshotSelect(RelationalSnapshotContext<P, O> snapshotContext,
                                                          TableId tableId, List<String> columns);

    protected RelationalDatabaseSchema schema() {
        return schema;
    }

    protected Object getColumnValue(ResultSet rs, int columnIndex, Column column, Table table) throws SQLException {
        return jdbcConnection.getColumnValue(rs, columnIndex, column, table, schema());
    }

    /**
     * Allow per-connector query creation to override for best database performance depending on the table size.
     */
    protected Statement readTableStatement(OptionalLong tableSize) throws SQLException {
        return jdbcConnection.readTableStatement(connectorConfig, tableSize);
    }

    private void rollbackTransaction(Connection connection) {
        if (connection != null) {
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
    public static class RelationalSnapshotContext<P extends Partition, O extends OffsetContext>
            extends SnapshotContext<P, O> {
        public final String catalogName;
        public final Tables tables;

        public Set<TableId> capturedTables;
        public Set<TableId> capturedSchemaTables;
        public boolean lastTable;
        public boolean lastRecordInTable;

        public RelationalSnapshotContext(P partition, String catalogName) throws SQLException {
            super(partition);
            this.catalogName = catalogName;
            this.tables = new Tables();
        }
    }

    protected Clock getClock() {
        return clock;
    }

    protected void postSnapshot() throws InterruptedException {
    }
}
