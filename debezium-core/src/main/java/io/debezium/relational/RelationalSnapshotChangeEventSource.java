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
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.EventDispatcher.SnapshotReceiver;
import io.debezium.pipeline.source.AbstractSnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
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
 * Base class for {@link SnapshotChangeEventSource} for relational databases with or without a schema history.
 * <p>
 * A transaction is managed by this base class, sub-classes shouldn't rollback or commit this transaction. They are free
 * to use nested transactions or savepoints, though.
 *
 * @author Gunnar Morling
 */
public abstract class RelationalSnapshotChangeEventSource extends AbstractSnapshotChangeEventSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(RelationalSnapshotChangeEventSource.class);

    /**
     * Interval for showing a log statement with the progress while scanning a single table.
     */
    private static final Duration LOG_INTERVAL = Duration.ofMillis(10_000);

    private final RelationalDatabaseConnectorConfig connectorConfig;
    private final OffsetContext previousOffset;
    private final JdbcConnection jdbcConnection;
    private final HistorizedRelationalDatabaseSchema schema;
    private final EventDispatcher<TableId> dispatcher;
    protected final Clock clock;
    private final SnapshotProgressListener snapshotProgressListener;

    public RelationalSnapshotChangeEventSource(RelationalDatabaseConnectorConfig connectorConfig,
                                               OffsetContext previousOffset, JdbcConnection jdbcConnection, HistorizedRelationalDatabaseSchema schema,
                                               EventDispatcher<TableId> dispatcher, Clock clock, SnapshotProgressListener snapshotProgressListener) {
        super(connectorConfig, previousOffset, snapshotProgressListener);
        this.connectorConfig = connectorConfig;
        this.previousOffset = previousOffset;
        this.jdbcConnection = jdbcConnection;
        this.schema = schema;
        this.dispatcher = dispatcher;
        this.clock = clock;
        this.snapshotProgressListener = snapshotProgressListener;
    }

    public RelationalSnapshotChangeEventSource(RelationalDatabaseConnectorConfig connectorConfig,
                                               OffsetContext previousOffset, JdbcConnection jdbcConnection,
                                               EventDispatcher<TableId> dispatcher, Clock clock, SnapshotProgressListener snapshotProgressListener) {
        this(connectorConfig, previousOffset, jdbcConnection, null, dispatcher, clock, snapshotProgressListener);
    }

    @Override
    public SnapshotResult doExecute(ChangeEventSourceContext context, SnapshotContext snapshotContext, SnapshottingTask snapshottingTask)
            throws Exception {
        final RelationalSnapshotContext ctx = (RelationalSnapshotContext) snapshotContext;

        Connection connection = null;
        try {
            LOGGER.info("Snapshot step 1 - Preparing");
            snapshotProgressListener.snapshotStarted();

            if (previousOffset != null && previousOffset.isSnapshotRunning()) {
                LOGGER.info("Previous snapshot was cancelled before completion; a new snapshot will be taken.");
            }

            connection = jdbcConnection.connection();
            connection.setAutoCommit(false);
            connectionCreated(ctx);

            LOGGER.info("Snapshot step 2 - Determining captured tables");

            // Note that there's a minor race condition here: a new table matching the filters could be created between
            // this call and the determination of the initial snapshot position below; this seems acceptable, though
            determineCapturedTables(ctx);
            snapshotProgressListener.monitoredDataCollectionsDetermined(ctx.capturedTables);

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
                ctx.offset.preSnapshotCompletion();
                ctx.offset.postSnapshotCompletion();
            }

            dispatcher.alwaysDispatchHeartbeatEvent(ctx.offset);
            snapshotProgressListener.snapshotCompleted();
            return SnapshotResult.completed(ctx.offset);
        }
        finally {
            rollbackTransaction(connection);
        }
    }

    /**
     * Executes steps which have to be taken just after the database connection is created.
     */
    protected void connectionCreated(RelationalSnapshotContext snapshotContext) throws Exception {
    }

    private Stream<TableId> toTableIds(Set<TableId> tableIds, Pattern pattern) {
        return tableIds
                .stream()
                .filter(tid -> pattern.asPredicate().test(tid.toString()))
                .sorted();
    }

    private Set<TableId> sort(Set<TableId> capturedTables) throws Exception {
        String value = connectorConfig.getConfig().getString(RelationalDatabaseConnectorConfig.TABLE_WHITELIST);
        if (value != null) {
            return Strings.listOfRegex(value, Pattern.CASE_INSENSITIVE)
                    .stream()
                    .flatMap(pattern -> toTableIds(capturedTables, pattern))
                    .collect(Collectors.toCollection(LinkedHashSet::new));
        }
        return capturedTables
                .stream()
                .sorted()
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    private void determineCapturedTables(RelationalSnapshotContext ctx) throws Exception {
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

        ctx.capturedTables = sort(capturedTables);
    }

    /**
     * Returns all candidate tables; the current filter configuration will be applied to the result set, resulting in
     * the effective set of captured tables.
     */
    protected abstract Set<TableId> getAllTableIds(RelationalSnapshotContext snapshotContext) throws Exception;

    /**
     * Locks all tables to be captured, so that no concurrent schema changes can be applied to them.
     */
    protected abstract void lockTablesForSchemaSnapshot(ChangeEventSourceContext sourceContext, RelationalSnapshotContext snapshotContext) throws Exception;

    /**
     * Determines the current offset (MySQL binlog position, Oracle SCN etc.), storing it into the passed context
     * object. Subsequently, the DB's schema (and data) will be be read at this position. Once the snapshot is
     * completed, a {@link StreamingChangeEventSource} will be set up with this initial position to continue with stream
     * reading from there.
     */
    protected abstract void determineSnapshotOffset(RelationalSnapshotContext snapshotContext) throws Exception;

    /**
     * Reads the structure of all the captured tables, writing it to {@link RelationalSnapshotContext#tables}.
     */
    protected abstract void readTableStructure(ChangeEventSourceContext sourceContext, RelationalSnapshotContext snapshotContext) throws Exception;

    /**
     * Releases all locks established in order to create a consistent schema snapshot.
     */
    protected abstract void releaseSchemaSnapshotLocks(RelationalSnapshotContext snapshotContext) throws Exception;

    private void createSchemaChangeEventsForTables(ChangeEventSourceContext sourceContext, RelationalSnapshotContext snapshotContext, SnapshottingTask snapshottingTask)
            throws Exception {
        tryStartingSnapshot(snapshotContext);
        for (Iterator<TableId> iterator = snapshotContext.capturedTables.iterator(); iterator.hasNext();) {
            final TableId tableId = iterator.next();
            if (!sourceContext.isRunning()) {
                throw new InterruptedException("Interrupted while capturing schema of table " + tableId);
            }

            LOGGER.debug("Capturing structure of table {}", tableId);

            Table table = snapshotContext.tables.forTable(tableId);

            if (schema != null) {
                snapshotContext.offset.event(tableId, getClock().currentTime());

                // If data are not snapshotted then the last schema change must set last snapshot flag
                if (!snapshottingTask.snapshotData() && !iterator.hasNext()) {
                    snapshotContext.offset.markLastSnapshotRecord();
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
    protected abstract SchemaChangeEvent getCreateTableEvent(RelationalSnapshotContext snapshotContext, Table table) throws Exception;

    private void createDataEvents(ChangeEventSourceContext sourceContext, RelationalSnapshotContext snapshotContext) throws InterruptedException {
        tryStartingSnapshot(snapshotContext);
        runSnapshot(sourceContext, snapshotContext);
        snapshotContext.offset.postSnapshotCompletion();
    }

    private void runSnapshot(ChangeEventSourceContext sourceContext, RelationalSnapshotContext snapshotContext)
            throws InterruptedException {
        int poolSize = snapshotWorkerThreadCount(snapshotContext);
        LOGGER.info("Creating snapshot worker pool with {} worker thread(s)", poolSize);
        ExecutorService executorService = Executors.newFixedThreadPool(poolSize);
        CompletionService<SnapshotWorkerResult> completionService = new ExecutorCompletionService<>(executorService);

        SnapshotWorkerResult lastSnapshotWorkerResult;
        try {
            queueTablesForSnapshot(sourceContext, snapshotContext, completionService);
            lastSnapshotWorkerResult = waitForSnapshotToFinish(snapshotContext, completionService);
        }
        catch (ExecutionException e) {
            LOGGER.error("A snapshot worker failed", e);
            throw new InterruptedException("Snapshot failed to complete");
        }
        finally {
            executorService.shutdownNow();
        }

        // Set snapshot completion on master offset and last event offset
        snapshotContext.offset.preSnapshotCompletion();
        if (lastSnapshotWorkerResult != null) {
            lastSnapshotWorkerResult.tableOffsetContext.markLastSnapshotRecord();
            lastSnapshotWorkerResult.tableOffsetContext.preSnapshotCompletion();
            lastSnapshotWorkerResult.snapshotReceiver.completeSnapshot();
        }
    }

    private void queueTablesForSnapshot(ChangeEventSourceContext sourceContext, RelationalSnapshotContext snapshotContext,
                                        CompletionService<SnapshotWorkerResult> completionService)
            throws InterruptedException {
        for (final TableId tableId : snapshotContext.capturedTables) {
            if (!sourceContext.isRunning()) {
                throw new InterruptedException("Interrupted while snapshotting table " + tableId);
            }

            LOGGER.debug("Snapshotting table {}", tableId);

            SnapshotReceiver snapshotReceiver = dispatcher.getSnapshotChangeEventReceiver();
            SnapshotWorker worker = new SnapshotWorker(tableId, sourceContext, snapshotContext, snapshotReceiver);
            completionService.submit(worker);
        }
    }

    private SnapshotWorkerResult waitForSnapshotToFinish(RelationalSnapshotContext snapshotContext,
                                                         CompletionService<SnapshotWorkerResult> completionService)
            throws ExecutionException, InterruptedException {
        SnapshotWorkerResult lastResults = null;

        for (int i = 0; i < snapshotContext.capturedTables.size(); i++) {
            Future<SnapshotWorkerResult> future = completionService.take();
            SnapshotWorkerResult snapshotWorkerResult = future.get();
            if (snapshotWorkerResult.snapshotReceiver.hasEventStaged()) {
                // Look for the last table to finish. Maintains table order with a single worker.
                if (lastResults != null) {
                    lastResults.snapshotReceiver.flushStagedEvent();
                }
                lastResults = snapshotWorkerResult;
            }
        }
        return lastResults;
    }

    private void snapshotTable(Table table, ChangeEventSourceContext sourceContext, RelationalSnapshotContext snapshotContext,
                               RelationalTableSnapshotContext tableSnapshotContext) {
        try {
            if (!sourceContext.isRunning()) {
                throw new InterruptedException("Interrupted while snapshotting table " + table.id());
            }

            LOGGER.debug("Snapshotting table {}", table.id());

            createDataEventsForTable(sourceContext, snapshotContext, tableSnapshotContext, table);
        }
        catch (InterruptedException e) {
            LOGGER.error("Snapshot worker failed", e);
        }
    }

    private void tryStartingSnapshot(RelationalSnapshotContext snapshotContext) {
        if (!snapshotContext.offset.isSnapshotRunning()) {
            snapshotContext.offset.preSnapshotStart();
        }
    }

    /**
     * Dispatches the data change events for the records of a single table.
     */
    private void createDataEventsForTable(ChangeEventSourceContext sourceContext, RelationalSnapshotContext snapshotContext,
                                          RelationalTableSnapshotContext tableSnapshotContext, Table table)
            throws InterruptedException {

        long exportStart = clock.currentTimeInMillis();
        LOGGER.info("\t Exporting data from table '{}'", table.id());

        final Optional<String> selectStatement = determineSnapshotSelect(snapshotContext, table.id());
        if (!selectStatement.isPresent()) {
            LOGGER.warn("For table '{}' the select statement was not provided, skipping table", table.id());
            return;
        }
        LOGGER.info("\t For table '{}' using select statement: '{}'", table.id(), selectStatement.get());

        long rows;
        try {
            Connection connection = null;
            try {
                connection = getSnapshotWorkerConnection(snapshotContext);
                try (Statement statement = readTableStatement(connection, snapshotContext);
                        ResultSet rs = statement.executeQuery(selectStatement.get())) {
                    rows = processSnapshotResultSet(rs, tableSnapshotContext, sourceContext, exportStart);
                }
            }
            finally {
                if (connection != null && isUsingConcurrentSnapshot(snapshotContext)) {
                    connection.close();
                }
            }
        }
        catch (SQLException e) {
            throw new ConnectException("Snapshotting of table " + table.id() + " failed", e);
        }

        LOGGER.info("\t Finished exporting {} records for table '{}'; total duration '{}'", rows,
                table.id(), Strings.duration(clock.currentTimeInMillis() - exportStart));
        snapshotProgressListener.dataCollectionSnapshotCompleted(table.id(), rows);
    }

    private long processSnapshotResultSet(ResultSet rs, RelationalTableSnapshotContext tableSnapshotContext,
                                          ChangeEventSourceContext sourceContext, long exportStart)
            throws SQLException, InterruptedException {
        Column[] columns = getColumnsForResultSet(tableSnapshotContext.getTable(), rs);
        final int numColumns = tableSnapshotContext.getTable().columns().size();
        long rows = 0;
        Timer logTimer = getTableScanLogTimer();
        boolean isLastRecordInTable = false;

        if (rs.next()) {
            while (!isLastRecordInTable) {
                if (!sourceContext.isRunning()) {
                    throw new InterruptedException("Interrupted while snapshotting table " + tableSnapshotContext.getTable().id());
                }

                rows++;
                final Object[] row = new Object[numColumns];
                for (int i = 0; i < numColumns; i++) {
                    row[i] = getColumnValue(rs, i + 1, columns[i]);
                }

                isLastRecordInTable = !rs.next();
                if (logTimer.expired()) {
                    long stop = clock.currentTimeInMillis();
                    LOGGER.info("\t Exported {} records for table '{}' after {}", rows, tableSnapshotContext.getTable().id(),
                            Strings.duration(stop - exportStart));
                    snapshotProgressListener.rowsScanned(tableSnapshotContext.getTable().id(), rows);
                    logTimer = getTableScanLogTimer();
                }

                ChangeRecordEmitter changeRecordEmitter = getChangeRecordEmitter(tableSnapshotContext, tableSnapshotContext.getTable().id(), row);
                dispatcher.dispatchSnapshotEvent(tableSnapshotContext.getTable().id(), changeRecordEmitter, tableSnapshotContext.getSnapshotReceiver());
            }
        }

        return rows;
    }

    private Timer getTableScanLogTimer() {
        return Threads.timer(clock, LOG_INTERVAL);
    }

    /**
     * Returns a {@link ChangeRecordEmitter} producing the change records for the given table row.
     */
    protected ChangeRecordEmitter getChangeRecordEmitter(RelationalTableSnapshotContext snapshotContext, TableId tableId, Object[] row) {
        snapshotContext.offset.event(tableId, getClock().currentTime());
        return new SnapshotChangeRecordEmitter(snapshotContext.offset, row, getClock());
    }

    /**
     * Returns a valid query string for the specified table, either given by the user via snapshot select overrides or
     * defaulting to a statement provided by the DB-specific change event source.
     *
     * @param tableId the table to generate a query for
     * @return a valid query string or empty if table will not be snapshotted
     */
    private Optional<String> determineSnapshotSelect(SnapshotContext snapshotContext, TableId tableId) {
        String overriddenSelect = connectorConfig.getSnapshotSelectOverridesByTable().get(tableId);

        // try without catalog id, as this might or might not be populated based on the given connector
        if (overriddenSelect == null) {
            overriddenSelect = connectorConfig.getSnapshotSelectOverridesByTable().get(new TableId(null, tableId.schema(), tableId.table()));
        }

        return overriddenSelect != null ? Optional.of(overriddenSelect) : getSnapshotSelect(snapshotContext, tableId);
    }

    /**
     * Returns the SELECT statement to be used for scanning the given table or empty value if
     * the table will be streamed from but not snapshotted
     */
    // TODO Should it be Statement or similar?
    // TODO Handle override option generically; a problem will be how to handle the dynamic part (Oracle's "... as of
    // scn xyz")
    protected abstract Optional<String> getSnapshotSelect(SnapshotContext snapshotContext, TableId tableId);

    private Column[] getColumnsForResultSet(Table table, ResultSet rs) throws SQLException {
        ResultSetMetaData metaData = rs.getMetaData();
        Column[] columns = new Column[metaData.getColumnCount()];

        for (int i = 0; i < columns.length; i++) {
            columns[i] = table.columnWithName(metaData.getColumnName(i + 1));
        }

        return columns;
    }

    protected Object getColumnValue(ResultSet rs, int columnIndex, Column column) throws SQLException {
        return rs.getObject(columnIndex);
    }

    private Connection getSnapshotWorkerConnection(RelationalSnapshotContext snapshotContext) throws SQLException {
        if (isUsingConcurrentSnapshot(snapshotContext)) {
            return jdbcConnection.workerConnection(true);
        }

        return jdbcConnection.connection();
    }

    private Statement readTableStatement(Connection connection, RelationalSnapshotContext snapshotContext) throws SQLException {
        int fetchSize = connectorConfig.getSnapshotFetchSize();
        // the default cursor is FORWARD_ONLY
        Statement statement = connection.createStatement();
        prepareSnapshotWorker(statement, snapshotContext);
        statement.setFetchSize(fetchSize);
        return statement;
    }

    protected void prepareSnapshotWorker(Statement statement, RelationalSnapshotContext snapshotContext) throws SQLException {
        // No-op
    }

    protected abstract boolean supportsConcurrentSnapshot();

    private int snapshotWorkerThreadCount(RelationalSnapshotContext snapshotContext) {
        if (supportsConcurrentSnapshot()) {
            int maxNumThreads = connectorConfig.getConfig().getInteger(CommonConnectorConfig.SNAPSHOT_MAX_THREADS);
            if (snapshotContext.capturedTables.size() > 0) {
                return Math.min(snapshotContext.capturedTables.size(), maxNumThreads);
            }
        }

        return 1;
    }

    protected boolean isUsingConcurrentSnapshot(RelationalSnapshotContext snapshotContext) {
        return snapshotWorkerThreadCount(snapshotContext) > 1;
    }

    /**
     * Completes the snapshot, doing any required clean-up (resource disposal etc.).
     * @param snapshotContext snapshot context
     */
    protected abstract void complete(SnapshotContext snapshotContext);

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
    public static class RelationalSnapshotContext extends SnapshotContext {
        public final String catalogName;
        public final Tables tables;

        public Set<TableId> capturedTables;

        public RelationalSnapshotContext(String catalogName) {
            this.catalogName = catalogName;
            this.tables = new Tables();
        }
    }

    protected static class RelationalTableSnapshotContext extends SnapshotContext {
        private final Table table;
        private final SnapshotReceiver snapshotReceiver;

        public RelationalTableSnapshotContext(Table table, OffsetContext offset, SnapshotReceiver snapshotReceiver) {
            // We want to avoid sharing the mutable offset context across threads so a copy is used
            this.offset = offset;
            this.table = table;
            this.snapshotReceiver = snapshotReceiver;
        }

        public Table getTable() {
            return table;
        }

        public SnapshotReceiver getSnapshotReceiver() {
            return snapshotReceiver;
        }

    }

    class SnapshotWorker implements Callable<SnapshotWorkerResult> {

        private final Table table;
        private final ChangeEventSourceContext sourceContext;
        private final RelationalSnapshotContext snapshotContext;
        private final RelationalTableSnapshotContext tableSnapshotContext;

        public SnapshotWorker(TableId tableId, ChangeEventSourceContext sourceContext,
                              RelationalSnapshotContext snapshotContext, SnapshotReceiver snapshotReceiver) {
            this.table = snapshotContext.tables.forTable(tableId);
            this.sourceContext = sourceContext;
            this.snapshotContext = snapshotContext;

            OffsetContext workerOffset;
            if (isUsingConcurrentSnapshot(snapshotContext)) {
                workerOffset = snapshotContext.offset.getDataCollectionOffsetContext(table.id());
            }
            else {
                workerOffset = snapshotContext.offset;
            }

            this.tableSnapshotContext = new RelationalTableSnapshotContext(table, workerOffset, snapshotReceiver);
        }

        @Override
        public SnapshotWorkerResult call() {
            snapshotTable(table, sourceContext, snapshotContext, tableSnapshotContext);
            return new SnapshotWorkerResult(tableSnapshotContext.getSnapshotReceiver(), tableSnapshotContext.offset);
        }
    }

    public static class SnapshotWorkerResult {
        public final SnapshotReceiver snapshotReceiver;
        public final OffsetContext tableOffsetContext;

        public SnapshotWorkerResult(SnapshotReceiver snapshotReceiver, OffsetContext tableOffsetContext) {
            this.snapshotReceiver = snapshotReceiver;
            this.tableOffsetContext = tableOffsetContext;
        }
    }

    protected Clock getClock() {
        return clock;
    }
}
