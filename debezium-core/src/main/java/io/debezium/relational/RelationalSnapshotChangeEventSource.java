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
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.SnapshotRecord;
import io.debezium.jdbc.CancellableResultSet;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.EventDispatcher.SnapshotReceiver;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.AbstractSnapshotChangeEventSource;
import io.debezium.pipeline.source.SnapshottingTask;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.pipeline.spi.ChangeRecordEmitter;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;
import io.debezium.pipeline.spi.SnapshotResult;
import io.debezium.relational.RelationalDatabaseConnectorConfig.SnapshotTablesRowCountOrder;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.spi.schema.DataCollectionId;
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

    public static final Pattern SELECT_ALL_PATTERN = Pattern.compile("\\*");
    public static final Pattern MATCH_ALL_PATTERN = Pattern.compile(".*");

    private final RelationalDatabaseConnectorConfig connectorConfig;
    private final JdbcConnection jdbcConnection;
    private final MainConnectionProvidingConnectionFactory<? extends JdbcConnection> jdbcConnectionFactory;
    private final RelationalDatabaseSchema schema;
    protected final EventDispatcher<P, TableId> dispatcher;
    protected final Clock clock;
    private final SnapshotProgressListener<P> snapshotProgressListener;
    protected Queue<JdbcConnection> connectionPool;

    public RelationalSnapshotChangeEventSource(RelationalDatabaseConnectorConfig connectorConfig,
                                               MainConnectionProvidingConnectionFactory<? extends JdbcConnection> jdbcConnectionFactory,
                                               RelationalDatabaseSchema schema, EventDispatcher<P, TableId> dispatcher, Clock clock,
                                               SnapshotProgressListener<P> snapshotProgressListener, NotificationService<P, O> notificationService) {
        super(connectorConfig, snapshotProgressListener, notificationService);
        this.connectorConfig = connectorConfig;
        this.jdbcConnection = jdbcConnectionFactory.mainConnection();
        this.jdbcConnectionFactory = jdbcConnectionFactory;
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
        Throwable exceptionWhileSnapshot = null;
        try {

            Set<Pattern> dataCollectionsToBeSnapshotted = getDataCollectionPattern(snapshottingTask.getDataCollections());

            Map<DataCollectionId, String> snapshotSelectOverridesByTable = snapshottingTask.getFilterQueries().entrySet().stream()
                    .collect(Collectors.toMap(e -> TableId.parse(e.getKey()), Map.Entry::getValue));

            preSnapshot();

            LOGGER.info("Snapshot step 1 - Preparing");

            if (previousOffset != null && previousOffset.isSnapshotRunning()) {
                LOGGER.info("Previous snapshot was cancelled before completion; a new snapshot will be taken.");
            }

            connection = createSnapshotConnection();
            connectionCreated(ctx);

            LOGGER.info("Snapshot step 2 - Determining captured tables");

            // Note that there's a minor race condition here: a new table matching the filters could be created between
            // this call and the determination of the initial snapshot position below; this seems acceptable, though
            determineCapturedTables(ctx, dataCollectionsToBeSnapshotted);
            snapshotProgressListener.monitoredDataCollectionsDetermined(snapshotContext.partition, ctx.capturedTables);
            // Init jdbc connection pool for reading table schema and data
            connectionPool = createConnectionPool(ctx);

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
                createDataEvents(context, ctx, connectionPool, snapshotSelectOverridesByTable);
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
        catch (final Exception | AssertionError e) {
            LOGGER.error("Error during snapshot", e);
            exceptionWhileSnapshot = e;
            throw e;
        }
        finally {
            try {
                if (connectionPool != null) {
                    for (JdbcConnection conn : connectionPool) {
                        if (!jdbcConnection.equals(conn)) {
                            conn.close();
                        }
                    }
                }
                rollbackTransaction(connection);
            }
            catch (final Exception e) {
                LOGGER.error("Error in finally block", e);
                if (exceptionWhileSnapshot != null) {
                    e.addSuppressed(exceptionWhileSnapshot);
                }
                throw e;
            }
        }
    }

    private Queue<JdbcConnection> createConnectionPool(final RelationalSnapshotContext<P, O> ctx) throws SQLException {
        Queue<JdbcConnection> connectionPool = new ConcurrentLinkedQueue<>();
        connectionPool.add(jdbcConnection);

        int snapshotMaxThreads = Math.max(1, Math.min(connectorConfig.getSnapshotMaxThreads(), ctx.capturedTables.size()));
        if (snapshotMaxThreads > 1) {
            Optional<String> firstQuery = getSnapshotConnectionFirstSelect(ctx, ctx.capturedTables.iterator().next());
            for (int i = 1; i < snapshotMaxThreads; i++) {
                JdbcConnection conn = jdbcConnectionFactory.newConnection().setAutoCommit(false);
                conn.connection().setTransactionIsolation(jdbcConnection.connection().getTransactionIsolation());
                connectionPoolConnectionCreated(ctx, conn);
                connectionPool.add(conn);
                if (firstQuery.isPresent()) {
                    conn.execute(firstQuery.get());
                }
            }
        }

        LOGGER.info("Created connection pool with {} threads", snapshotMaxThreads);
        return connectionPool;
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

    /**
     * Executes steps which have to be taken just after a connection pool connection is created.
     */
    protected void connectionPoolConnectionCreated(RelationalSnapshotContext<P, O> snapshotContext, JdbcConnection connection) throws SQLException {
    }

    protected List<Pattern> getSignalDataCollectionPattern(String signalingDataCollection) {
        return Strings.listOfRegex(signalingDataCollection, Pattern.CASE_INSENSITIVE);
    }

    private Stream<TableId> toTableIds(Set<TableId> tableIds, Pattern pattern) {
        return tableIds
                .stream()
                .filter(tid -> pattern.asMatchPredicate().test(connectorConfig.getTableIdMapper().toString(tid)))
                .sorted();
    }

    private Set<TableId> addSignalingCollectionAndSort(Set<TableId> capturedTables) {

        String tableIncludeList = connectorConfig.tableIncludeList();
        String signalingDataCollection = connectorConfig.getSignalingDataCollectionId();

        List<Pattern> captureTablePatterns = new ArrayList<>();
        if (!Strings.isNullOrBlank(tableIncludeList)) {
            captureTablePatterns.addAll(Strings.listOfRegex(tableIncludeList, Pattern.CASE_INSENSITIVE));
        }
        else {
            captureTablePatterns.add(MATCH_ALL_PATTERN);
        }

        if (!Strings.isNullOrBlank(signalingDataCollection)) {
            captureTablePatterns.addAll(getSignalDataCollectionPattern(signalingDataCollection));
        }

        return captureTablePatterns
                .stream()
                .flatMap(pattern -> toTableIds(capturedTables, pattern))
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    private void determineCapturedTables(RelationalSnapshotContext<P, O> ctx, Set<Pattern> dataCollectionsToBeSnapshotted) throws Exception {
        Set<TableId> allTableIds = getAllTableIds(ctx);
        Set<TableId> snapshottedTableIds = determineDataCollectionsToBeSnapshotted(allTableIds, dataCollectionsToBeSnapshotted).collect(Collectors.toSet());

        Set<TableId> capturedTables = new HashSet<>();
        Set<TableId> capturedSchemaTables = new HashSet<>();

        for (TableId tableId : allTableIds) {
            if (connectorConfig.getTableFilters().eligibleForSchemaDataCollectionFilter().isIncluded(tableId)) {
                LOGGER.info("Adding table {} to the list of capture schema tables", tableId);
                capturedSchemaTables.add(tableId);
            }
        }

        for (TableId tableId : snapshottedTableIds) {
            if (connectorConfig.getTableFilters().dataCollectionFilter().isIncluded(tableId)) {
                LOGGER.trace("Adding table {} to the list of captured tables for which the data will be snapshotted", tableId);
                capturedTables.add(tableId);
            }
            else {
                LOGGER.trace("Ignoring table {} for data snapshotting as it's not included in the filter configuration", tableId);
            }
        }

        ctx.capturedTables = addSignalingCollectionAndSort(capturedTables);
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
        if (!schema.isHistorized()) {
            return;
        }
        for (Iterator<TableId> iterator = getTablesForSchemaChange(snapshotContext).iterator(); iterator.hasNext();) {
            final TableId tableId = iterator.next();
            if (!sourceContext.isRunning()) {
                throw new InterruptedException("Interrupted while capturing schema of table " + tableId);
            }

            LOGGER.info("Capturing structure of table {}", tableId);

            snapshotContext.offset.event(tableId, getClock().currentTime());

            // If data are not snapshotted then the last schema change must set last snapshot flag
            if (!snapshottingTask.snapshotData() && !iterator.hasNext()) {
                lastSnapshotRecord(snapshotContext);
            }

            SchemaChangeEvent event = getCreateTableEvent(snapshotContext, snapshotContext.tables.forTable(tableId));
            if (HistorizedRelationalDatabaseSchema.class.isAssignableFrom(schema.getClass()) &&
                    ((HistorizedRelationalDatabaseSchema) schema).skipSchemaChangeEvent(event)) {
                continue;
            }

            dispatcher.dispatchSchemaChangeEvent(snapshotContext.partition, snapshotContext.offset, tableId, (receiver) -> {
                try {
                    receiver.schemaChangeEvent(event);
                }
                catch (Exception e) {
                    throw new DebeziumException(e);
                }
            });
        }
    }

    protected Collection<TableId> getTablesForSchemaChange(RelationalSnapshotContext<P, O> snapshotContext) {
        return snapshotContext.capturedTables;
    }

    /**
     * Creates a {@link SchemaChangeEvent} representing the creation of the given table.
     */
    protected abstract SchemaChangeEvent getCreateTableEvent(RelationalSnapshotContext<P, O> snapshotContext,
                                                             Table table)
            throws Exception;

    private void createDataEvents(ChangeEventSourceContext sourceContext,
                                  RelationalSnapshotContext<P, O> snapshotContext,
                                  Queue<JdbcConnection> connectionPool, Map<DataCollectionId, String> snapshotSelectOverridesByTable)
            throws Exception {
        tryStartingSnapshot(snapshotContext);

        SnapshotReceiver<P> snapshotReceiver = dispatcher.getSnapshotChangeEventReceiver();

        int snapshotMaxThreads = connectionPool.size();
        LOGGER.info("Creating snapshot worker pool with {} worker thread(s)", snapshotMaxThreads);
        ExecutorService executorService = Executors.newFixedThreadPool(snapshotMaxThreads);
        CompletionService<Void> completionService = new ExecutorCompletionService<>(executorService);

        Map<TableId, String> queryTables = new HashMap<>();
        Map<TableId, OptionalLong> rowCountTables = new LinkedHashMap<>();
        for (TableId tableId : snapshotContext.capturedTables) {
            final Optional<String> selectStatement = determineSnapshotSelect(snapshotContext, tableId, snapshotSelectOverridesByTable);
            if (selectStatement.isPresent()) {
                LOGGER.info("For table '{}' using select statement: '{}'", tableId, selectStatement.get());
                queryTables.put(tableId, selectStatement.get());

                final OptionalLong rowCount = rowCountForTable(tableId);
                rowCountTables.put(tableId, rowCount);
            }
            else {
                LOGGER.warn("For table '{}' the select statement was not provided, skipping table", tableId);
                snapshotProgressListener.dataCollectionSnapshotCompleted(snapshotContext.partition, tableId, 0);
            }
        }

        if (connectorConfig.snapshotOrderByRowCount() != SnapshotTablesRowCountOrder.DISABLED) {
            LOGGER.info("Sort tables by row count '{}'", connectorConfig.snapshotOrderByRowCount());
            final var orderFactor = (connectorConfig.snapshotOrderByRowCount() == SnapshotTablesRowCountOrder.ASCENDING) ? 1 : -1;
            rowCountTables = rowCountTables.entrySet().stream()
                    .sorted(Map.Entry.comparingByValue((a, b) -> orderFactor * Long.compare(a.orElse(0), b.orElse(0))))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
        }

        Queue<O> offsets = new ConcurrentLinkedQueue<>();
        offsets.add(snapshotContext.offset);
        for (int i = 1; i < snapshotMaxThreads; i++) {
            offsets.add(copyOffset(snapshotContext));
        }

        try {
            int tableCount = rowCountTables.size();
            int tableOrder = 1;
            for (TableId tableId : rowCountTables.keySet()) {
                boolean firstTable = tableOrder == 1 && snapshotMaxThreads == 1;
                boolean lastTable = tableOrder == tableCount && snapshotMaxThreads == 1;
                String selectStatement = queryTables.get(tableId);
                OptionalLong rowCount = rowCountTables.get(tableId);

                Callable<Void> callable = createDataEventsForTableCallable(sourceContext, snapshotContext, snapshotReceiver,
                        snapshotContext.tables.forTable(tableId), firstTable, lastTable, tableOrder++, tableCount, selectStatement, rowCount, connectionPool, offsets);
                completionService.submit(callable);
            }

            for (int i = 0; i < tableCount; i++) {
                completionService.take().get();
            }
        }
        finally {
            executorService.shutdownNow();
        }

        releaseDataSnapshotLocks(snapshotContext);
        for (O offset : offsets) {
            offset.preSnapshotCompletion();
        }
        snapshotReceiver.completeSnapshot();
        for (O offset : offsets) {
            offset.postSnapshotCompletion();
        }
    }

    protected abstract O copyOffset(RelationalSnapshotContext<P, O> snapshotContext);

    protected void tryStartingSnapshot(RelationalSnapshotContext<P, O> snapshotContext) {
        if (!snapshotContext.offset.isSnapshotRunning()) {
            snapshotContext.offset.preSnapshotStart();
        }
    }

    /**
     * For the given table gets source.ts_ms value from the database for snapshot data!
     * For Postgresql its globally static for all tables since postgresql snapshot process setting auto commit off.
     * For Mysql its static per table and might be ~second behind of the select statements start ts.
     */
    protected Instant getSnapshotSourceTimestamp(JdbcConnection jdbcConnection, O offset, TableId tableId) {
        try {
            Optional<Instant> snapshotTs = jdbcConnection.getCurrentTimestamp();
            if (snapshotTs.isEmpty()) {
                throw new ConnectException("Failed reading CURRENT_TIMESTAMP from source database");
            }

            return snapshotTs.get();
        }
        catch (SQLException e) {
            throw new ConnectException("Failed reading CURRENT_TIMESTAMP from source database", e);
        }
    }

    private Callable<Void> createDataEventsForTableCallable(ChangeEventSourceContext sourceContext, RelationalSnapshotContext<P, O> snapshotContext,
                                                            SnapshotReceiver<P> snapshotReceiver, Table table, boolean firstTable, boolean lastTable, int tableOrder,
                                                            int tableCount, String selectStatement, OptionalLong rowCount, Queue<JdbcConnection> connectionPool,
                                                            Queue<O> offsets) {
        return () -> {
            JdbcConnection connection = connectionPool.poll();
            O offset = offsets.poll();
            try {
                doCreateDataEventsForTable(sourceContext, snapshotContext.partition, offset, snapshotReceiver, table, firstTable, lastTable, tableOrder, tableCount,
                        selectStatement, rowCount, connection);
            }
            finally {
                offsets.add(offset);
                connectionPool.add(connection);
            }
            return null;
        };
    }

    private void doCreateDataEventsForTable(ChangeEventSourceContext sourceContext, P partition, O offset, SnapshotReceiver<P> snapshotReceiver, Table table,
                                            boolean firstTable, boolean lastTable, int tableOrder, int tableCount, String selectStatement, OptionalLong rowCount,
                                            JdbcConnection jdbcConnection)
            throws InterruptedException {

        if (!sourceContext.isRunning()) {
            throw new InterruptedException("Interrupted while snapshotting table " + table.id());
        }

        long exportStart = clock.currentTimeInMillis();
        LOGGER.info("Exporting data from table '{}' ({} of {} tables)", table.id(), tableOrder, tableCount);

        Instant sourceTableSnapshotTimestamp = getSnapshotSourceTimestamp(jdbcConnection, offset, table.id());

        try (Statement statement = readTableStatement(jdbcConnection, rowCount);
                ResultSet rs = CancellableResultSet.from(statement.executeQuery(selectStatement))) {

            ColumnUtils.ColumnArray columnArray = ColumnUtils.toArray(rs, table);
            long rows = 0;
            Timer logTimer = getTableScanLogTimer();
            boolean hasNext = rs.next();

            if (hasNext) {
                while (hasNext) {
                    if (!sourceContext.isRunning()) {
                        throw new InterruptedException("Interrupted while snapshotting table " + table.id());
                    }

                    rows++;
                    final Object[] row = jdbcConnection.rowToArray(table, rs, columnArray);

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
                        snapshotProgressListener.rowsScanned(partition, table.id(), rows);
                        logTimer = getTableScanLogTimer();
                    }

                    hasNext = rs.next();
                    setSnapshotMarker(offset, firstTable, lastTable, rows == 1, !hasNext);

                    dispatcher.dispatchSnapshotEvent(partition, table.id(),
                            getChangeRecordEmitter(partition, offset, table.id(), row, sourceTableSnapshotTimestamp), snapshotReceiver);
                }
            }
            else {
                setSnapshotMarker(offset, firstTable, lastTable, false, true);
            }

            LOGGER.info("\t Finished exporting {} records for table '{}' ({} of {} tables); total duration '{}'",
                    rows, table.id(), tableOrder, tableCount, Strings.duration(clock.currentTimeInMillis() - exportStart));
            snapshotProgressListener.dataCollectionSnapshotCompleted(partition, table.id(), rows);
        }
        catch (SQLException e) {
            throw new ConnectException("Snapshotting of table " + table.id() + " failed", e);
        }
    }

    private void setSnapshotMarker(OffsetContext offset, boolean firstTable, boolean lastTable, boolean firstRecordInTable,
                                   boolean lastRecordInTable) {
        if (lastRecordInTable && lastTable) {
            offset.markSnapshotRecord(SnapshotRecord.LAST);
        }
        else if (firstRecordInTable && firstTable) {
            offset.markSnapshotRecord(SnapshotRecord.FIRST);
        }
        else if (lastRecordInTable) {
            offset.markSnapshotRecord(SnapshotRecord.LAST_IN_DATA_COLLECTION);
        }
        else if (firstRecordInTable) {
            offset.markSnapshotRecord(SnapshotRecord.FIRST_IN_DATA_COLLECTION);
        }
        else {
            offset.markSnapshotRecord(SnapshotRecord.TRUE);
        }
    }

    protected void lastSnapshotRecord(RelationalSnapshotContext<P, O> snapshotContext) {
        snapshotContext.offset.markSnapshotRecord(SnapshotRecord.LAST);
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
    protected ChangeRecordEmitter<P> getChangeRecordEmitter(P partition, O offset, TableId tableId,
                                                            Object[] row, Instant timestamp) {
        offset.event(tableId, timestamp);
        return new SnapshotChangeRecordEmitter<>(partition, offset, row, getClock(), connectorConfig);
    }

    /**
     * Returns a valid query string for the specified table, either given by the user via snapshot select overrides or
     * defaulting to a statement provided by the DB-specific change event source.
     *
     * @param tableId the table to generate a query for
     * @param snapshotSelectOverridesByTable the select overrides by table
     * @return a valid query string or empty if table will not be snapshotted
     */
    private Optional<String> determineSnapshotSelect(RelationalSnapshotContext<P, O> snapshotContext, TableId tableId,
                                                     Map<DataCollectionId, String> snapshotSelectOverridesByTable) {
        String overriddenSelect = getSnapshotSelectOverridesByTable(tableId, snapshotSelectOverridesByTable);
        if (overriddenSelect != null) {
            return Optional.of(enhanceOverriddenSelect(snapshotContext, overriddenSelect, tableId));
        }

        List<String> columns = getPreparedColumnNames(snapshotContext.partition, schema.tableFor(tableId));

        return getSnapshotSelect(snapshotContext, tableId, columns);
    }

    protected String getSnapshotSelectOverridesByTable(TableId tableId, Map<DataCollectionId, String> snapshotSelectOverrides) {
        String overriddenSelect = snapshotSelectOverrides.get(tableId);

        // try without catalog id, as this might or might not be populated based on the given connector
        if (overriddenSelect == null) {
            overriddenSelect = snapshotSelectOverrides.get(new TableId(null, tableId.schema(), tableId.table()));
        }

        return overriddenSelect;
    }

    /**
     * Prepares a list of columns to be used in the snapshot select.
     * The selected columns are based on the column include/exclude filters and if all columns are excluded,
     * the list will contain all the primary key columns.
     *
     * @return list of snapshot select columns
     */
    protected List<String> getPreparedColumnNames(P partition, Table table) {
        List<String> columnNames = table.retrieveColumnNames()
                .stream()
                .filter(columnName -> additionalColumnFilter(partition, table.id(), columnName))
                .filter(columnName -> connectorConfig.getColumnFilter().matches(table.id().catalog(), table.id().schema(), table.id().table(), columnName))
                .map(jdbcConnection::quotedColumnIdString)
                .collect(Collectors.toList());

        if (columnNames.isEmpty()) {
            LOGGER.info("\t All columns in table {} were excluded due to include/exclude lists, defaulting to selecting all columns", table.id());

            columnNames = table.retrieveColumnNames()
                    .stream()
                    .map(jdbcConnection::quotedColumnIdString)
                    .collect(Collectors.toList());
        }

        return columnNames;
    }

    /**
     * Additional filter handling for preparing column names for snapshot select
     */
    protected boolean additionalColumnFilter(P partition, TableId tableId, String columnName) {
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

    protected Optional<String> getSnapshotConnectionFirstSelect(RelationalSnapshotContext<P, O> snapshotContext, TableId tableId) {
        return Optional.empty();
    }

    /**
     * Allow per-connector query creation to override for best database performance depending on the table size.
     */
    protected Statement readTableStatement(JdbcConnection jdbcConnection, OptionalLong tableSize) throws SQLException {
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

    protected void preSnapshot() throws InterruptedException {

    }
}
