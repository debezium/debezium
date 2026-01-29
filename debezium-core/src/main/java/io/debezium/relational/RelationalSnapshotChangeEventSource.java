/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
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
import io.debezium.pipeline.signal.actions.snapshotting.AdditionalCondition;
import io.debezium.pipeline.signal.actions.snapshotting.SnapshotConfiguration;
import io.debezium.pipeline.source.AbstractSnapshotChangeEventSource;
import io.debezium.pipeline.source.SnapshottingTask;
import io.debezium.pipeline.source.snapshot.chunked.ChunkBoundaryCalculator;
import io.debezium.pipeline.source.snapshot.chunked.SnapshotChunk;
import io.debezium.pipeline.source.snapshot.chunked.SnapshotChunkQueryBuilder;
import io.debezium.pipeline.source.snapshot.chunked.SnapshotProgress;
import io.debezium.pipeline.source.snapshot.chunked.TableChunkProgress;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.pipeline.spi.ChangeRecordEmitter;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;
import io.debezium.pipeline.spi.SnapshotResult;
import io.debezium.relational.RelationalDatabaseConnectorConfig.SnapshotTablesRowCountOrder;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.spi.snapshot.Snapshotter;
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
    protected final SnapshotterService snapshotterService;
    protected Queue<JdbcConnection> connectionPool;
    private final TableId signalDataCollectionTableId;

    public RelationalSnapshotChangeEventSource(RelationalDatabaseConnectorConfig connectorConfig,
                                               MainConnectionProvidingConnectionFactory<? extends JdbcConnection> jdbcConnectionFactory,
                                               RelationalDatabaseSchema schema, EventDispatcher<P, TableId> dispatcher, Clock clock,
                                               SnapshotProgressListener<P> snapshotProgressListener, NotificationService<P, O> notificationService,
                                               SnapshotterService snapshotterService) {
        super(connectorConfig, snapshotProgressListener, notificationService);
        this.connectorConfig = connectorConfig;
        this.jdbcConnection = jdbcConnectionFactory.mainConnection();
        this.jdbcConnectionFactory = jdbcConnectionFactory;
        this.schema = schema;
        this.dispatcher = dispatcher;
        this.clock = clock;
        this.snapshotProgressListener = snapshotProgressListener;
        this.snapshotterService = snapshotterService;

        if (!connectorConfig.getSignalingDataCollectionIds().isEmpty()) {
            this.signalDataCollectionTableId = TableId.parse(connectorConfig.getSignalingDataCollectionIds().get(0));
        }
        else {
            this.signalDataCollectionTableId = null;
        }
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

            Map<DataCollectionId, String> snapshotSelectOverridesByTable = snapshottingTask.getFilterQueries();

            preSnapshot();

            LOGGER.info("Snapshot step 1 - Preparing");

            if (previousOffset != null && previousOffset.isInitialSnapshotRunning()) {
                LOGGER.info("Previous snapshot was cancelled before completion; a new snapshot will be taken.");
            }

            connection = createSnapshotConnection();
            connectionCreated(ctx);

            LOGGER.info("Snapshot step 2 - Determining captured tables");

            // Note that there's a minor race condition here: a new table matching the filters could be created between
            // this call and the determination of the initial snapshot position below; this seems acceptable, though
            determineCapturedTables(ctx, dataCollectionsToBeSnapshotted, snapshottingTask);
            snapshotProgressListener.monitoredDataCollectionsDetermined(snapshotContext.partition, ctx.capturedTables);
            // Init jdbc connection pool for reading table schema and data
            connectionPool = createConnectionPool(ctx);

            LOGGER.info("Snapshot step 3 - Locking captured tables {}", ctx.capturedTables);

            if (snapshottingTask.snapshotSchema()) {
                lockTablesForSchemaSnapshot(context, ctx);
            }

            // In case of a bocking snapshot the offsets of snapshot context must be the set to avoid reinitialization
            // to an empty one during the determineSnapshotOffset function
            if (!snapshottingTask.isOnDemand()) {
                LOGGER.info("Snapshot step 4 - Determining snapshot offset");
                determineSnapshotOffset(ctx, previousOffset);
            }
            else {
                LOGGER.info("Snapshot step 4 - Determining snapshot offset (SKIPPED)");
            }

            LOGGER.info("Snapshot step 5 - Reading structure of captured tables");
            readTableStructure(context, ctx, previousOffset, snapshottingTask);

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

        int snapshotMaxThreads = connectorConfig.getSnapshotMaxThreads();
        if (connectorConfig.isLegacySnapshotMaxThreads()) {
            // Legacy snapshot max thread logic would only use a connection pool of N threads, where N was the minimum
            // between the number of tables and snapshot max threads. The new parallel behavior is designed to create
            // the full pool regardless of tables, as tables are snapshotted in chunks, utilizing the full pool.
            snapshotMaxThreads = Math.max(1, Math.min(connectorConfig.getSnapshotMaxThreads(), ctx.capturedTables.size()));
        }

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

        if (!jdbcConnection.isValid()) {
            jdbcConnection.reconnect();
        }

        Connection connection = jdbcConnection.connection();
        connection.setAutoCommit(false);
        return connection;
    }

    @Override
    public SnapshottingTask getBlockingSnapshottingTask(P partition, O previousOffset, SnapshotConfiguration snapshotConfiguration) {

        Map<DataCollectionId, String> filtersByTable = snapshotConfiguration.getAdditionalConditions().stream()
                .collect(Collectors.toMap(k -> TableId.parse(k.getDataCollection().toString()), AdditionalCondition::getFilter));

        return new SnapshottingTask(true, true, snapshotConfiguration.getDataCollections(), filtersByTable, true);
    }

    public SnapshottingTask getSnapshottingTask(P partition, O previousOffset) {

        final Snapshotter snapshotter = snapshotterService.getSnapshotter();

        List<String> dataCollectionsToBeSnapshotted = connectorConfig.getDataCollectionsToBeSnapshotted();
        Map<DataCollectionId, String> snapshotSelectOverridesByTable = connectorConfig.getSnapshotSelectOverridesByTable();

        boolean offsetExists = previousOffset != null;
        boolean snapshotInProgress = false;

        if (offsetExists) {
            snapshotInProgress = previousOffset.isInitialSnapshotRunning();
        }

        if (offsetExists && !previousOffset.isInitialSnapshotRunning()) {
            LOGGER.info("A previous offset indicating a completed snapshot has been found.");
        }

        boolean shouldSnapshotSchema = snapshotter.shouldSnapshotSchema(offsetExists, snapshotInProgress);
        boolean shouldSnapshotData = snapshotter.shouldSnapshotData(offsetExists, snapshotInProgress);

        if (shouldSnapshotData && shouldSnapshotSchema) {
            LOGGER.info("According to the connector configuration both schema and data will be snapshot.");
        }
        else if (shouldSnapshotSchema) {
            LOGGER.info("According to the connector configuration only schema will be snapshot.");
        }

        return new SnapshottingTask(shouldSnapshotSchema, shouldSnapshotData,
                dataCollectionsToBeSnapshotted, snapshotSelectOverridesByTable,
                false);
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
        List<String> signalingDataCollections = connectorConfig.getSignalingDataCollectionIds();

        List<Pattern> captureTablePatterns = new ArrayList<>();
        if (!Strings.isNullOrBlank(tableIncludeList)) {
            captureTablePatterns.addAll(Strings.listOfRegex(tableIncludeList, Pattern.CASE_INSENSITIVE));
        }
        else {
            captureTablePatterns.add(MATCH_ALL_PATTERN);
        }

        for (String signalingDataCollection : signalingDataCollections) {
            captureTablePatterns.addAll(getSignalDataCollectionPattern(signalingDataCollection));
        }

        return captureTablePatterns
                .stream()
                .flatMap(pattern -> toTableIds(capturedTables, pattern))
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    private void determineCapturedTables(RelationalSnapshotContext<P, O> ctx, Set<Pattern> dataCollectionsToBeSnapshotted, SnapshottingTask snapshottingTask)
            throws Exception {

        Set<TableId> allTableIds = getAllTableIds(ctx);
        Set<TableId> snapshottedTableIds = determineDataCollectionsToBeSnapshotted(allTableIds, dataCollectionsToBeSnapshotted).collect(Collectors.toSet());

        Set<TableId> capturedTables = new HashSet<>();
        Set<TableId> capturedSchemaTables = new HashSet<>();

        for (TableId tableId : allTableIds) {
            if (connectorConfig.getTableFilters().eligibleForSchemaDataCollectionFilter().isIncluded(tableId) && !snapshottingTask.isOnDemand()) {
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
        ctx.capturedSchemaTables = snapshottingTask.isOnDemand() ? ctx.capturedTables
                : capturedSchemaTables
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
                                               RelationalSnapshotContext<P, O> snapshotContext, O offsetContext, SnapshottingTask snapshottingTask)
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

            final Table table = snapshotContext.tables.forTable(tableId);
            if (table == null) {
                throw new DebeziumException("Unable to find relational table model for '" + tableId +
                        "', there may be an issue with your include/exclude list configuration.");
            }

            SchemaChangeEvent event = getCreateTableEvent(snapshotContext, table);
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

    private void createDataEvents(ChangeEventSourceContext sourceContext, RelationalSnapshotContext<P, O> snapshotContext,
                                  Queue<JdbcConnection> connectionPool, Map<DataCollectionId, String> snapshotSelectOverridesByTable)
            throws Exception {
        tryStartingSnapshot(snapshotContext);

        try {
            final SnapshotReceiver<P> snapshotReceiver = dispatcher.getSnapshotChangeEventReceiver();
            final int snapshotMaxThreads = connectionPool.size();
            final Queue<O> offsets = createOffsetPool(snapshotContext, snapshotMaxThreads);

            // When legacy snapshot max threads is enabled, we fall back to the table per thread behavior. This provides
            // a reasonable fallback for parallelism while the new chunked solution matures.
            if (connectorConfig.isLegacySnapshotMaxThreads()) {
                createLegacyDataEvents(sourceContext, snapshotContext, connectionPool, snapshotSelectOverridesByTable, offsets, snapshotReceiver);
            }
            else {
                createChunkedDataEvents(sourceContext, snapshotContext, connectionPool, snapshotSelectOverridesByTable, offsets, snapshotReceiver);
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
        finally {
            releaseDataSnapshotLocks(snapshotContext);
        }
    }

    private void createLegacyDataEvents(ChangeEventSourceContext sourceContext, RelationalSnapshotContext<P, O> snapshotContext,
                                        Queue<JdbcConnection> connectionPool, Map<DataCollectionId, String> snapshotSelectOverridesByTable,
                                        Queue<O> offsets, SnapshotReceiver<P> snapshotReceiver)
            throws Exception {

        final int snapshotMaxThreads = connectionPool.size();

        final PreparedTables prepared = prepareTables(snapshotContext,
                tableId -> determineSnapshotSelect(snapshotContext, tableId, snapshotSelectOverridesByTable),
                this::rowCountForTable);

        try (ThreadedSnapshotExecutor executor = new ThreadedSnapshotExecutor(snapshotMaxThreads, "snapshot")) {
            final int tableCount = prepared.rowCountTables.size();
            int tableOrder = 1;
            final Set<TableId> rowCountTablesKeySet = Collections.unmodifiableSet(new HashSet<>(prepared.rowCountTables.keySet()));
            for (TableId tableId : prepared.rowCountTables.keySet()) {
                boolean firstTable = tableOrder == 1 && snapshotMaxThreads == 1;
                boolean lastTable = tableOrder == tableCount && snapshotMaxThreads == 1;
                String selectStatement = prepared.queryTables.get(tableId);
                OptionalLong rowCount = prepared.rowCountTables.get(tableId);
                Callable<Void> callable = createDataEventsForTableCallable(sourceContext, snapshotContext, snapshotReceiver,
                        snapshotContext.tables.forTable(tableId), firstTable, lastTable, tableOrder++, tableCount, selectStatement, rowCount, rowCountTablesKeySet,
                        connectionPool, offsets);
                executor.submit(callable);
            }
            executor.awaitCompletion();
        }
    }

    private void createChunkedDataEvents(ChangeEventSourceContext sourceContext, RelationalSnapshotContext<P, O> snapshotContext,
                                         Queue<JdbcConnection> connectionPool, Map<DataCollectionId, String> snapshotSelectOverridesByTable,
                                         Queue<O> offsets, SnapshotReceiver<P> snapshotReceiver)
            throws Exception {

        final int snapshotMaxThreads = connectionPool.size();

        // todo: support snapshot select overrides
        final PreparedTables prepared = prepareTables(snapshotContext,
                tableId -> {
                    final List<String> columns = getPreparedColumnNames(snapshotContext.partition, schema.tableFor(tableId));
                    return getSnapshotSelect(snapshotContext, tableId, columns);
                },
                tableId -> OptionalLong.of(rowCountForTableChunked(tableId)));

        // Create progress tracking and chunks
        final Map<TableId, TableChunkProgress> progressMap = new ConcurrentHashMap<>();
        final List<SnapshotChunk> allChunks = new ArrayList<>();

        final ChunkBoundaryCalculator boundaryCalculator = new ChunkBoundaryCalculator(jdbcConnection);

        int tableOrder = 1;
        final int tableCount = prepared.rowCountTables.size();

        // Create global snapshot progress for coordination
        final SnapshotProgress snapshotProgress = new SnapshotProgress(tableCount);

        // Each snapshotted table, generate chunk details
        for (TableId tableId : prepared.rowCountTables.keySet()) {
            final Table table = snapshotContext.tables.forTable(tableId);
            final String selectStatement = prepared.queryTables.get(tableId);
            final OptionalLong rowCount = prepared.rowCountTables.get(tableId);

            final List<SnapshotChunk> tableChunks;
            final List<Column> keyColumns = getKeyColumnsForChunking(table);
            if (keyColumns.isEmpty()) {
                // Keyless table - single chunk
                LOGGER.info("Table '{}' has no key columns, using single chunk.", tableId);
                tableChunks = List.of(new SnapshotChunk(tableId, table, null, null, 0, 1, tableOrder, tableCount, selectStatement, rowCount));
            }
            else {
                // Calculate chunk count and boundaries
                final int multiplier = connectorConfig.getSnapshotMaxThreadsMultiplierForTable(tableId);
                final int numChunks = calculateChunkCount(rowCount, snapshotMaxThreads, multiplier);
                LOGGER.info("Table '{}' calculating chunk boundaries using multiplier {} with {} chunks.", tableId, multiplier, numChunks);
                final List<Object[]> boundaries = boundaryCalculator.calculateBoundaries(table, keyColumns, rowCount, numChunks);

                tableChunks = boundaryCalculator.createChunks(table, boundaries, tableOrder, tableCount, selectStatement, rowCount);
                LOGGER.info("Table '{}' will be processed in {} chunks.", tableId, tableChunks.size());
            }

            progressMap.put(tableId, new TableChunkProgress(tableId, tableChunks.size()));
            allChunks.addAll(tableChunks);
            tableOrder++;
        }

        final long exportStart = clock.currentTimeInMillis();
        try (ThreadedSnapshotExecutor executor = new ThreadedSnapshotExecutor(snapshotMaxThreads, "chunked snapshot")) {
            for (SnapshotChunk chunk : allChunks) {
                final Callable<Void> callable = createDataEventsForChunkedTableCallable(sourceContext, snapshotContext, snapshotReceiver,
                        chunk, progressMap, snapshotProgress, connectionPool, offsets);
                executor.submit(callable);
            }
            executor.awaitCompletion();
        }

        LOGGER.info("Finished chunk snapshot of {} tables ({} chunks); duration '{}'",
                tableCount, allChunks.size(), Strings.duration(clock.currentTimeInMillis() - exportStart));
    }

    /**
     * Emits a record with proper coordination to ensure correct snapshot marker ordering.
     */
    private void emitRecordWithCoordination(RelationalSnapshotContext<P, O> snapshotContext, O offset,
                                            SnapshotReceiver<P> snapshotReceiver, SnapshotChunk chunk,
                                            TableChunkProgress progress, SnapshotProgress snapshotProgress,
                                            TableId tableId, Object[] row, Instant sourceTableSnapshotTimestamp,
                                            boolean isFirstRecord, boolean isLastRecord)
            throws InterruptedException {

        final boolean isFirstChunk = chunk.isFirstChunk();
        final boolean isLastChunk = chunk.isLastChunk();
        final boolean isFirstChunkOfSnapshot = chunk.isFirstChunkOfSnapshot();
        final boolean isLastChunkOfSnapshot = chunk.isLastChunkOfSnapshot();

        // Handle first record of first chunk - signals that others can proceed
        if (isFirstRecord && isFirstChunk) {
            if (isFirstChunkOfSnapshot) {
                // FIRST record of entire snapshot - emit immediately, then signal
                setChunkSnapshotMarker(offset, chunk, true, isLastRecord && isLastChunkOfSnapshot);
                dispatcher.dispatchSnapshotEvent(snapshotContext.partition, tableId,
                        getChangeRecordEmitter(snapshotContext.partition, offset, tableId, row, sourceTableSnapshotTimestamp),
                        snapshotReceiver);
                snapshotProgress.signalFirstRecordEmitted();
                progress.signalFirstRecordEmitted();
            }
            else {
                // FIRST_IN_DATA_COLLECTION - wait for global first, then emit, then signal table first
                snapshotProgress.waitForFirstRecord();
                setChunkSnapshotMarker(offset, chunk, true, isLastRecord && isLastChunk);
                dispatcher.dispatchSnapshotEvent(snapshotContext.partition, tableId,
                        getChangeRecordEmitter(snapshotContext.partition, offset, tableId, row, sourceTableSnapshotTimestamp),
                        snapshotReceiver);
                progress.signalFirstRecordEmitted();
            }

            // If this is also the last record of the last chunk, handle table completion
            if (isLastRecord && isLastChunk && !isLastChunkOfSnapshot) {
                // Single-record table that's not the last table - signal table complete
                // No need to wait for other chunks since this is the only chunk
                snapshotProgress.signalTableComplete();
            }
        }
        else if (isLastRecord && isLastChunkOfSnapshot) {
            // LAST record of entire snapshot - wait for all other chunks and tables, then emit
            progress.waitForFirstRecord();
            progress.waitForOtherChunks();
            snapshotProgress.waitForOtherTables();
            setChunkSnapshotMarker(offset, chunk, false, true);
            dispatcher.dispatchSnapshotEvent(snapshotContext.partition, tableId,
                    getChangeRecordEmitter(snapshotContext.partition, offset, tableId, row, sourceTableSnapshotTimestamp),
                    snapshotReceiver);
        }
        else if (isLastRecord && isLastChunk) {
            // LAST_IN_DATA_COLLECTION - wait for other chunks of this table, then emit, then signal table complete
            progress.waitForFirstRecord();
            progress.waitForOtherChunks();
            setChunkSnapshotMarker(offset, chunk, false, true);
            dispatcher.dispatchSnapshotEvent(snapshotContext.partition, tableId,
                    getChangeRecordEmitter(snapshotContext.partition, offset, tableId, row, sourceTableSnapshotTimestamp),
                    snapshotReceiver);
            snapshotProgress.signalTableComplete();
        }
        else {
            // Regular record - wait for table's first record, then emit
            progress.waitForFirstRecord();
            setChunkSnapshotMarker(offset, chunk, isFirstRecord, isLastRecord);
            dispatcher.dispatchSnapshotEvent(snapshotContext.partition, tableId,
                    getChangeRecordEmitter(snapshotContext.partition, offset, tableId, row, sourceTableSnapshotTimestamp),
                    snapshotReceiver);
        }
    }

    /**
     * Handles coordination for empty chunks to ensure latches are properly signaled.
     */
    private void handleEmptyChunkCoordination(SnapshotChunk chunk, TableChunkProgress progress, SnapshotProgress snapshotProgress)
            throws InterruptedException {
        // For empty first chunk, signal that first record has been "emitted" (skipped)
        if (chunk.isFirstChunk()) {
            if (chunk.isFirstChunkOfSnapshot()) {
                snapshotProgress.signalFirstRecordEmitted();
            }

            progress.signalFirstRecordEmitted();
        }

        // For empty last chunk, wait for others and signal completion
        if (chunk.isLastChunk()) {
            if (!chunk.isFirstChunk()) {
                // Not also the first chunk, so we need to wait for first record signal
                progress.waitForFirstRecord();
            }

            progress.waitForOtherChunks();

            if (chunk.isLastChunkOfSnapshot()) {
                snapshotProgress.waitForOtherTables();
            }
            else {
                snapshotProgress.signalTableComplete();
            }
        }
    }

    protected List<Column> getKeyColumnsForChunking(Table table) {
        final Key.KeyMapper keyMapper = connectorConfig.getKeyMapper();
        if (keyMapper != null) {
            final List<Column> customKeys = keyMapper.getKeyKolumns(table);
            if (!customKeys.isEmpty()) {
                return customKeys;
            }
        }
        return table.primaryKeyColumns();
    }

    protected int calculateChunkCount(OptionalLong rowCount, int maxThreads, int multiplier) {
        if (rowCount.isEmpty() || rowCount.getAsLong() == 0) {
            return 1;
        }

        final int desiredChunks = maxThreads * multiplier;
        final long rowsPerChunk = Math.max(1, rowCount.getAsLong() / desiredChunks);
        return (int) Math.min(desiredChunks, Math.max(1, rowCount.getAsLong() / rowsPerChunk));
    }

    private Queue<O> createOffsetPool(RelationalSnapshotContext<P, O> snapshotContext, int poolSize) {
        final Queue<O> offsets = new ConcurrentLinkedQueue<>();
        offsets.add(snapshotContext.offset);

        for (int i = 1; i < poolSize; i++) {
            offsets.add(copyOffset(snapshotContext));
        }

        return offsets;
    }

    private Map<TableId, OptionalLong> sortByRowCount(Map<TableId, OptionalLong> rowCountTables, SnapshotTablesRowCountOrder order) {
        if (order == SnapshotTablesRowCountOrder.DISABLED) {
            return rowCountTables;
        }

        LOGGER.info("Sort tables by row count '{}'", order);
        final int orderFactor = (order == SnapshotTablesRowCountOrder.ASCENDING) ? 1 : -1;
        return rowCountTables.entrySet().stream()
                .sorted(Map.Entry.comparingByValue((a, b) -> orderFactor * Long.compare(a.orElse(0), b.orElse(0))))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
    }

    protected abstract O copyOffset(RelationalSnapshotContext<P, O> snapshotContext);

    protected void tryStartingSnapshot(RelationalSnapshotContext<P, O> snapshotContext) {
        if (!snapshotContext.offset.isInitialSnapshotRunning()) {
            snapshotContext.offset.preSnapshotStart(snapshotContext.onDemand);
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

    protected Callable<Void> createDataEventsForTableCallable(ChangeEventSourceContext sourceContext, RelationalSnapshotContext<P, O> snapshotContext,
                                                              SnapshotReceiver<P> snapshotReceiver, Table table, boolean firstTable, boolean lastTable, int tableOrder,
                                                              int tableCount, String selectStatement, OptionalLong rowCount, Set<TableId> rowCountTablesKeySet,
                                                              Queue<JdbcConnection> connectionPool, Queue<O> offsets) {
        return createPooledResourceCallable(connectionPool, offsets,
                (connection, offset) -> {
                    try {
                        doCreateDataEventsForTable(sourceContext, snapshotContext, offset, snapshotReceiver, table, firstTable, lastTable, tableOrder, tableCount,
                                selectStatement, rowCount, rowCountTablesKeySet, connection);
                    }
                    catch (SQLException e) {
                        notificationService.initialSnapshotNotificationService().notifyCompletedTableWithError(snapshotContext.partition,
                                snapshotContext.offset,
                                table.id().identifier());
                        throw new ConnectException("Snapshotting of table " + table.id() + " failed", e);
                    }
                },
                null);
    }

    protected Callable<Void> createDataEventsForChunkedTableCallable(ChangeEventSourceContext sourceContext, RelationalSnapshotContext<P, O> snapshotContext,
                                                                     SnapshotReceiver<P> snapshotReceiver, SnapshotChunk chunk,
                                                                     Map<TableId, TableChunkProgress> progressMap, SnapshotProgress snapshotProgress,
                                                                     Queue<JdbcConnection> connectionPool, Queue<O> offsets) {
        return createPooledResourceCallable(connectionPool, offsets,
                (connection, offset) -> {
                    try {
                        doCreateDataEventsForChunk(sourceContext, snapshotContext, offset, snapshotReceiver, chunk, progressMap, snapshotProgress, connection);
                    }
                    catch (SQLException e) {
                        notificationService.initialSnapshotNotificationService().notifyCompletedTableWithError(
                                snapshotContext.partition, snapshotContext.offset, chunk.getTableId().identifier());
                        throw new ConnectException("Snapshotting of table " + chunk.getTableId() + " chunk " + chunk.getChunkId() + " failed", e);
                    }
                },
                null);
    }

    protected void doCreateDataEventsForTable(ChangeEventSourceContext sourceContext, RelationalSnapshotContext<P, O> snapshotContext, O offset,
                                              SnapshotReceiver<P> snapshotReceiver, Table table,
                                              boolean firstTable, boolean lastTable, int tableOrder, int tableCount, String selectStatement, OptionalLong rowCount,
                                              Set<TableId> rowCountTablesKeySet, JdbcConnection jdbcConnection)
            throws InterruptedException, SQLException {

        if (!sourceContext.isRunning()) {
            throw new InterruptedException("Interrupted while snapshotting table " + table.id());
        }

        long exportStart = clock.currentTimeInMillis();
        LOGGER.info("Exporting data from table '{}' ({} of {} tables)", table.id(), tableOrder, tableCount);

        notificationService.initialSnapshotNotificationService().notifyTableInProgress(
                snapshotContext.partition,
                snapshotContext.offset,
                table.id().identifier(),
                rowCountTablesKeySet);

        Instant sourceTableSnapshotTimestamp = getSnapshotSourceTimestamp(jdbcConnection, offset, table.id());

        try (Statement statement = readTableStatement(jdbcConnection, rowCount);
                ResultSet rs = resultSetForDataEvents(selectStatement, statement)) {

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
                        snapshotProgressListener.rowsScanned(snapshotContext.partition, table.id(), rows);
                        logTimer = getTableScanLogTimer();
                    }

                    hasNext = rs.next();
                    setSnapshotMarker(offset, firstTable, lastTable, rows == 1, !hasNext);

                    dispatcher.dispatchSnapshotEvent(snapshotContext.partition, table.id(),
                            getChangeRecordEmitter(snapshotContext.partition, offset, table.id(), row, sourceTableSnapshotTimestamp), snapshotReceiver);
                }
            }
            else {
                setSnapshotMarker(offset, firstTable, lastTable, false, true);
            }

            LOGGER.info("\t Finished exporting {} records for table '{}' ({} of {} tables); total duration '{}'",
                    rows, table.id(), tableOrder, tableCount, Strings.duration(clock.currentTimeInMillis() - exportStart));
            snapshotProgressListener.dataCollectionSnapshotCompleted(snapshotContext.partition, table.id(), rows);
            notificationService.initialSnapshotNotificationService().notifyCompletedTableSuccessfully(snapshotContext.partition,
                    snapshotContext.offset, table.id().identifier(), rows, snapshotContext.capturedTables);
        }
    }

    protected void doCreateDataEventsForChunk(ChangeEventSourceContext sourceContext, RelationalSnapshotContext<P, O> snapshotContext,
                                              O offset, SnapshotReceiver<P> snapshotReceiver, SnapshotChunk chunk,
                                              Map<TableId, TableChunkProgress> progressMap, SnapshotProgress snapshotProgress,
                                              JdbcConnection jdbcConnection)
            throws InterruptedException, SQLException {

        if (!sourceContext.isRunning()) {
            throw new InterruptedException("Interrupted while snapshotting chunk " + chunk.getChunkId());
        }

        final TableId tableId = chunk.getTableId();
        final Table table = chunk.getTable();
        final TableChunkProgress progress = progressMap.get(tableId);

        final long exportStart = clock.currentTimeInMillis();
        LOGGER.info("Exporting chunk {}/{} from table '{}' ({}/{} tables)",
                chunk.getChunkIndex() + 1, chunk.getTotalChunks(),
                tableId, chunk.getTableOrder(), chunk.getTableCount());

        // Get key columns for query building
        final List<Column> keyColumns = getKeyColumnsForChunking(table);

        // Build chunk query using standalone SnapshotChunkQueryBuilder
        final SnapshotChunkQueryBuilder queryBuilder = new SnapshotChunkQueryBuilder(jdbcConnection);
        final String chunkQuery = queryBuilder.buildChunkQuery(chunk, keyColumns, chunk.getBaseSelectStatement());
        final Instant sourceTableSnapshotTimestamp = getSnapshotSourceTimestamp(jdbcConnection, offset, tableId);

        try (PreparedStatement statement = queryBuilder.prepareChunkStatement(chunk, keyColumns, chunkQuery);
                ResultSet rs = statement.executeQuery()) {

            final ColumnUtils.ColumnArray columnArray = ColumnUtils.toArray(rs, table);
            long rows = 0;
            Timer logTimer = getTableScanLogTimer();
            boolean hasNext = rs.next();

            if (hasNext) {
                while (hasNext) {
                    if (!sourceContext.isRunning()) {
                        throw new InterruptedException("Interrupted while snapshotting chunk " + chunk.getChunkId());
                    }

                    rows++;
                    final Object[] row = jdbcConnection.rowToArray(table, rs, columnArray);

                    if (logTimer.expired()) {
                        long stop = clock.currentTimeInMillis();
                        LOGGER.info("\t Chunk {}: Exported {} records for table '{}' after {}",
                                chunk.getChunkIndex() + 1, rows, tableId,
                                Strings.duration(stop - exportStart));
                        logTimer = getTableScanLogTimer();
                    }

                    hasNext = rs.next();

                    final boolean isFirstRecord = (rows == 1);
                    final boolean isLastRecord = !hasNext;

                    // Coordinate emission based on marker type
                    emitRecordWithCoordination(snapshotContext, offset, snapshotReceiver, chunk, progress,
                            snapshotProgress, tableId, row, sourceTableSnapshotTimestamp, isFirstRecord, isLastRecord);
                }
            }
            else {
                // Empty chunk - handle coordination for empty first/last chunks
                handleEmptyChunkCoordination(chunk, progress, snapshotProgress);
            }

            // Update progress
            progress.markChunkComplete(rows);

            // Signal chunk completion for non-last chunks
            if (!chunk.isLastChunk()) {
                progress.signalChunkComplete();
            }

            LOGGER.info("\t Finished chunk {}/{} ({} records) for table '{}'; duration '{}'",
                    chunk.getChunkIndex() + 1, chunk.getTotalChunks(), rows, tableId,
                    Strings.duration(clock.currentTimeInMillis() - exportStart));

            // Report table completion when all chunks done
            if (progress.isTableComplete()) {
                snapshotProgressListener.dataCollectionSnapshotCompleted(
                        snapshotContext.partition, tableId, progress.getTotalRowsScanned());
            }

            snapshotProgressListener.rowsScanned(snapshotContext.partition, tableId,
                    progress.getTotalRowsScanned());
        }
    }

    protected ResultSet resultSetForDataEvents(String selectStatement, Statement statement)
            throws SQLException {
        return CancellableResultSet.from(statement.executeQuery(selectStatement));
    }

    private void setSnapshotMarker(OffsetContext offset, boolean firstTable, boolean lastTable, boolean firstRecordInTable,
                                   boolean lastRecordInTable) {
        final SnapshotRecord marker = SnapshotMarkerResolver.resolve(
                firstRecordInTable && firstTable,
                lastRecordInTable && lastTable,
                firstRecordInTable,
                lastRecordInTable);
        offset.markSnapshotRecord(marker);
    }

    private void setChunkSnapshotMarker(OffsetContext offset, SnapshotChunk chunk, boolean firstRecordInChunk, boolean lastRecordInChunk) {
        final SnapshotRecord marker = SnapshotMarkerResolver.resolve(
                firstRecordInChunk && chunk.isFirstChunkOfSnapshot(),
                lastRecordInChunk && chunk.isLastChunkOfSnapshot(),
                firstRecordInChunk && chunk.isFirstChunk(),
                lastRecordInChunk && chunk.isLastChunk());
        offset.markSnapshotRecord(marker);
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

    protected Long rowCountForTableChunked(TableId tableId) throws SQLException {
        // todo: snapshot select overrides?
        return jdbcConnection.queryAndMap(
                "SELECT COUNT(1) FROM %s".formatted(jdbcConnection.getQualifiedTableName(tableId)),
                rs -> rs.next() ? rs.getLong(1) : 0L);
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
        if (tableId.equals(signalDataCollectionTableId)) {
            // Skip the signal data collection as data shouldn't be captured
            return Optional.empty();
        }

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
                .map(jdbcConnection::quoteIdentifier)
                .collect(Collectors.toList());

        if (columnNames.isEmpty()) {
            LOGGER.info("\t All columns in table {} were excluded due to include/exclude lists, defaulting to selecting all columns", table.id());

            columnNames = table.retrieveColumnNames()
                    .stream()
                    .map(jdbcConnection::quoteIdentifier)
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
     * Determines the appropriate SnapshotRecord marker based on record position.
     */
    private static class SnapshotMarkerResolver {
        /**
         * Resolves the snapshot marker for a record based on its position within the snapshot.
         *
         * @param isFirstInSnapshot true if this is the first record of the entire snapshot
         * @param isLastInSnapshot true if this is the last record of the entire snapshot
         * @param isFirstInDataCollection true if this is the first record of the current table/data collection
         * @param isLastInDataCollection true if this is the last record of the current table/data collection
         * @return the appropriate SnapshotRecord marker
         */
        static SnapshotRecord resolve(boolean isFirstInSnapshot, boolean isLastInSnapshot,
                                      boolean isFirstInDataCollection, boolean isLastInDataCollection) {
            if (isLastInSnapshot) {
                return SnapshotRecord.LAST;
            }
            else if (isFirstInSnapshot) {
                return SnapshotRecord.FIRST;
            }
            else if (isLastInDataCollection) {
                return SnapshotRecord.LAST_IN_DATA_COLLECTION;
            }
            else if (isFirstInDataCollection) {
                return SnapshotRecord.FIRST_IN_DATA_COLLECTION;
            }
            else {
                return SnapshotRecord.TRUE;
            }
        }
    }

    /**
     * Manages threaded execution of snapshot work using a thread pool.
     */
    private static class ThreadedSnapshotExecutor implements AutoCloseable {

        private final ExecutorService executorService;
        private final CompletionService<Void> completionService;
        private int submittedTasks = 0;

        ThreadedSnapshotExecutor(int threadCount, String description) {
            LOGGER.info("Creating {} worker pool with {} worker thread(s)", description, threadCount);
            this.executorService = Executors.newFixedThreadPool(threadCount);
            this.completionService = new ExecutorCompletionService<>(executorService);
        }

        void submit(Callable<Void> task) {
            completionService.submit(task);
            submittedTasks++;
        }

        void awaitCompletion() throws InterruptedException, ExecutionException {
            for (int i = 0; i < submittedTasks; i++) {
                completionService.take().get();
            }
        }

        @Override
        public void close() {
            executorService.shutdownNow();
        }
    }

    /**
     * Functional interface for snapshot work that uses pooled resources.
     *
     * @param <T> the offset context type
     */
    @FunctionalInterface
    interface PooledWork<T extends OffsetContext> {
        /**
         * Execute the work with the provided pooled resources.
         *
         * @param connection the JDBC connection from the pool
         * @param offset the offset context from the pool
         * @throws Exception if an error occurs during execution
         */
        void execute(JdbcConnection connection, T offset) throws Exception;
    }

    /**
     * Holds the prepared table information for snapshot processing.
     */
    private record PreparedTables(Map<TableId, String> queryTables, Map<TableId, OptionalLong> rowCountTables) {
    }

    /**
     * Functional interface for operations that may throw SQLException.
     *
     * @param <T> the input type
     * @param <R> the result type
     */
    @FunctionalInterface
    interface CheckedFunction<T, R> {
        R apply(T t) throws SQLException;
    }

    /**
     * Prepares tables for snapshot by generating select statements and row counts.
     *
     * @param snapshotContext the snapshot context
     * @param selectGenerator function to generate select statement for a table
     * @param rowCountProvider function to get row count for a table
     * @return the prepared tables with query and row count maps
     * @throws SQLException if row count retrieval fails
     */
    private PreparedTables prepareTables(RelationalSnapshotContext<P, O> snapshotContext,
                                         Function<TableId, Optional<String>> selectGenerator,
                                         CheckedFunction<TableId, OptionalLong> rowCountProvider)
            throws SQLException {

        final Map<TableId, String> queryTables = new LinkedHashMap<>();
        Map<TableId, OptionalLong> rowCountTables = new LinkedHashMap<>();

        for (TableId tableId : snapshotContext.capturedTables) {
            final Optional<String> selectStatement = selectGenerator.apply(tableId);
            if (selectStatement.isPresent()) {
                LOGGER.info("For table '{}' using select statement: '{}'", tableId, selectStatement.get());
                queryTables.put(tableId, selectStatement.get());
                rowCountTables.put(tableId, rowCountProvider.apply(tableId));
            }
            else {
                LOGGER.warn("For table '{}' the select statement was not provided, skipping table", tableId);
                snapshotProgressListener.dataCollectionSnapshotCompleted(snapshotContext.partition, tableId, 0);
            }
        }

        rowCountTables = sortByRowCount(rowCountTables, connectorConfig.snapshotOrderByRowCount());

        return new PreparedTables(queryTables, rowCountTables);
    }

    /**
     * Creates a Callable that borrows resources from pools, executes work, and returns resources.
     *
     * @param connectionPool the pool of JDBC connections
     * @param offsetPool the pool of offset contexts
     * @param work the work to execute with borrowed resources
     * @param errorHandler called if work throws an exception, can be {@code null}
     * @return a Callable wrapping the resource management
     */
    @SuppressWarnings("SameParameterValue")
    private Callable<Void> createPooledResourceCallable(Queue<JdbcConnection> connectionPool,
                                                        Queue<O> offsetPool,
                                                        PooledWork<O> work,
                                                        Runnable errorHandler) {
        return () -> {
            final JdbcConnection connection = connectionPool.poll();
            final O offset = offsetPool.poll();
            try {
                work.execute(connection, offset);
            }
            catch (Exception e) {
                if (errorHandler != null) {
                    errorHandler.run();
                }
                throw e;
            }
            finally {
                offsetPool.add(offset);
                connectionPool.add(connection);
            }
            return null;
        };
    }

    /**
     * Mutable context which is populated in the course of snapshotting.
     */
    public static class RelationalSnapshotContext<P extends Partition, O extends OffsetContext>
            extends SnapshotContext<P, O> {
        public final String catalogName;
        public final Tables tables;
        public final boolean onDemand;

        public Set<TableId> capturedTables;
        public Set<TableId> capturedSchemaTables;

        public RelationalSnapshotContext(P partition, String catalogName, boolean onDemand) {
            super(partition);
            this.catalogName = catalogName;
            this.tables = new Tables();
            this.onDemand = onDemand;
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
