/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot.incremental;

import static io.debezium.pipeline.notification.IncrementalSnapshotNotificationService.TableScanCompletionStatus.EMPTY;
import static io.debezium.pipeline.notification.IncrementalSnapshotNotificationService.TableScanCompletionStatus.NO_PRIMARY_KEY;
import static io.debezium.pipeline.notification.IncrementalSnapshotNotificationService.TableScanCompletionStatus.SQL_EXCEPTION;
import static io.debezium.pipeline.notification.IncrementalSnapshotNotificationService.TableScanCompletionStatus.SUCCEEDED;
import static io.debezium.pipeline.notification.IncrementalSnapshotNotificationService.TableScanCompletionStatus.UNKNOWN_SCHEMA;
import static io.debezium.util.Loggings.maybeRedactSensitiveData;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.annotation.NotThreadSafe;
import io.debezium.data.ValueWrapper;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.IncrementalSnapshotNotificationService.TableScanCompletionStatus;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.signal.SignalPayload;
import io.debezium.pipeline.signal.actions.snapshotting.SnapshotConfiguration;
import io.debezium.pipeline.source.spi.DataChangeEventListener;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.ChangeRecordEmitter;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;
import io.debezium.relational.Column;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.RelationalDatabaseSchema;
import io.debezium.relational.RelationalSnapshotChangeEventSource;
import io.debezium.relational.SnapshotChangeRecordEmitter;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchema;
import io.debezium.relational.Tables;
import io.debezium.schema.DatabaseSchema;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.util.Clock;
import io.debezium.util.ColumnUtils;
import io.debezium.util.Strings;
import io.debezium.util.Threads;
import io.debezium.util.Threads.Timer;

/**
 * An incremental snapshot change event source that emits events from a DB log interleaved with snapshot events.
 */
@NotThreadSafe
public abstract class AbstractIncrementalSnapshotChangeEventSource<P extends Partition, T extends DataCollectionId>
        implements IncrementalSnapshotChangeEventSource<P, T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractIncrementalSnapshotChangeEventSource.class);

    protected final RelationalDatabaseConnectorConfig connectorConfig;
    private final Clock clock;
    private final RelationalDatabaseSchema databaseSchema;
    private final SnapshotProgressListener<P> progressListener;
    private final DataChangeEventListener<P> dataListener;
    private long totalRowsScanned = 0;

    private Table currentTable;

    protected EventDispatcher<P, T> dispatcher;
    protected IncrementalSnapshotContext<T> context = null;
    protected JdbcConnection jdbcConnection;
    protected ChunkQueryBuilder<T> chunkQueryBuilder;
    protected final Map<Struct, Object[]> window = new LinkedHashMap<>();
    protected final NotificationService<P, ? extends OffsetContext> notificationService;
    protected ParallelIncrementalSnapshotCoordinator<P, T> parallelCoordinator;
    protected final IncrementalSnapshotRetryPolicy retryPolicy;

    public AbstractIncrementalSnapshotChangeEventSource(RelationalDatabaseConnectorConfig config,
                                                        JdbcConnection jdbcConnection,
                                                        EventDispatcher<P, T> dispatcher,
                                                        DatabaseSchema<?> databaseSchema,
                                                        Clock clock,
                                                        SnapshotProgressListener<P> progressListener,
                                                        DataChangeEventListener<P> dataChangeEventListener,
                                                        NotificationService<P, ? extends OffsetContext> notificationService) {
        this.connectorConfig = config;
        this.jdbcConnection = jdbcConnection;
        this.chunkQueryBuilder = jdbcConnection.chunkQueryBuilder(config);
        this.dispatcher = dispatcher;
        this.databaseSchema = (RelationalDatabaseSchema) databaseSchema;
        this.clock = clock;
        this.progressListener = progressListener;
        this.dataListener = dataChangeEventListener;
        this.notificationService = notificationService;

        int threads = config.getSnapshotMaxThreads();
        if (threads > 1) {
            try {
                JdbcConnection testConnection = createSnapshotConnection();
                testConnection.close();

                this.parallelCoordinator = new ParallelIncrementalSnapshotCoordinator<>(
                        threads, () -> createSnapshotConnection());

                LOGGER.info("Incremental snapshot multi-threading enabled with {} threads", threads);
            }
            catch (UnsupportedOperationException e) {
                LOGGER.warn("Incremental snapshot multi-threading requested ({} threads) but not supported by this connector. " +
                        "Falling back to single-threaded mode. Reason: {}", threads, e.getMessage());
                this.parallelCoordinator = null;
            }
            catch (Exception e) {
                LOGGER.warn("Failed to initialize parallel incremental snapshot. " +
                        "Falling back to single-threaded mode. Error: {}", e.getMessage());
                this.parallelCoordinator = null;
            }
        }
        else {
            this.parallelCoordinator = null;
            LOGGER.info("Incremental snapshot running in single-threaded mode");
        }

        // Initialize retry policy for handling database connection failures
        this.retryPolicy = new IncrementalSnapshotRetryPolicy(
                config.getIncrementalSnapshotRetryMaxAttempts(),
                config.getIncrementalSnapshotRetryInitialDelayMs(),
                config.getIncrementalSnapshotRetryMaxDelayMs(),
                config.getIncrementalSnapshotRetryBackoffMultiplier(),
                clock);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void closeWindow(P partition, String id, OffsetContext offsetContext) throws InterruptedException {
        context = (IncrementalSnapshotContext<T>) offsetContext.getIncrementalSnapshotContext();
        LOGGER.trace("Closing Window {}", maybeRedactSensitiveData(context));
        if (!context.closeWindow(id)) {
            return;
        }
        sendWindowEvents(partition, offsetContext);
        readChunk(partition, offsetContext);
    }

    @Override
    public void pauseSnapshot(P partition, OffsetContext offsetContext) {
        context = (IncrementalSnapshotContext<T>) offsetContext.getIncrementalSnapshotContext();
        if (context.snapshotRunning() && !context.isSnapshotPaused()) {
            context.pauseSnapshot();
            progressListener.snapshotPaused(partition);
            notificationService.incrementalSnapshotNotificationService().notifyPaused(context, partition, offsetContext);
        }
    }

    @Override
    public void resumeSnapshot(P partition, OffsetContext offsetContext) throws InterruptedException {
        context = (IncrementalSnapshotContext<T>) offsetContext.getIncrementalSnapshotContext();
        if (context.snapshotRunning() && context.isSnapshotPaused()) {
            context.resumeSnapshot();
            progressListener.snapshotResumed(partition);
            notificationService.incrementalSnapshotNotificationService().notifyResumed(context, partition, offsetContext);
            readChunk(partition, offsetContext);
        }
    }

    @Override
    public void processSchemaChange(P partition, OffsetContext offsetContext, DataCollectionId dataCollectionId) throws InterruptedException {
        context = (IncrementalSnapshotContext<T>) offsetContext.getIncrementalSnapshotContext();
        if (dataCollectionId != null && (context.currentDataCollectionId() != null) &&
                dataCollectionId.equals(context.currentDataCollectionId().getId())) {
            rereadChunk(partition, offsetContext);
        }
    }

    public void rereadChunk(P partition, OffsetContext offsetContext) throws InterruptedException {
        if (context == null) {
            return;
        }
        final T dataCollectionId = context.currentDataCollectionId().getId();
        final Map<Struct, Object[]> currentWindow = getWindowForDataCollection(dataCollectionId);

        if (!context.snapshotRunning() || !context.deduplicationNeeded() || currentWindow.isEmpty()) {
            return;
        }
        currentWindow.clear();
        context.revertChunk();
        readChunk(partition, offsetContext);
    }

    protected String getSignalTableName(String dataCollectionId) {
        if (Strings.isNullOrEmpty(dataCollectionId)) {
            return dataCollectionId;
        }
        return jdbcConnection.quotedTableIdString(TableId.parse(dataCollectionId));
    }

    /**
     * Returns the appropriate window buffer for the current data collection.
     * If parallel coordinator is enabled, returns the per-table buffer;
     * otherwise returns the global window buffer.
     *
     * @param dataCollectionId the data collection identifier
     * @return the window buffer to use
     */
    protected Map<Struct, Object[]> getWindowForDataCollection(T dataCollectionId) {
        if (parallelCoordinator != null) {
            return parallelCoordinator.getWindowBuffer(dataCollectionId);
        }
        return window;  // Fallback to global window for single-threaded mode
    }

    protected void sendWindowEvents(P partition, OffsetContext offsetContext) throws InterruptedException {
        final T dataCollectionId = context.currentDataCollectionId().getId();
        final Map<Struct, Object[]> currentWindow = getWindowForDataCollection(dataCollectionId);

        LOGGER.debug("[{}] Sending {} events from window buffer for table {}",
                Thread.currentThread().getName(), currentWindow.size(), dataCollectionId);
        offsetContext.incrementalSnapshotEvents();
        for (Object[] row : currentWindow.values()) {
            sendEvent(partition, dispatcher, offsetContext, row);
        }
        offsetContext.postSnapshotCompletion();
        currentWindow.clear();
    }

    protected void sendEvent(P partition, EventDispatcher<P, T> dispatcher, OffsetContext offsetContext, Object[] row) throws InterruptedException {
        context.sendEvent(keyFromRow(row));
        offsetContext.event(context.currentDataCollectionId().getId(), clock.currentTimeAsInstant());
        dispatcher.dispatchSnapshotEvent(partition, context.currentDataCollectionId().getId(),
                getChangeRecordEmitter(partition, context.currentDataCollectionId().getId(), offsetContext, row),
                dispatcher.getIncrementalSnapshotChangeEventReceiver(dataListener));
    }

    /**
     * Returns a {@link ChangeRecordEmitter} producing the change records for
     * the given table row.
     */
    protected ChangeRecordEmitter<P> getChangeRecordEmitter(P partition, T dataCollectionId,
                                                            OffsetContext offsetContext, Object[] row) {
        return new SnapshotChangeRecordEmitter<>(partition, offsetContext, row, clock, connectorConfig);
    }

    @SuppressWarnings("unchecked")
    protected void deduplicateWindow(DataCollectionId dataCollectionId, Object key) {
        if (context.currentDataCollectionId() == null || !context.currentDataCollectionId().getId().equals(dataCollectionId)) {
            return;
        }
        if (key instanceof Struct) {
            final Map<Struct, Object[]> currentWindow = getWindowForDataCollection((T) dataCollectionId);
            if (currentWindow.remove((Struct) key) != null) {
                LOGGER.info("[{}] Removed '{}' from window",
                        Thread.currentThread().getName(), maybeRedactSensitiveData(key));
            }
        }
    }

    /**
     * Update low watermark for the incremental snapshot chunk
     */
    protected abstract void emitWindowOpen(P partition, OffsetContext offsetContext) throws SQLException;

    /**
     * Update high watermark for the incremental snapshot chunk
     */
    protected abstract void emitWindowClose(P partition, OffsetContext offsetContext) throws Exception;

    /**
     * Creates a new JDBC connection in NORMAL mode for snapshot reads.
     *
     * <p>This connection is separate from the main WAL streaming connection and is used
     * exclusively for parallel snapshot processing. The connection MUST be in normal mode
     * (not replication mode) to allow standard SQL SELECT queries.
     *
     * <p><b>Important:</b> Do not return the main jdbcConnection - this method should
     * create a NEW connection instance.
     *
     * <p><b>Default implementation:</b> Throws {@link UnsupportedOperationException}. Connectors
     * that support parallel incremental snapshots must override this method.
     *
     * @return a new JDBC connection in NORMAL mode
     * @throws SQLException if connection creation fails
     * @throws UnsupportedOperationException if this connector does not support parallel snapshots
     */
    protected JdbcConnection createSnapshotConnection() throws SQLException {
        throw new UnsupportedOperationException(
                "Parallel snapshot connections not supported by this connector implementation. " +
                "Override createSnapshotConnection() to enable multi-threaded incremental snapshots.");
    }

    @Override
    @SuppressWarnings("unchecked")
    public void init(P partition, OffsetContext offsetContext) {
        if (offsetContext == null) {
            LOGGER.info("Empty incremental snapshot change event source started, no action needed");
            postIncrementalSnapshotCompleted();
            return;
        }
        context = (IncrementalSnapshotContext<T>) offsetContext.getIncrementalSnapshotContext();
        if (!context.snapshotRunning()) {
            LOGGER.info("No incremental snapshot in progress, no action needed on start");
            postIncrementalSnapshotCompleted();
            return;
        }

        // FASE 3: Validate restored context for PostgreSQL Read-Only incremental snapshot
        LOGGER.info("Incremental snapshot in progress detected. Validating restored state...");
        if (!validateRestoredContext(context)) {
            LOGGER.warn("Restored snapshot context validation failed. Resetting snapshot context.");
            postIncrementalSnapshotCompleted();
            return;
        }

        LOGGER.info("Incremental snapshot resuming from: table={}, chunk position={}",
                context.currentDataCollectionId() != null ? context.currentDataCollectionId().getId() : "start",
                context.chunkEndPosititon() != null ? java.util.Arrays.toString(context.chunkEndPosititon()) : "start");

        try {
            preIncrementalSnapshotStart();
            progressListener.snapshotStarted(partition);
            readChunk(partition, offsetContext);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new DebeziumException("Reading of initial chunk after connector restart was interrupted", e);
        }
        LOGGER.info("Incremental snapshot resumed successfully after restart");
    }

    protected void readChunk(P partition, OffsetContext offsetContext) throws InterruptedException {

        LOGGER.trace("[{}] Reading chunk", Thread.currentThread().getName());
        checkAndProcessStopFlag(partition, offsetContext);
        if (!context.snapshotRunning()) {
            LOGGER.info("Skipping read chunk because snapshot is not running");
            postIncrementalSnapshotCompleted();
            return;
        }
        if (context.isSnapshotPaused()) {
            LOGGER.info("Incremental snapshot was paused.");
            return;
        }

        JdbcConnection effectiveConnection = jdbcConnection;
        boolean acquiredPoolConnection = false;

        try {
            if (parallelCoordinator != null) {
                JdbcConnection pooled = parallelCoordinator.borrowConnection();
                if (pooled != null) {
                    effectiveConnection = pooled;
                    acquiredPoolConnection = true;
                    LOGGER.debug("[{}] Acquired worker connection for table '{}' chunk",
                            Thread.currentThread().getName(),
                            context.currentDataCollectionId() != null ? context.currentDataCollectionId().getId() : "unknown");
                }
            }

            preReadChunk(context);
            // This commit should be unnecessary and might be removed later
            effectiveConnection.commit();
            context.startNewChunk();
            emitWindowOpen(partition, offsetContext);
            LOGGER.trace("Window open emitted");
            while (context.snapshotRunning()) {

                LOGGER.trace("Checking if current table is invalid");
                if (isTableInvalid(partition, offsetContext)) {
                    continue;
                }
                if (connectorConfig.isIncrementalSnapshotSchemaChangesEnabled() && !schemaHistoryIsUpToDate()) {
                    // Schema has changed since the previous window.
                    // Closing the current window and repeating schema verification within the following window.
                    break;
                }
                final TableId currentTableId = (TableId) context.currentDataCollectionId().getId();
                if (context.maximumKey().isEmpty()) {
                    currentTable = chunkQueryBuilder.prepareTable(context, refreshTableSchema(currentTable));
                    Object[] maximumKey;
                    try {
                        final JdbcConnection connForMaxKey = effectiveConnection;
                        maximumKey = retryPolicy.executeWithRetry(
                                () -> connForMaxKey.queryAndMap(
                                        chunkQueryBuilder.buildMaxPrimaryKeyQuery(context, currentTable, context.currentDataCollectionId().getAdditionalCondition()), rs -> {
                                            if (!rs.next()) {
                                                return null;
                                            }
                                            return keyFromRow(connForMaxKey.rowToArray(currentTable, rs,
                                                    ColumnUtils.toArray(rs, currentTable)));
                                        }),
                                "read maximum key for table " + currentTableId);
                        context.maximumKey(maximumKey);
                    }
                    catch (SQLException e) {
                        LOGGER.error("[{}] Failed to read maximum key for table {} after retries",
                                Thread.currentThread().getName(), currentTableId, e);
                        notificationService.incrementalSnapshotNotificationService().notifyTableScanCompleted(context, partition, offsetContext, totalRowsScanned,
                                SQL_EXCEPTION);
                        nextDataCollection(partition, offsetContext);
                        continue;
                    }
                    if (context.maximumKey().isEmpty()) {
                        LOGGER.info("[{}] No maximum key returned by the query, incremental snapshotting of table '{}' finished as it is empty",
                                Thread.currentThread().getName(),
                                currentTableId);
                        notificationService.incrementalSnapshotNotificationService().notifyTableScanCompleted(context, partition, offsetContext, totalRowsScanned, EMPTY);
                        nextDataCollection(partition, offsetContext);
                        continue;
                    }
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("[{}] Incremental snapshot for table '{}' will end at position {}",
                                Thread.currentThread().getName(), currentTableId,
                                maybeRedactSensitiveData(context.maximumKey().orElse(new Object[0])));
                    }
                }

                try {
                    // Use worker connection for data read
                    if (createDataEventsForTable(partition, effectiveConnection)) {

                        if (!context.snapshotRunning()) { // A stop signal has been processed and window cleared.
                            return;
                        }

                        @SuppressWarnings("unchecked")
                        final Map<Struct, Object[]> currentWindow = getWindowForDataCollection((T) currentTableId);

                        if (currentWindow.isEmpty()) {
                            LOGGER.info("[{}] No data returned by the query, incremental snapshotting of table '{}' finished",
                                    Thread.currentThread().getName(), currentTableId);

                            notificationService.incrementalSnapshotNotificationService().notifyTableScanCompleted(context, partition, offsetContext, totalRowsScanned,
                                    SUCCEEDED);

                            tableScanCompleted(partition);
                            nextDataCollection(partition, offsetContext);
                        }
                        else {

                            notificationService.incrementalSnapshotNotificationService().notifyInProgress(context, partition, offsetContext);
                            break;
                        }
                    }
                    else {
                        context.revertChunk();
                        break;
                    }
                }
                catch (SQLException e) {
                    notificationService.incrementalSnapshotNotificationService().notifyTableScanCompleted(context, partition, offsetContext, totalRowsScanned,
                            SQL_EXCEPTION);
                    nextDataCollection(partition, offsetContext);
                }
            }
            emitWindowClose(partition, offsetContext);
            LOGGER.trace("Window close emitted");
        }
        catch (SQLException e) {
            warnAndSkip((TableId) context.currentDataCollectionId().getId(), partition, offsetContext,
                    SQL_EXCEPTION,
                    "SQL error while executing incremental snapshot for table '{}', skipping and continuing streaming");
        }
        catch (Exception e) {
            if (e.getCause() instanceof SQLException) {
                warnAndSkip((TableId) context.currentDataCollectionId().getId(), partition, offsetContext,
                        SQL_EXCEPTION,
                        "SQL error while executing incremental snapshot for table '{}', skipping and continuing streaming");
            }
            else {
                throw new DebeziumException(String.format("Database error while executing incremental snapshot for table '%s'", context.currentDataCollectionId()), e);
            }
        }
        finally {
            if (acquiredPoolConnection && effectiveConnection != null) {
                parallelCoordinator.returnConnection(effectiveConnection);
            }

            postReadChunk(context);
            if (!context.snapshotRunning()) {
                postIncrementalSnapshotCompleted();
            }
        }
    }

    /**
     * Processes multiple tables in parallel using worker threads.
     *
     * <p>This method spawns worker threads to process tables concurrently, significantly
     * improving throughput when snapshotting multiple tables. Each worker processes one
     * table completely (all chunks) before moving to the next table.
     *
     * @param partition the partition
     * @param offsetContext the offset context
     * @param dataCollections list of tables to snapshot
     * @param correlationId correlation ID for this snapshot operation
     * @param additionalConditions additional SQL conditions per table
     * @param surrogateKey surrogate key column name
     */
    protected void processTablesInParallel(
            P partition,
            OffsetContext offsetContext,
            List<DataCollection<T>> dataCollections,
            String correlationId) {

        final int maxParallelTables = parallelCoordinator.getConnectionPoolSize();
        final int totalTables = dataCollections.size();

        LOGGER.info("Starting parallel table processing for {} tables with {} workers",
                totalTables, parallelCoordinator.getThreadCount());

        long totalRowsAllTables = 0;
        int tablesProcessed = 0;

        for (int batchStart = 0; batchStart < totalTables; batchStart += maxParallelTables) {
            int batchEnd = Math.min(batchStart + maxParallelTables, totalTables);
            List<DataCollection<T>> batch = dataCollections.subList(batchStart, batchEnd);
            int batchNumber = (batchStart / maxParallelTables) + 1;
            int totalBatches = (int) Math.ceil((double) totalTables / maxParallelTables);

            LOGGER.info("Processing batch {}/{}: tables {}-{} of {}",
                    batchNumber, totalBatches, batchStart + 1, batchEnd, totalTables);

            final List<TableSnapshotWorker<P, T>> workers = new ArrayList<>();
            final List<JdbcConnection> acquiredConnections = new ArrayList<>();

            try {
                parallelCoordinator.resetTaskCount();

                for (DataCollection<T> dataCollection : batch) {
                    TableSnapshotContext<T> tableContext = new TableSnapshotContext<>(dataCollection);

                    JdbcConnection workerConnection = parallelCoordinator.borrowConnection();
                    if (workerConnection == null) {
                        LOGGER.warn("No connection available for table '{}', skipping", dataCollection.getId());
                        continue;
                    }
                    acquiredConnections.add(workerConnection);

                    Map<Struct, Object[]> tableWindowBuffer = parallelCoordinator.getWindowBuffer(dataCollection.getId());

                    TableSnapshotWorker.WatermarkCallback watermarkCallback = new TableSnapshotWorker.WatermarkCallback() {
                        @Override
                        public void openWindow() {
                        }

                        @Override
                        public void closeWindow() {
                        }
                    };

                    TableSnapshotWorker<P, T> worker = new TableSnapshotWorker<>(
                            tableContext,
                            workerConnection,
                            connectorConfig,
                            databaseSchema,
                            chunkQueryBuilder,
                            dispatcher,
                            tableWindowBuffer,
                            retryPolicy,
                            partition,
                            watermarkCallback,
                            offsetContext);

                    workers.add(worker);

                    final JdbcConnection conn = workerConnection;
                    parallelCoordinator.submit(() -> {
                        try {
                            worker.run();
                        }
                        finally {
                            parallelCoordinator.returnConnection(conn);
                        }
                        return null;
                    });

                    LOGGER.info("Spawned worker for table '{}' (batch {}/{})",
                            dataCollection.getId(), batchNumber, totalBatches);
                }

                parallelCoordinator.awaitCompletion();

                for (TableSnapshotWorker<P, T> worker : workers) {
                    long rowsRead = worker.getTotalRowsRead();
                    totalRowsAllTables += rowsRead;
                    tablesProcessed++;

                    LOGGER.info("Completed table '{}' ({} rows)",
                            worker.getContext().currentDataCollectionId().getId(), rowsRead);
                }

                for (TableSnapshotWorker<P, T> worker : workers) {
                    if (worker.getContext().isCompleted()) {
                        context.nextDataCollection();
                    }
                }
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOGGER.error("Interrupted during parallel table processing", e);
                throw new DebeziumException("Parallel snapshot interrupted", e);
            }
            catch (ExecutionException e) {
                LOGGER.error("Worker failed during parallel table processing", e);
                throw new DebeziumException("Parallel snapshot worker failed", e);
            }
            finally {
                acquiredConnections.clear();
            }
        }

        LOGGER.info("All {} tables completed. Total rows: {}", tablesProcessed, totalRowsAllTables);
    }

    private boolean isTableInvalid(P partition, OffsetContext offsetContext) {
        final TableId currentTableId = (TableId) context.currentDataCollectionId().getId();
        currentTable = databaseSchema.tableFor(currentTableId);
        if (currentTable == null) {
            LOGGER.info("Schema not found for table '{}', known tables {}. Will attempt to retrieve this schema",
                    currentTableId, databaseSchema.tableIds());
            try {
                retrieveAndRefreshSchema(currentTableId, partition, offsetContext);
                currentTable = databaseSchema.tableFor(currentTableId);
                if (currentTable == null) {
                    warnAndSkip(currentTableId, partition, offsetContext, UNKNOWN_SCHEMA, "Schema retrieval failed to populate the schema as expected for {}");
                    return true;
                }
            }
            catch (Exception e) {
                LOGGER.warn("Failed to retrieve schema for {}", currentTableId, e);
                warnAndSkip(currentTableId, partition, offsetContext, UNKNOWN_SCHEMA, "Schema retrieval failed due to an exception for {}");
                return true;
            }
        }
        currentTable = chunkQueryBuilder.prepareTable(context, currentTable);
        if (chunkQueryBuilder.getQueryColumns(context, currentTable).isEmpty()) {
            warnAndSkip(currentTableId, partition, offsetContext, NO_PRIMARY_KEY, "Incremental snapshot for table '{}' skipped because the table has no primary keys");
            return true;
        }
        return false;
    }

    private void warnAndSkip(TableId currentTableId, P partition, OffsetContext offsetContext, TableScanCompletionStatus status, String reason) {
        LOGGER.warn(reason, currentTableId);
        notificationService.incrementalSnapshotNotificationService()
                .notifyTableScanCompleted(context, partition, offsetContext, totalRowsScanned, status);
        nextDataCollection(partition, offsetContext);
    }

    private void retrieveAndRefreshSchema(TableId tableId, P partition, OffsetContext offsetContext) throws SQLException, InterruptedException {
        Table newTable = readSchemaForTable(tableId);

        // Create and dispatch schema change event
        createAndDispatchSchemaChangeEvent(newTable, partition, offsetContext, tableId);

        // Refresh schema and assign to current table
        databaseSchema.refresh(newTable);
        currentTable = newTable;
        LOGGER.info("Schema successfully read and dispatched for table '{}'", tableId);
    }

    /**
     * Verifies that in-memory representation of the table’s schema is up to date with the table's schema in the database.
     * <p>
     * Verification is a two step process:
     * <ol>
     * <li>Save table's schema from the database to the context
     * <li>Verify schema hasn't changed in the following window. If schema has changed repeat the process
     * </ol>
     * Two step process allows to wait for the connector to receive the DDL event in the binlog stream and update the in-memory representation of the table’s schema.
     * <p>
     * Verification is done at the beginning of the incremental snapshot and on every schema change during the snapshotting.
     */
    private boolean schemaHistoryIsUpToDate() {
        if (context.isSchemaVerificationPassed()) {
            return true;
        }
        verifySchemaUnchanged();
        return context.isSchemaVerificationPassed();
    }

    /**
     * Verifies that table's schema in the database has not changed since it was captured in the previous window
     */
    private void verifySchemaUnchanged() {
        Table tableSchemaInDatabase = readSchema();
        if (context.getSchema() != null) {
            context.setSchemaVerificationPassed(context.getSchema().equals(tableSchemaInDatabase));
        }
        context.setSchema(tableSchemaInDatabase);
    }

    private Table readSchema() {
        final String selectStatement = chunkQueryBuilder.buildChunkQuery(context, currentTable, 0, Optional.empty());
        LOGGER.debug("Reading schema for table '{}' using select statement: '{}'", currentTable.id(), selectStatement);

        try (PreparedStatement statement = chunkQueryBuilder.readTableChunkStatement(context, currentTable, selectStatement);
                ResultSet rs = statement.executeQuery()) {
            return getTable(rs);
        }
        catch (SQLException e) {
            throw new DebeziumException("Snapshotting of table " + currentTable.id() + " failed", e);
        }
    }

    private void nextDataCollection(P partition, OffsetContext offsetContext) {
        context.nextDataCollection();
        if (!context.snapshotRunning()) {
            progressListener.snapshotCompleted(partition);
            notificationService.incrementalSnapshotNotificationService().notifyCompleted(context, partition, offsetContext);
            context.unsetCorrelationId();
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void addDataCollectionNamesToSnapshot(SignalPayload<P> signalPayload, SnapshotConfiguration snapshotConfiguration)
            throws InterruptedException {

        final OffsetContext offsetContext = signalPayload.offsetContext;
        final P partition = signalPayload.partition;
        final String correlationId = signalPayload.id;

        context = (IncrementalSnapshotContext<T>) offsetContext.getIncrementalSnapshotContext();
        boolean shouldReadChunk = !context.snapshotRunning();

        List<String> expandedDataCollectionIds = expandAndDedupeDataCollectionIds(snapshotConfiguration.getDataCollections());
        LOGGER.trace("Configured data collections {}", snapshotConfiguration.getDataCollections());
        LOGGER.trace("Expanded data collections {}", expandedDataCollectionIds);
        if (expandedDataCollectionIds.size() > snapshotConfiguration.getDataCollections().size()) {
            LOGGER.info("Data-collections to snapshot have been expanded from {} to {}", snapshotConfiguration.getDataCollections(), expandedDataCollectionIds);
        }

        final List<DataCollection<T>> newDataCollectionIds = context.addDataCollectionNamesToSnapshot(correlationId, expandedDataCollectionIds,
                snapshotConfiguration.getAdditionalConditions(),
                snapshotConfiguration.getSurrogateKey());

        validateSignalTableConfiguration(newDataCollectionIds);

        if (shouldReadChunk) {

            List<T> monitoredDataCollections = newDataCollectionIds.stream()
                    .map(DataCollection::getId).collect(Collectors.toList());
            LOGGER.trace("Monitored data collections {}", newDataCollectionIds);

            progressListener.snapshotStarted(partition);

            notificationService.incrementalSnapshotNotificationService().notifyStarted(context, partition, offsetContext);

            progressListener.monitoredDataCollectionsDetermined(partition, monitoredDataCollections);

            // Use parallel processing if available
            if (parallelCoordinator != null && newDataCollectionIds.size() > 1) {
                LOGGER.info("Starting PARALLEL table-level snapshot for {} tables with {} worker threads",
                        newDataCollectionIds.size(), parallelCoordinator.getThreadCount());
                processTablesInParallel(partition, offsetContext, newDataCollectionIds, correlationId);
            }
            else {
                // Fall back to sequential processing
                readChunk(partition, offsetContext);
            }
        }
    }

    /**
     * Validates that signal table configuration is appropriate for the tables being snapshotted.
     * For multi-database connectors with a single signal table, warns if tables from different
     * databases are being snapshotted.
     */
    protected void validateSignalTableConfiguration(List<DataCollection<T>> dataCollections) {

        if (connectorConfig.getSignalingDataCollectionIds().size() != 1) {
            // Either no signal tables or multiple signal tables configured - no validation needed
            return;
        }

        // This means that for multitask connector cross signaling is not permitted.
        String signalTableId = connectorConfig.getSignalingDataCollectionIds().get(0);
        Optional<String> signalDatabase = getDatabaseName(signalTableId);

        if (signalDatabase.isEmpty()) {
            return;
        }

        // Check if any data collection is from a different database than the signal table
        for (DataCollection<T> dataCollection : dataCollections) {
            Optional<String> targetDatabase = getDatabaseName(dataCollection.getId());
            if (targetDatabase.isPresent() && !targetDatabase.get().equals(signalDatabase.get())) {
                LOGGER.warn("Incremental snapshot for table '{}' in database '{}' is using signal table from database '{}'. " +
                        "This may result in incorrect partition/offset context in emitted events. " +
                        "For multi-database connectors, configure one signal table per database using a comma-separated list " +
                        "in 'signal.data.collection' property (e.g., 'db1.dbo.debezium_signal,db2.dbo.debezium_signal').",
                        dataCollection.getId(), targetDatabase, signalDatabase);
                return;
            }
        }
    }

    /**
     * Extract the database name from a data collection ID.
     * Default implementation handles TableId, subclasses may override.
     */
    protected Optional<String> getDatabaseName(Object dataCollectionId) {

        if (dataCollectionId instanceof io.debezium.relational.TableId tableId) {
            return Optional.ofNullable(tableId.catalog());
        }
        if (dataCollectionId instanceof String tableId) {
            return Optional.ofNullable(io.debezium.relational.TableId.parse(tableId).catalog());
        }

        return Optional.empty();
    }

    @Override
    @SuppressWarnings("unchecked")
    public void requestStopSnapshot(P partition, OffsetContext offsetContext, Map<String, Object> additionalData, List<String> dataCollectionIds) {
        context = (IncrementalSnapshotContext<T>) offsetContext.getIncrementalSnapshotContext();
        context.requestSnapshotStop(dataCollectionIds);
    }

    @SuppressWarnings("unchecked")
    private void checkAndProcessStopFlag(P partition, OffsetContext offsetContext) {
        context = (IncrementalSnapshotContext<T>) offsetContext.getIncrementalSnapshotContext();
        List<String> dataCollectionsToStop = context.getDataCollectionsToStop();
        if (dataCollectionsToStop.isEmpty()) {
            return;
        }
        LOGGER.trace("Stopping incremental snapshot with context {}", maybeRedactSensitiveData(context));
        if (!context.snapshotRunning()) {
            LOGGER.warn("No active incremental snapshot, stop ignored");
            context.unsetCorrelationId();
            return;
        }
        final List<String> expandedDataCollectionIds = expandAndDedupeDataCollectionIds(dataCollectionsToStop);
        final List<String> stopped = new ArrayList<>();
        LOGGER.info("Removing '{}' collections from incremental snapshot", expandedDataCollectionIds);
        // Iterate and remove any collections that are not current.
        // If current is marked for removal, delay that until after others have been removed.
        TableId stopCurrentTableId = null;
        for (String dataCollectionId : expandedDataCollectionIds) {
            final TableId collectionId = TableId.parse(dataCollectionId);
            if (currentTable != null && currentTable.id().equals(collectionId)) {
                stopCurrentTableId = currentTable.id();
            }
            else {
                if (context.removeDataCollectionFromSnapshot(dataCollectionId)) {
                    stopped.add(dataCollectionId);
                    LOGGER.info("Removed '{}' from incremental snapshot collection list.", collectionId);
                }
                else {
                    LOGGER.warn("Could not remove '{}', collection is not part of the incremental snapshot.", collectionId);
                }
            }
        }
        // If current is requested to stop, proceed with stopping it.
        if (stopCurrentTableId != null) {
            LOGGER.info("Removed current collection '{}' from incremental snapshot collection list.", stopCurrentTableId);
            tableScanCompleted(partition);
            stopped.add(stopCurrentTableId.identifier());
            // If snapshot has no more collections, abort; otherwise advance to the next collection.
            if (!context.snapshotRunning()) {
                LOGGER.info("Incremental snapshot has stopped.");
                progressListener.snapshotAborted(partition);
            }
            else {
                LOGGER.info("Advancing to next available collection in the incremental snapshot.");
                nextDataCollection(partition, offsetContext);
            }
        }
        notificationService.incrementalSnapshotNotificationService().notifyAborted(context, partition, offsetContext, stopped);
        if (!context.snapshotRunning()) {
            context.unsetCorrelationId();
        }
        LOGGER.info("Removed collections from incremental snapshot: '{}'", stopped);
    }

    /**
     * Expands the string-based list of data collection ids if supplied using regex to a list of
     * all matching explicit data collection ids.
     */
    private List<String> expandAndDedupeDataCollectionIds(List<String> dataCollectionIds) {

        return dataCollectionIds
                .stream()
                .flatMap(x -> {
                    final List<String> ids = databaseSchema
                            .tableIds()
                            .stream()
                            .map(TableId::identifier)
                            .filter(t -> Pattern.compile(x).matcher(t).matches())
                            .collect(Collectors.toList());
                    return ids.isEmpty() ? Stream.of(x) : ids.stream();
                }).distinct().collect(Collectors.toList());
    }

    /**
     * Dispatches the data change events for the records of a single table.
     */
    /**
     * Creates data events for the current table using a specific JDBC connection.
     *
     * <p>This overload allows specifying which connection to use, enabling parallel reads
     * with worker connections from the connection pool.
     *
     * @param partition the partition context
     * @param connection the JDBC connection to use for this read
     * @return true if data was read, false if schema changed
     * @throws SQLException if database error occurs
     */
    private boolean createDataEventsForTable(P partition, JdbcConnection connection) throws SQLException {
        // Temporarily swap the connection
        JdbcConnection originalConnection = this.jdbcConnection;
        try {
            this.jdbcConnection = connection;
            return createDataEventsForTable(partition);
        }
        finally {
            // Restore original connection
            this.jdbcConnection = originalConnection;
        }
    }

    private boolean createDataEventsForTable(P partition) throws SQLException {
        long exportStart = clock.currentTimeInMillis();
        LOGGER.debug("Exporting data chunk from table '{}' (total {} tables)", currentTable.id(), context.dataCollectionsToBeSnapshottedCount());

        final String selectStatement = chunkQueryBuilder.buildChunkQuery(context, currentTable, context.currentDataCollectionId().getAdditionalCondition());
        LOGGER.debug("\t For table '{}' using select statement: '{}', key: '{}', maximum key: '{}'", currentTable.id(),
                selectStatement, context.chunkEndPosititon(), maybeRedactSensitiveData(context.maximumKey().get()));

        final TableSchema tableSchema = databaseSchema.schemaFor(currentTable.id());
        final T dataCollectionId = context.currentDataCollectionId().getId();
        final Map<Struct, Object[]> currentWindow = getWindowForDataCollection(dataCollectionId);

        try (PreparedStatement statement = chunkQueryBuilder.readTableChunkStatement(context, currentTable, selectStatement);
                ResultSet rs = statement.executeQuery()) {
            if (checkSchemaChanges(rs)) {
                return false;
            }
            final ColumnUtils.ColumnArray columnArray = ColumnUtils.toArray(rs, currentTable);
            long rows = 0;
            Timer logTimer = getTableScanLogTimer();

            Object[] lastRow = null;
            Object[] firstRow = null;
            while (rs.next()) {
                rows++;
                final Object[] row = jdbcConnection.rowToArray(currentTable, rs, columnArray);
                if (firstRow == null) {
                    firstRow = row;
                }
                final Struct keyStruct = tableSchema.keyFromColumnData(row);
                currentWindow.put(keyStruct, row);
                if (logTimer.expired()) {
                    long stop = clock.currentTimeInMillis();
                    LOGGER.debug("\t Exported {} records for table '{}' after {}", rows, currentTable.id(),
                            Strings.duration(stop - exportStart));
                    logTimer = getTableScanLogTimer();
                }
                lastRow = row;
            }
            final Object[] firstKey = keyFromRow(firstRow);
            final Object[] lastKey = keyFromRow(lastRow);
            if (context.isNonInitialChunk()) {
                progressListener.currentChunk(partition, context.currentChunkId(), firstKey, lastKey);
            }
            else {
                progressListener.currentChunk(partition, context.currentChunkId(), firstKey, lastKey, context.maximumKey().orElse(null));
            }
            context.nextChunkPosition(lastKey);
            if (lastRow != null) {
                LOGGER.debug("\t Next window will resume from {}", (Object) context.chunkEndPosititon());
            }

            LOGGER.debug("\t Finished exporting {} records for window of table table '{}'; total duration '{}'", rows,
                    currentTable.id(), Strings.duration(clock.currentTimeInMillis() - exportStart));
            incrementTableRowsScanned(partition, rows);
        }
        catch (SQLException e) {
            LOGGER.error("Snapshotting of table {} failed. Skipping it", currentTable.id(), e);
            throw e;
        }
        return true;
    }

    private boolean checkSchemaChanges(ResultSet rs) throws SQLException {
        if (!connectorConfig.isIncrementalSnapshotSchemaChangesEnabled()) {
            return false;
        }
        Table schema = getTable(rs);
        if (!schema.equals(context.getSchema())) {
            context.setSchemaVerificationPassed(false);
            Table oldSchema = context.getSchema();
            context.setSchema(schema);
            LOGGER.info("Schema has changed during the incremental snapshot: Old Schema: {} New Schema: {}", oldSchema, schema);
            return true;
        }
        return false;
    }

    private Table getTable(ResultSet rs) throws SQLException {
        final ResultSetMetaData metaData = rs.getMetaData();
        List<Column> columns = new ArrayList<>();
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            Column column = Column.editor()
                    .name(metaData.getColumnName(i))
                    .jdbcType(metaData.getColumnType(i))
                    .type(metaData.getColumnTypeName(i))
                    .optional(metaData.isNullable(i) > 0)
                    .length(metaData.getPrecision(i))
                    .scale(metaData.getScale(i))
                    .create();
            columns.add(column);
        }
        Collections.sort(columns);
        return Table.editor()
                .tableId(currentTable.id())
                .addColumns(columns)
                .create();
    }

    private void incrementTableRowsScanned(P partition, long rows) {
        totalRowsScanned += rows;
        progressListener.rowsScanned(partition, currentTable.id(), totalRowsScanned);
    }

    private void tableScanCompleted(P partition) {
        progressListener.dataCollectionSnapshotCompleted(partition, currentTable.id(), totalRowsScanned);
        totalRowsScanned = 0;
        // Reset chunk/table information in metrics
        progressListener.currentChunk(partition, null, null, null, null);
    }

    private Timer getTableScanLogTimer() {
        return Threads.timer(clock, RelationalSnapshotChangeEventSource.LOG_INTERVAL);
    }

    @SuppressWarnings("unchecked")
    private Object[] keyFromRow(Object[] row) {
        if (row == null) {
            return null;
        }
        final List<Column> keyColumns = chunkQueryBuilder.getQueryColumns(context, currentTable);
        final Object[] key = new Object[keyColumns.size()];
        for (int i = 0; i < keyColumns.size(); i++) {
            final Object fieldValue = row[keyColumns.get(i).position() - 1];
            key[i] = fieldValue instanceof ValueWrapper<?> ? ((ValueWrapper<Object>) fieldValue).getWrappedValue()
                    : fieldValue;
        }
        return key;
    }

    protected void setContext(IncrementalSnapshotContext<T> context) {
        this.context = context;
    }

    protected void preIncrementalSnapshotStart() {
        // no-op
    }

    protected void preReadChunk(IncrementalSnapshotContext<T> context) {
        LOGGER.trace("[{}] Pre read chunk - checking database connection", Thread.currentThread().getName());

        try {
            retryPolicy.executeWithRetry(
                    () -> {
                        if (!jdbcConnection.isValid()) {
                            LOGGER.debug("[{}] Database connection not valid, reconnecting...",
                                    Thread.currentThread().getName());
                            jdbcConnection.connect();
                        }
                        return null;
                    },
                    "preReadChunk connection validation");
        }
        catch (SQLException e) {
            throw new DebeziumException("Database error while checking jdbcConnection in preReadChunk after retries", e);
        }
    }

    protected void postReadChunk(IncrementalSnapshotContext<T> context) {
        // no-op
    }

    protected void postIncrementalSnapshotCompleted() {
        // no-op
    }

    /**
     * Shutdown the incremental snapshot change event source and release resources.
     * This should be called when the connector is stopping.
     */
    public void shutdown() {
        if (parallelCoordinator != null) {
            LOGGER.info("Shutting down parallel incremental snapshot coordinator");
            parallelCoordinator.shutdown();
        }
    }

    /**
     * Determines if parallel read should be used for the current snapshot.
     *
     * <p>Parallel read is enabled when:
     * <ul>
     *   <li>Connection pool is initialized (multi-threading enabled)</li>
     *   <li>There are at least 2 tables to snapshot (no benefit for single table)</li>
     * </ul>
     *
     * @return true if parallel read should be used, false for sequential read
     */
    protected boolean shouldUseParallelRead() {
        if (parallelCoordinator == null) {
            return false;
        }

        int remainingTables = context.dataCollectionsToBeSnapshottedCount();
        if (remainingTables < 2) {
            LOGGER.trace("[{}] Only {} table(s) remaining, using sequential read",
                    Thread.currentThread().getName(), remainingTables);
            return false;
        }

        LOGGER.debug("[{}] Multiple tables available ({} remaining), using parallel read",
                Thread.currentThread().getName(), remainingTables);
        return true;
    }

    /**
     * Reads chunks from the current table in parallel using worker threads.
     *
     * <p>Each worker thread:
     * <ul>
     *   <li>Acquires a dedicated JDBC connection from the pool</li>
     *   <li>Reads one chunk from the current table</li>
     *   <li>Writes data to the shared window buffer</li>
     *   <li>Returns the connection to the pool</li>
     * </ul>
     *
     * <p>This approach parallelizes chunk reading within a single table, maintaining
     * consistency while improving throughput for large tables.
     *
     * @param partition the partition context
     * @param offsetContext the offset context
     * @throws InterruptedException if interrupted while waiting for workers
     */

    /**
     * Validates the restored incremental snapshot context after a connector restart.
     *
     * <p>This method provides a hook for connector-specific validation logic.
     * The default implementation always returns true (no validation).
     * Subclasses can override this to implement specific validation rules.
     *
     * <p>For PostgreSQL, this method will:
     * <ul>
     *   <li>Reset stale watermarks (pg_snapshot is session-specific)</li>
     *   <li>Verify that there are tables to snapshot</li>
     *   <li>Log information about the restored state</li>
     * </ul>
     *
     * @param context The incremental snapshot context to validate
     * @return true if the context is valid and snapshot can resume, false if snapshot should stop
     */
    protected boolean validateRestoredContext(IncrementalSnapshotContext<T> context) {
        // Try to call validateRestoredContext() if available (PostgreSQL Read-Only implementation)
        try {
            java.lang.reflect.Method method = context.getClass().getMethod("validateRestoredContext");
            Boolean result = (Boolean) method.invoke(context);
            return result != null ? result : true;
        }
        catch (NoSuchMethodException e) {
            // Method not available, use default validation
            LOGGER.debug("No validateRestoredContext() method found in context class {}, using default validation",
                    context.getClass().getSimpleName());
        }
        catch (Exception e) {
            LOGGER.warn("Error calling validateRestoredContext() on context, assuming valid", e);
        }

        // Default validation: just check if there are tables to snapshot
        if (context.dataCollectionsToBeSnapshottedCount() == 0) {
            LOGGER.warn("No data collections to snapshot in restored context");
            return false;
        }

        LOGGER.info("Restored incremental snapshot context validated (default validation). Tables to snapshot: {}",
                context.dataCollectionsToBeSnapshottedCount());
        return true;
    }

    protected Table refreshTableSchema(Table table) throws SQLException {
        // default behavior is to simply return the existing table with no refresh
        // this allows connectors that may require a schema refresh to trigger it, such as PostgreSQL
        // since schema changes are not emitted as change events in the same way that they are for
        // connectors like MySQL or Oracle
        return table;
    }

    /**
     * Reads the schema for the specified table ID.
     * This method can be overridden if custom schema retrieval logic is required.
     */
    protected Table readSchemaForTable(TableId tableId) throws SQLException {
        Tables tempTables = new Tables();
        try {
            jdbcConnection.readSchema(
                    tempTables,
                    null,
                    tableId.schema(),
                    Tables.TableFilter.fromPredicate(id -> id.equals(tableId)),
                    null,
                    false);
        }
        catch (SQLException e) {
            LOGGER.error("SQL error while reading schema for table '{}'", tableId, e);
            throw new DebeziumException(e);
        }

        Table newTable = tempTables.forTable(tableId);
        if (newTable == null) {
            throw new DebeziumException("Failed to populate table with schema for " + tableId);
        }
        return newTable;
    }

    /**
     * Creates and dispatches a schema change event for the given table.
     * This method can be overridden if custom event creation or dispatching is required.
     */
    @SuppressWarnings("unchecked")
    protected void createAndDispatchSchemaChangeEvent(Table newTable, P partition, OffsetContext offsetContext, TableId tableId)
            throws SQLException {
        SchemaChangeEvent schemaChangeEvent = SchemaChangeEvent.ofCreate(
                partition,
                offsetContext,
                newTable.id().catalog(),
                newTable.id().schema(),
                getTableDDL(tableId),
                newTable,
                true);

        try {
            dispatcher.dispatchSchemaChangeEvent(
                    partition,
                    offsetContext,
                    (T) tableId,
                    receiver -> {
                        try {
                            receiver.schemaChangeEvent(schemaChangeEvent);
                        }
                        catch (Exception e) {
                            throw new DebeziumException("Error dispatching schema change for table " + tableId, e);
                        }
                    });
        }
        catch (InterruptedException e) {
            LOGGER.error("Processing interrupted for table '{}'", tableId);
            throw new DebeziumException(e);
        }
    }

    protected String getTableDDL(TableId tableId) throws SQLException {
        // default behavior is to return a null value, this allows connectors that require DDL
        // for a schemaChangeEvent to implement this, such as Oracle
        return null;
    }
}
