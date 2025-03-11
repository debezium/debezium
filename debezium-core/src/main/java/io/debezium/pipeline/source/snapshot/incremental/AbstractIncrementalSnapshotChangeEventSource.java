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
import io.debezium.util.Loggings;
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
    }

    @Override
    @SuppressWarnings("unchecked")
    public void closeWindow(P partition, String id, OffsetContext offsetContext) throws InterruptedException {
        context = (IncrementalSnapshotContext<T>) offsetContext.getIncrementalSnapshotContext();
        Loggings.logTraceAndTraceRecord(LOGGER, context.toString(), "Closing Window");
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
        if (!context.snapshotRunning() || !context.deduplicationNeeded() || window.isEmpty()) {
            return;
        }
        window.clear();
        context.revertChunk();
        readChunk(partition, offsetContext);
    }

    protected String getSignalTableName(String dataCollectionId) {
        if (Strings.isNullOrEmpty(dataCollectionId)) {
            return dataCollectionId;
        }
        return jdbcConnection.quotedTableIdString(TableId.parse(dataCollectionId));
    }

    protected void sendWindowEvents(P partition, OffsetContext offsetContext) throws InterruptedException {
        LOGGER.debug("Sending {} events from window buffer", window.size());
        offsetContext.incrementalSnapshotEvents();
        for (Object[] row : window.values()) {
            sendEvent(partition, dispatcher, offsetContext, row);
        }
        offsetContext.postSnapshotCompletion();
        window.clear();
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

    protected void deduplicateWindow(DataCollectionId dataCollectionId, Object key) {
        if (context.currentDataCollectionId() == null || !context.currentDataCollectionId().getId().equals(dataCollectionId)) {
            return;
        }
        if (key instanceof Struct) {
            if (window.remove((Struct) key) != null) {
                Loggings.logInfoAndTraceRecord(LOGGER, key, "Removed key from window");
            }
        }
    }

    /**
     * Update low watermark for the incremental snapshot chunk
     */
    protected abstract void emitWindowOpen() throws SQLException;

    /**
     * Update high watermark for the incremental snapshot chunk
     */
    protected abstract void emitWindowClose(P partition, OffsetContext offsetContext) throws Exception;

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
        LOGGER.info("Incremental snapshot in progress, need to read new chunk on start");
        try {
            preIncrementalSnapshotStart();
            progressListener.snapshotStarted(partition);
            readChunk(partition, offsetContext);
        }
        catch (InterruptedException e) {
            throw new DebeziumException("Reading of an initial chunk after connector restart has been interrupted");
        }
        LOGGER.info("Incremental snapshot in progress, loading of initial chunk completed");
    }

    protected void readChunk(P partition, OffsetContext offsetContext) throws InterruptedException {

        LOGGER.trace("Reading chunk");
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
        try {
            preReadChunk(context);
            // This commit should be unnecessary and might be removed later
            jdbcConnection.commit();
            context.startNewChunk();
            emitWindowOpen();
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
                    currentTable = refreshTableSchema(currentTable);
                    Object[] maximumKey;
                    try {
                        maximumKey = jdbcConnection.queryAndMap(
                                chunkQueryBuilder.buildMaxPrimaryKeyQuery(context, currentTable, context.currentDataCollectionId().getAdditionalCondition()), rs -> {
                                    if (!rs.next()) {
                                        return null;
                                    }
                                    return keyFromRow(jdbcConnection.rowToArray(currentTable, rs,
                                            ColumnUtils.toArray(rs, currentTable)));
                                });
                        context.maximumKey(maximumKey);
                    }
                    catch (SQLException e) {
                        LOGGER.error("Failed to read maximum key for table {}", currentTableId, e);
                        notificationService.incrementalSnapshotNotificationService().notifyTableScanCompleted(context, partition, offsetContext, totalRowsScanned,
                                SQL_EXCEPTION);
                        nextDataCollection(partition, offsetContext);
                        continue;
                    }
                    if (context.maximumKey().isEmpty()) {
                        LOGGER.info(
                                "No maximum key returned by the query, incremental snapshotting of table '{}' finished as it is empty",
                                currentTableId);
                        notificationService.incrementalSnapshotNotificationService().notifyTableScanCompleted(context, partition, offsetContext, totalRowsScanned, EMPTY);
                        nextDataCollection(partition, offsetContext);
                        continue;
                    }
                    if (LOGGER.isInfoEnabled()) {
                        Loggings.logInfoAndTraceRecord(LOGGER, context.maximumKey().orElse(new Object[0]),
                                "Incremental snapshot for table '{}' will end at position", currentTableId);
                    }
                }

                try {
                    if (createDataEventsForTable(partition)) {

                        if (!context.snapshotRunning()) { // A stop signal has been processed and window cleared.
                            return;
                        }

                        if (window.isEmpty()) {
                            LOGGER.info("No data returned by the query, incremental snapshotting of table '{}' finished",
                                    currentTableId);

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
        catch (Exception e) {
            throw new DebeziumException(String.format("Database error while executing incremental snapshot for table '%s'", context.currentDataCollectionId()), e);
        }
        finally {
            postReadChunk(context);
            if (!context.snapshotRunning()) {
                postIncrementalSnapshotCompleted();
            }
        }
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
        if (shouldReadChunk) {

            List<T> monitoredDataCollections = newDataCollectionIds.stream()
                    .map(DataCollection::getId).collect(Collectors.toList());
            LOGGER.trace("Monitored data collections {}", newDataCollectionIds);

            progressListener.snapshotStarted(partition);

            notificationService.incrementalSnapshotNotificationService().notifyStarted(context, partition, offsetContext);

            progressListener.monitoredDataCollectionsDetermined(partition, monitoredDataCollections);
            readChunk(partition, offsetContext);
        }
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
        Loggings.logTraceAndTraceRecord(LOGGER, context, "Stopping incremental snapshot");
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
    private boolean createDataEventsForTable(P partition) throws SQLException {
        long exportStart = clock.currentTimeInMillis();
        LOGGER.debug("Exporting data chunk from table '{}' (total {} tables)", currentTable.id(), context.dataCollectionsToBeSnapshottedCount());

        final String selectStatement = chunkQueryBuilder.buildChunkQuery(context, currentTable, context.currentDataCollectionId().getAdditionalCondition());
        Loggings.logDebugAndTraceRecord(LOGGER, context.maximumKey().get(), "\t For table '{}' using select statement: '{}', key: '{}'",
                currentTable.id(), selectStatement, context.chunkEndPosititon());

        final TableSchema tableSchema = databaseSchema.schemaFor(currentTable.id());

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
                window.put(keyStruct, row);
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

        LOGGER.trace("Pre read chunk");
        try {
            if (!jdbcConnection.isValid()) {
                jdbcConnection.connect();
            }
        }
        catch (SQLException e) {
            throw new DebeziumException("Database error while checking jdbcConnection in preReadChunk", e);
        }
    }

    protected void postReadChunk(IncrementalSnapshotContext<T> context) {
        // no-op
    }

    protected void postIncrementalSnapshotCompleted() {
        // no-op
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
