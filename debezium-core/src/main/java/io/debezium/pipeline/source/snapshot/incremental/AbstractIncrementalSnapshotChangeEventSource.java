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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
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
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.signal.SignalPayload;
import io.debezium.pipeline.signal.actions.snapshotting.SnapshotConfiguration;
import io.debezium.pipeline.source.spi.DataChangeEventListener;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.ChangeRecordEmitter;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;
import io.debezium.relational.Column;
import io.debezium.relational.Key.KeyMapper;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.RelationalDatabaseSchema;
import io.debezium.relational.RelationalSnapshotChangeEventSource;
import io.debezium.relational.SnapshotChangeRecordEmitter;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchema;
import io.debezium.relational.Tables.ColumnNameFilter;
import io.debezium.schema.DatabaseSchema;
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
    protected ColumnNameFilter columnFilter;
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
        this.columnFilter = config.getColumnFilter();
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
        LOGGER.trace("Closing Window {}", context.toString());
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
                LOGGER.info("Removed '{}' from window", key);
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
    protected abstract void emitWindowClose(P partition, OffsetContext offsetContext) throws SQLException, InterruptedException;

    protected String buildChunkQuery(Table table, Optional<String> additionalCondition) {
        return buildChunkQuery(table, connectorConfig.getIncrementalSnapshotChunkSize(), additionalCondition);
    }

    protected String buildChunkQuery(Table table, int limit, Optional<String> additionalCondition) {
        String condition = null;
        // Add condition when this is not the first query
        if (context.isNonInitialChunk()) {
            final StringBuilder sql = new StringBuilder();
            // Window boundaries
            addLowerBound(table, sql);
            // Table boundaries
            sql.append(" AND NOT ");
            addLowerBound(table, sql);
            condition = sql.toString();
        }
        final String orderBy = getQueryColumns(table).stream()
                .map(c -> jdbcConnection.quotedColumnIdString(c.name()))
                .collect(Collectors.joining(", "));
        return jdbcConnection.buildSelectWithRowLimits(table.id(),
                limit,
                buildProjection(table),
                Optional.ofNullable(condition),
                additionalCondition,
                orderBy);
    }

    protected String buildProjection(Table table) {
        String projection = "*";
        if (connectorConfig.isColumnsFiltered()) {
            TableId tableId = table.id();
            projection = table.columns().stream()
                    .filter(column -> columnFilter.matches(tableId.catalog(), tableId.schema(), tableId.table(), column.name()))
                    .map(column -> jdbcConnection.quotedColumnIdString(column.name()))
                    .collect(Collectors.joining(", "));
        }
        return projection;
    }

    private void addLowerBound(Table table, StringBuilder sql) {
        // To make window boundaries working for more than one column it is necessary to calculate
        // with independently increasing values in each column independently.
        // For one column the condition will be (? will always be the last value seen for the given column)
        // (k1 > ?)
        // For two columns
        // (k1 > ?) OR (k1 = ? AND k2 > ?)
        // For four columns
        // (k1 > ?) OR (k1 = ? AND k2 > ?) OR (k1 = ? AND k2 = ? AND k3 > ?) OR (k1 = ? AND k2 = ? AND k3 = ? AND k4 > ?)
        // etc.
        final List<Column> pkColumns = getQueryColumns(table);
        if (pkColumns.size() > 1) {
            sql.append('(');
        }
        for (int i = 0; i < pkColumns.size(); i++) {
            final boolean isLastIterationForI = (i == pkColumns.size() - 1);
            sql.append('(');
            for (int j = 0; j < i + 1; j++) {
                final boolean isLastIterationForJ = (i == j);
                sql.append(jdbcConnection.quotedColumnIdString(pkColumns.get(j).name()));
                sql.append(isLastIterationForJ ? " > ?" : " = ?");
                if (!isLastIterationForJ) {
                    sql.append(" AND ");
                }
            }
            sql.append(")");
            if (!isLastIterationForI) {
                sql.append(" OR ");
            }
        }
        if (pkColumns.size() > 1) {
            sql.append(')');
        }
    }

    protected String buildMaxPrimaryKeyQuery(Table table, Optional<String> additionalCondition) {
        final String orderBy = getQueryColumns(table).stream()
                .map(c -> jdbcConnection.quotedColumnIdString(c.name()))
                .collect(Collectors.joining(" DESC, ")) + " DESC";
        return jdbcConnection.buildSelectWithRowLimits(table.id(), 1, buildProjection(table), Optional.empty(),
                additionalCondition, orderBy);
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
        LOGGER.info("Incremental snapshot in progress, need to read new chunk on start");
        try {
            progressListener.snapshotStarted(partition);
            readChunk(partition, offsetContext);
        }
        catch (InterruptedException e) {
            throw new DebeziumException("Reading of an initial chunk after connector restart has been interrupted");
        }
        LOGGER.info("Incremental snapshot in progress, loading of initial chunk completed");
    }

    protected void readChunk(P partition, OffsetContext offsetContext) throws InterruptedException {
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
            while (context.snapshotRunning()) {
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
                                buildMaxPrimaryKeyQuery(currentTable, context.currentDataCollectionId().getAdditionalCondition()), rs -> {
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
                        LOGGER.info("Incremental snapshot for table '{}' will end at position {}", currentTableId,
                                context.maximumKey().orElse(new Object[0]));
                    }
                }
                if (createDataEventsForTable(partition)) {

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
            emitWindowClose(partition, offsetContext);
        }
        catch (SQLException e) {
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
            LOGGER.warn("Schema not found for table '{}', known tables {}", currentTableId, databaseSchema.tableIds());
            notificationService.incrementalSnapshotNotificationService().notifyTableScanCompleted(context, partition, offsetContext, totalRowsScanned, UNKNOWN_SCHEMA);
            nextDataCollection(partition, offsetContext);
            return true;
        }
        if (getQueryColumns(currentTable).isEmpty()) {
            LOGGER.warn("Incremental snapshot for table '{}' skipped cause the table has no primary keys", currentTableId);
            notificationService.incrementalSnapshotNotificationService().notifyTableScanCompleted(context, partition, offsetContext, totalRowsScanned, NO_PRIMARY_KEY);
            nextDataCollection(partition, offsetContext);
            return true;
        }
        return false;
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
        final String selectStatement = buildChunkQuery(currentTable, 0, Optional.empty());
        LOGGER.debug("Reading schema for table '{}' using select statement: '{}'", currentTable.id(), selectStatement);

        try (PreparedStatement statement = readTableChunkStatement(selectStatement);
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

        List<String> expandedDataCollectionIds = expandDataCollectionIds(snapshotConfiguration.getDataCollections());
        if (expandedDataCollectionIds.size() > snapshotConfiguration.getDataCollections().size()) {
            LOGGER.info("Data-collections to snapshot have been expanded from {} to {}", snapshotConfiguration.getDataCollections(), expandedDataCollectionIds);
        }

        final List<DataCollection<T>> newDataCollectionIds = context.addDataCollectionNamesToSnapshot(correlationId, expandedDataCollectionIds,
                snapshotConfiguration.getAdditionalConditions(),
                snapshotConfiguration.getSurrogateKey());
        if (shouldReadChunk) {

            List<T> monitoredDataCollections = newDataCollectionIds.stream()
                    .map(DataCollection::getId).collect(Collectors.toList());
            progressListener.snapshotStarted(partition);

            notificationService.incrementalSnapshotNotificationService().notifyStarted(context, partition, offsetContext);

            progressListener.monitoredDataCollectionsDetermined(partition, monitoredDataCollections);
            readChunk(partition, offsetContext);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void stopSnapshot(P partition, OffsetContext offsetContext, Map<String, Object> additionalData, List<String> dataCollectionIds) {

        context = (IncrementalSnapshotContext<T>) offsetContext.getIncrementalSnapshotContext();
        LOGGER.trace("Stopping incremental snapshot with context {}", context);
        if (context.snapshotRunning()) {
            if (dataCollectionIds == null || dataCollectionIds.isEmpty()) {
                LOGGER.info("Stopping incremental snapshot.");
                try {
                    // This must be called prior to closeWindow to ensure the correct state is set
                    // to prevent chunk reads from triggering additional open/close events.
                    context.stopSnapshot();

                    // Clear the state
                    window.clear();
                    closeWindow(partition, context.currentChunkId(), offsetContext);

                    progressListener.snapshotAborted(partition);

                    notificationService.incrementalSnapshotNotificationService().notifyAborted(context, partition, offsetContext);
                }
                catch (InterruptedException e) {
                    LOGGER.warn("Failed to stop snapshot successfully.", e);
                }
            }
            else {
                final List<String> expandedDataCollectionIds = expandDataCollectionIds(dataCollectionIds);
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
                            LOGGER.info("Removed '{}' from incremental snapshot collection list.", collectionId);
                        }
                        else {
                            LOGGER.warn("Could not remove '{}', collection is not part of the incremental snapshot.", collectionId);
                        }
                    }
                }
                // If current is requested to stop, proceed with stopping it.
                if (stopCurrentTableId != null) {
                    window.clear();
                    LOGGER.info("Removed '{}' from incremental snapshot collection list.", stopCurrentTableId);
                    tableScanCompleted(partition);
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

                notificationService.incrementalSnapshotNotificationService().notifyAborted(context, partition, offsetContext, expandedDataCollectionIds);
            }
        }
        else {
            LOGGER.warn("No active incremental snapshot, stop ignored");
        }
    }

    protected void addKeyColumnsToCondition(Table table, StringBuilder sql, String predicate) {
        for (Iterator<Column> i = getQueryColumns(table).iterator(); i.hasNext();) {
            final Column key = i.next();
            sql.append(jdbcConnection.quotedColumnIdString(key.name())).append(predicate);
            if (i.hasNext()) {
                sql.append(" AND ");
            }
        }
    }

    /**
     * Expands the string-based list of data collection ids if supplied using regex to a list of
     * all matching explicit data collection ids.
     */
    private List<String> expandDataCollectionIds(List<String> dataCollectionIds) {

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
                })
                .collect(Collectors.toList());
    }

    /**
     * Dispatches the data change events for the records of a single table.
     */
    private boolean createDataEventsForTable(P partition) {
        long exportStart = clock.currentTimeInMillis();
        LOGGER.debug("Exporting data chunk from table '{}' (total {} tables)", currentTable.id(), context.dataCollectionsToBeSnapshottedCount());

        final String selectStatement = buildChunkQuery(currentTable, context.currentDataCollectionId().getAdditionalCondition());
        LOGGER.debug("\t For table '{}' using select statement: '{}', key: '{}', maximum key: '{}'", currentTable.id(),
                selectStatement, context.chunkEndPosititon(), context.maximumKey().get());

        final TableSchema tableSchema = databaseSchema.schemaFor(currentTable.id());

        try (PreparedStatement statement = readTableChunkStatement(selectStatement);
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
            throw new DebeziumException("Snapshotting of table " + currentTable.id() + " failed", e);
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

    protected PreparedStatement readTableChunkStatement(String sql) throws SQLException {
        final PreparedStatement statement = jdbcConnection.readTablePreparedStatement(connectorConfig, sql,
                OptionalLong.empty());
        if (context.isNonInitialChunk()) {
            final Object[] maximumKey = context.maximumKey().get();
            final Object[] chunkEndPosition = context.chunkEndPosititon();
            // Fill boundaries placeholders
            int pos = 0;
            final List<Column> queryColumns = getQueryColumns(currentTable);
            for (int i = 0; i < chunkEndPosition.length; i++) {
                for (int j = 0; j < i + 1; j++) {
                    jdbcConnection.setQueryColumnValue(statement, queryColumns.get(j), ++pos, chunkEndPosition[j]);
                }
            }
            // Fill maximum key placeholders
            for (int i = 0; i < chunkEndPosition.length; i++) {
                for (int j = 0; j < i + 1; j++) {
                    jdbcConnection.setQueryColumnValue(statement, queryColumns.get(j), ++pos, maximumKey[j]);
                }
            }
        }
        return statement;
    }

    private Timer getTableScanLogTimer() {
        return Threads.timer(clock, RelationalSnapshotChangeEventSource.LOG_INTERVAL);
    }

    @SuppressWarnings("unchecked")
    private Object[] keyFromRow(Object[] row) {
        if (row == null) {
            return null;
        }
        final List<Column> keyColumns = getQueryColumns(currentTable);
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

    protected void preReadChunk(IncrementalSnapshotContext<T> context) {
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

    private KeyMapper getKeyMapper() {
        return connectorConfig.getKeyMapper() == null ? table -> table.primaryKeyColumns() : connectorConfig.getKeyMapper();
    }

    private List<Column> getQueryColumns(Table table) {
        if (context != null && context.currentDataCollectionId() != null) {
            Optional<String> surrogateKey = context.currentDataCollectionId().getSurrogateKey();
            if (surrogateKey.isPresent()) {
                return Collections.singletonList(table.columnWithName(surrogateKey.get()));
            }
        }
        return getKeyMapper().getKeyKolumns(table);
    }
}
