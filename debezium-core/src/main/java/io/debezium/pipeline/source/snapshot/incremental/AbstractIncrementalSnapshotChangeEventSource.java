/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot.incremental;

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
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.annotation.NotThreadSafe;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.EventDispatcher;
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
import io.debezium.schema.DataCollectionId;
import io.debezium.schema.DatabaseSchema;
import io.debezium.util.Clock;
import io.debezium.util.ColumnUtils;
import io.debezium.util.Strings;
import io.debezium.util.Threads;
import io.debezium.util.Threads.Timer;

/**
 * An incremental snapshot change event source that emits events from a DB log interleaved with snapshot events.
 */
@NotThreadSafe
public abstract class AbstractIncrementalSnapshotChangeEventSource<T extends DataCollectionId> implements IncrementalSnapshotChangeEventSource<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractIncrementalSnapshotChangeEventSource.class);

    private final RelationalDatabaseConnectorConfig connectorConfig;
    private final Clock clock;
    private final RelationalDatabaseSchema databaseSchema;
    private final SnapshotProgressListener progressListener;
    private final DataChangeEventListener dataListener;
    private long totalRowsScanned = 0;

    private Table currentTable;

    protected EventDispatcher<T> dispatcher;
    protected IncrementalSnapshotContext<T> context = null;
    protected JdbcConnection jdbcConnection;
    protected final Map<Struct, Object[]> window = new LinkedHashMap<>();

    public AbstractIncrementalSnapshotChangeEventSource(RelationalDatabaseConnectorConfig config,
                                                        JdbcConnection jdbcConnection,
                                                        EventDispatcher<T> dispatcher,
                                                        DatabaseSchema<?> databaseSchema,
                                                        Clock clock,
                                                        SnapshotProgressListener progressListener,
                                                        DataChangeEventListener dataChangeEventListener) {
        this.connectorConfig = config;
        this.jdbcConnection = jdbcConnection;
        this.dispatcher = dispatcher;
        this.databaseSchema = (RelationalDatabaseSchema) databaseSchema;
        this.clock = clock;
        this.progressListener = progressListener;
        this.dataListener = dataChangeEventListener;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void closeWindow(Partition partition, String id, OffsetContext offsetContext) throws InterruptedException {
        context = (IncrementalSnapshotContext<T>) offsetContext.getIncrementalSnapshotContext();
        if (!context.closeWindow(id)) {
            return;
        }
        sendWindowEvents(partition, offsetContext);
        readChunk();
    }

    protected String getSignalTableName(String dataCollectionId) {
        if (Strings.isNullOrEmpty(dataCollectionId)) {
            return dataCollectionId;
        }
        return jdbcConnection.quotedTableIdString(TableId.parse(dataCollectionId));
    }

    protected void sendWindowEvents(Partition partition, OffsetContext offsetContext) throws InterruptedException {
        LOGGER.debug("Sending {} events from window buffer", window.size());
        offsetContext.incrementalSnapshotEvents();
        for (Object[] row : window.values()) {
            sendEvent(partition, dispatcher, offsetContext, row);
        }
        offsetContext.postSnapshotCompletion();
        window.clear();
    }

    protected void sendEvent(Partition partition, EventDispatcher<T> dispatcher, OffsetContext offsetContext, Object[] row) throws InterruptedException {
        context.sendEvent(keyFromRow(row));
        offsetContext.event(context.currentDataCollectionId(), clock.currentTimeAsInstant());
        dispatcher.dispatchSnapshotEvent(context.currentDataCollectionId(),
                getChangeRecordEmitter(partition, context.currentDataCollectionId(), offsetContext, row),
                dispatcher.getIncrementalSnapshotChangeEventReceiver(dataListener));
    }

    /**
     * Returns a {@link ChangeRecordEmitter} producing the change records for
     * the given table row.
     */
    protected ChangeRecordEmitter getChangeRecordEmitter(Partition partition, T dataCollectionId,
                                                         OffsetContext offsetContext, Object[] row) {
        return new SnapshotChangeRecordEmitter(partition, offsetContext, row, clock);
    }

    protected void deduplicateWindow(DataCollectionId dataCollectionId, Object key) {
        if (!context.currentDataCollectionId().equals(dataCollectionId)) {
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
    protected abstract void emitWindowClose() throws SQLException, InterruptedException;

    protected String buildChunkQuery(Table table) {
        return buildChunkQuery(table, connectorConfig.getIncrementalSnashotChunkSize());
    }

    protected String buildChunkQuery(Table table, int limit) {
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
        final String orderBy = getKeyMapper().getKeyKolumns(table).stream()
                .map(Column::name)
                .collect(Collectors.joining(", "));
        return jdbcConnection.buildSelectWithRowLimits(table.id(),
                limit,
                "*",
                Optional.ofNullable(condition),
                orderBy);
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
        final List<Column> pkColumns = getKeyMapper().getKeyKolumns(table);
        if (pkColumns.size() > 1) {
            sql.append('(');
        }
        for (int i = 0; i < pkColumns.size(); i++) {
            final boolean isLastIterationForI = (i == pkColumns.size() - 1);
            sql.append('(');
            for (int j = 0; j < i + 1; j++) {
                final boolean isLastIterationForJ = (i == j);
                sql.append(pkColumns.get(j).name());
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

    protected String buildMaxPrimaryKeyQuery(Table table) {
        final String orderBy = getKeyMapper().getKeyKolumns(table).stream()
                .map(Column::name)
                .collect(Collectors.joining(" DESC, ")) + " DESC";
        return jdbcConnection.buildSelectWithRowLimits(table.id(), 1, "*", Optional.empty(), orderBy);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void init(OffsetContext offsetContext) {
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
            progressListener.snapshotStarted();
            readChunk();
        }
        catch (InterruptedException e) {
            throw new DebeziumException("Reading of an initial chunk after connector restart has been interrupted");
        }
        LOGGER.info("Incremental snapshot in progress, loading of initial chunk completed");
    }

    protected void readChunk() throws InterruptedException {
        if (!context.snapshotRunning()) {
            LOGGER.info("Skipping read chunk because snapshot is not running");
            postIncrementalSnapshotCompleted();
            return;
        }
        try {
            preReadChunk(context);
            // This commit should be unnecessary and might be removed later
            jdbcConnection.commit();
            context.startNewChunk();
            emitWindowOpen();
            while (context.snapshotRunning()) {
                if (isTableInvalid()) {
                    continue;
                }
                if (connectorConfig.isIncrementalSnapshotSchemaChangesEnabled() && !schemaHistoryIsUpToDate()) {
                    // Schema has changed since the previous window.
                    // Closing the current window and repeating schema verification within the following window.
                    break;
                }
                final TableId currentTableId = (TableId) context.currentDataCollectionId();
                if (!context.maximumKey().isPresent()) {
                    context.maximumKey(jdbcConnection.queryAndMap(buildMaxPrimaryKeyQuery(currentTable), rs -> {
                        if (!rs.next()) {
                            return null;
                        }
                        return keyFromRow(jdbcConnection.rowToArray(currentTable, databaseSchema, rs,
                                ColumnUtils.toArray(rs, currentTable)));
                    }));
                    if (!context.maximumKey().isPresent()) {
                        LOGGER.info(
                                "No maximum key returned by the query, incremental snapshotting of table '{}' finished as it is empty",
                                currentTableId);
                        nextDataCollection();
                        continue;
                    }
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("Incremental snapshot for table '{}' will end at position {}", currentTableId,
                                context.maximumKey().orElse(new Object[0]));
                    }
                }
                if (createDataEventsForTable()) {
                    if (window.isEmpty()) {
                        LOGGER.info("No data returned by the query, incremental snapshotting of table '{}' finished",
                                currentTableId);
                        tableScanCompleted();
                        nextDataCollection();
                    }
                    else {
                        break;
                    }
                }
                else {
                    context.revertChunk();
                    break;
                }
            }
            emitWindowClose();
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

    private boolean isTableInvalid() {
        final TableId currentTableId = (TableId) context.currentDataCollectionId();
        currentTable = databaseSchema.tableFor(currentTableId);
        if (currentTable == null) {
            LOGGER.warn("Schema not found for table '{}', known tables {}", currentTableId, databaseSchema.tableIds());
            nextDataCollection();
            return true;
        }
        if (getKeyMapper().getKeyKolumns(currentTable).isEmpty()) {
            LOGGER.warn("Incremental snapshot for table '{}' skipped cause the table has no primary keys", currentTableId);
            nextDataCollection();
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
        final String selectStatement = buildChunkQuery(currentTable, 0);
        LOGGER.debug("Reading schema for table '{}' using select statement: '{}'", currentTable.id(), selectStatement);

        try (PreparedStatement statement = readTableChunkStatement(selectStatement);
                ResultSet rs = statement.executeQuery()) {
            return getTable(rs);
        }
        catch (SQLException e) {
            throw new DebeziumException("Snapshotting of table " + currentTable.id() + " failed", e);
        }
    }

    private void nextDataCollection() {
        context.nextDataCollection();
        if (!context.snapshotRunning()) {
            progressListener.snapshotCompleted();
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void addDataCollectionNamesToSnapshot(List<String> dataCollectionIds, OffsetContext offsetContext) throws InterruptedException {
        context = (IncrementalSnapshotContext<T>) offsetContext.getIncrementalSnapshotContext();
        boolean shouldReadChunk = !context.snapshotRunning();
        final List<T> newDataCollectionIds = context.addDataCollectionNamesToSnapshot(dataCollectionIds);
        if (shouldReadChunk) {
            progressListener.snapshotStarted();
            progressListener.monitoredDataCollectionsDetermined(newDataCollectionIds);
            readChunk();
        }
    }

    protected void addKeyColumnsToCondition(Table table, StringBuilder sql, String predicate) {
        for (Iterator<Column> i = getKeyMapper().getKeyKolumns(table).iterator(); i.hasNext();) {
            final Column key = i.next();
            sql.append(key.name()).append(predicate);
            if (i.hasNext()) {
                sql.append(" AND ");
            }
        }
    }

    /**
     * Dispatches the data change events for the records of a single table.
     */
    private boolean createDataEventsForTable() {
        long exportStart = clock.currentTimeInMillis();
        LOGGER.debug("Exporting data chunk from table '{}' (total {} tables)", currentTable.id(), context.tablesToBeSnapshottedCount());

        final String selectStatement = buildChunkQuery(currentTable);
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
                final Object[] row = jdbcConnection.rowToArray(currentTable, databaseSchema, rs, columnArray);
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
                progressListener.currentChunk(context.currentChunkId(), firstKey, lastKey);
            }
            else {
                progressListener.currentChunk(context.currentChunkId(), firstKey, lastKey, context.maximumKey().orElse(null));
            }
            context.nextChunkPosition(lastKey);
            if (lastRow != null) {
                LOGGER.debug("\t Next window will resume from {}", (Object) context.chunkEndPosititon());
            }

            LOGGER.debug("\t Finished exporting {} records for window of table table '{}'; total duration '{}'", rows,
                    currentTable.id(), Strings.duration(clock.currentTimeInMillis() - exportStart));
            incrementTableRowsScanned(rows);
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

    private void incrementTableRowsScanned(long rows) {
        totalRowsScanned += rows;
        progressListener.rowsScanned(currentTable.id(), totalRowsScanned);
    }

    private void tableScanCompleted() {
        progressListener.dataCollectionSnapshotCompleted(currentTable.id(), totalRowsScanned);
        totalRowsScanned = 0;
        // Reset chunk/table information in metrics
        progressListener.currentChunk(null, null, null, null);
    }

    protected PreparedStatement readTableChunkStatement(String sql) throws SQLException {
        final PreparedStatement statement = jdbcConnection.readTablePreparedStatement(connectorConfig, sql,
                OptionalLong.empty());
        if (context.isNonInitialChunk()) {
            final Object[] maximumKey = context.maximumKey().get();
            final Object[] chunkEndPosition = context.chunkEndPosititon();
            // Fill boundaries placeholders
            int pos = 0;
            for (int i = 0; i < chunkEndPosition.length; i++) {
                for (int j = 0; j < i + 1; j++) {
                    statement.setObject(++pos, chunkEndPosition[j]);
                }
            }
            // Fill maximum key placeholders
            for (int i = 0; i < chunkEndPosition.length; i++) {
                for (int j = 0; j < i + 1; j++) {
                    statement.setObject(++pos, maximumKey[j]);
                }
            }
        }
        return statement;
    }

    private Timer getTableScanLogTimer() {
        return Threads.timer(clock, RelationalSnapshotChangeEventSource.LOG_INTERVAL);
    }

    private Object[] keyFromRow(Object[] row) {
        if (row == null) {
            return null;
        }
        final List<Column> keyColumns = getKeyMapper().getKeyKolumns(currentTable);
        final Object[] key = new Object[keyColumns.size()];
        for (int i = 0; i < keyColumns.size(); i++) {
            key[i] = row[keyColumns.get(i).position() - 1];
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

    private KeyMapper getKeyMapper() {
        return connectorConfig.getKeyMapper() == null ? table -> table.primaryKeyColumns() : connectorConfig.getKeyMapper();
    }
}
