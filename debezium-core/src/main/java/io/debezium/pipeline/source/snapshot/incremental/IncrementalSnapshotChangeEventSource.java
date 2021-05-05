/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot.incremental;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
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
import io.debezium.config.CommonConnectorConfig;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.DataChangeEventListener;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.ChangeRecordEmitter;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.Column;
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

@NotThreadSafe
public class IncrementalSnapshotChangeEventSource<T extends DataCollectionId> {

    private static final Logger LOGGER = LoggerFactory.getLogger(IncrementalSnapshotChangeEventSource.class);

    // TODO Metrics
    // Need to discuss and decide if
    // - SnapshotChangeEventSourceMetricsMXBean would be extended with window metrics
    // - new MXBean would be introduced with subset of SnapshotChangeEventSourceMetricsMXBean and additional window metrics
    // - SnapshotChangeEventSourceMetricsMXBean would be reused and new MXBean should be introduce for window metrics

    // List needs to be used as key as it implements hashCode/equals contract
    private Map<Struct, Object[]> window = new LinkedHashMap<>();
    private CommonConnectorConfig connectorConfig;
    private JdbcConnection jdbcConnection;
    private final Clock clock;
    private final String signalWindowStatement;
    private final RelationalDatabaseSchema databaseSchema;
    private final SnapshotProgressListener progressListener;
    private final DataChangeEventListener dataListener;
    private long totalRowsScanned = 0;

    private Table currentTable;

    private IncrementalSnapshotContext<T> context = null;

    public IncrementalSnapshotChangeEventSource(CommonConnectorConfig config, JdbcConnection jdbcConnection,
                                                DatabaseSchema<?> databaseSchema, Clock clock, SnapshotProgressListener progressListener,
                                                DataChangeEventListener dataChangeEventListener) {
        this.connectorConfig = config;
        this.jdbcConnection = jdbcConnection;
        signalWindowStatement = "INSERT INTO " + connectorConfig.getSignalingDataCollectionId()
                + " VALUES (?, ?, null)";
        this.databaseSchema = (RelationalDatabaseSchema) databaseSchema;
        this.clock = clock;
        this.progressListener = progressListener;
        this.dataListener = dataChangeEventListener;
    }

    @SuppressWarnings("unchecked")
    public void closeWindow(String id, EventDispatcher<T> dispatcher, OffsetContext offsetContext) throws InterruptedException {
        context = (IncrementalSnapshotContext<T>) offsetContext.getIncrementalSnapshotContext();
        if (!context.closeWindow(id)) {
            return;
        }
        LOGGER.debug("Sending {} events from window buffer", window.size());
        offsetContext.incrementalSnapshotEvents();
        for (Object[] row : window.values()) {
            sendEvent(dispatcher, offsetContext, row);
        }
        offsetContext.postSnapshotCompletion();
        window.clear();
        readChunk();
    }

    protected void sendEvent(EventDispatcher<T> dispatcher, OffsetContext offsetContext, Object[] row) throws InterruptedException {
        context.sendEvent(keyFromRow(row));
        offsetContext.event((T) context.currentDataCollectionId(), clock.currentTimeAsInstant());
        dispatcher.dispatchSnapshotEvent((T) context.currentDataCollectionId(),
                getChangeRecordEmitter(context.currentDataCollectionId(), offsetContext, row),
                dispatcher.getIncrementalSnapshotChangeEventReceiver(dataListener));
    }

    /**
     * Returns a {@link ChangeRecordEmitter} producing the change records for
     * the given table row.
     */
    protected ChangeRecordEmitter getChangeRecordEmitter(T dataCollectionId, OffsetContext offsetContext,
                                                         Object[] row) {
        return new SnapshotChangeRecordEmitter(offsetContext, row, clock);
    }

    @SuppressWarnings("unchecked")
    public void processMessage(DataCollectionId dataCollectionId, Object key, OffsetContext offsetContext) {
        context = (IncrementalSnapshotContext<T>) offsetContext.getIncrementalSnapshotContext();
        if (context == null) {
            return;
        }
        LOGGER.trace("Checking window for table '{}', key '{}', window contains '{}'", dataCollectionId, key, window);
        if (!context.deduplicationNeeded() || window.isEmpty()) {
            return;
        }
        if (!context.currentDataCollectionId().equals(dataCollectionId)) {
            return;
        }
        if (key instanceof Struct) {
            if (window.remove((Struct) key) != null) {
                LOGGER.info("Removed '{}' from window", key);
            }
        }
    }

    private void emitWindowOpen() throws SQLException {
        jdbcConnection.prepareUpdate(signalWindowStatement, x -> {
            x.setString(1, context.currentChunkId() + "-open");
            x.setString(2, OpenIncrementalSnapshotWindow.NAME);
        });
    }

    private void emitWindowClose() throws SQLException {
        jdbcConnection.prepareUpdate(signalWindowStatement, x -> {
            x.setString(1, context.currentChunkId() + "-close");
            x.setString(2, CloseIncrementalSnapshotWindow.NAME);
        });
    }

    protected String buildChunkQuery(Table table) {
        String condition = null;
        // Add condition when this is not the first query
        if (context.isNonInitialChunk()) {
            final StringBuilder sql = new StringBuilder();
            // Window boundaries
            addKeyColumnsToCondition(table, sql, " >= ?");
            sql.append(" AND NOT (");
            addKeyColumnsToCondition(table, sql, " = ?");
            sql.append(")");
            // Table boundaries
            sql.append(" AND ");
            addKeyColumnsToCondition(table, sql, " <= ?");
            condition = sql.toString();
        }
        final String orderBy = table.primaryKeyColumns().stream()
                .map(Column::name)
                .collect(Collectors.joining(", "));
        return jdbcConnection.buildSelectWithRowLimits(table.id(),
                connectorConfig.getIncrementalSnashotChunkSize(),
                "*",
                Optional.ofNullable(condition),
                orderBy);
    }

    protected String buildMaxPrimaryKeyQuery(Table table) {
        final String orderBy = table.primaryKeyColumns().stream()
                .map(Column::name)
                .collect(Collectors.joining(" DESC, ")) + " DESC";
        return jdbcConnection.buildSelectWithRowLimits(table.id(), 1, "*", Optional.empty(), orderBy.toString());
    }

    @SuppressWarnings("unchecked")
    public void init(OffsetContext offsetContext) {
        if (offsetContext == null) {
            LOGGER.info("Empty incremental snapshot change event source started, no action needed");
            return;
        }
        context = (IncrementalSnapshotContext<T>) offsetContext.getIncrementalSnapshotContext();
        if (!context.snapshotRunning()) {
            LOGGER.info("No incremental snapshot in progress, no action needed on start");
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

    private void readChunk() throws InterruptedException {
        if (!context.snapshotRunning()) {
            return;
        }
        try {
            // This commit should be unnecessary and might be removed later
            jdbcConnection.commit();
            context.startNewChunk();
            emitWindowOpen();
            jdbcConnection.commit();
            while (context.snapshotRunning()) {
                final TableId currentTableId = (TableId) context.currentDataCollectionId();
                currentTable = databaseSchema.tableFor(currentTableId);
                if (currentTable == null) {
                    break;
                }
                if (!context.maximumKey().isPresent()) {
                    context.maximumKey(jdbcConnection.queryAndMap(buildMaxPrimaryKeyQuery(currentTable), rs -> {
                        if (!rs.next()) {
                            return null;
                        }
                        return keyFromRow(rowToArray(currentTable, rs, ColumnUtils.toArray(rs, currentTable)));
                    }));
                    if (!context.maximumKey().isPresent()) {
                        LOGGER.info(
                                "No maximum key returned by the query, incremental snapshotting of table '{}' finished as it is empty",
                                currentTableId);
                        context.nextDataCollection();
                        continue;
                    }
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("Incremental snapshot for table '{}' will end at position {}", currentTableId,
                                context.maximumKey().orElse(new Object[0]));
                    }
                }
                createDataEventsForTable();
                if (window.isEmpty()) {
                    LOGGER.info("No data returned by the query, incremental snapshotting of table '{}' finished",
                            currentTableId);
                    tableScanCompleted();
                    context.nextDataCollection();
                    if (!context.snapshotRunning()) {
                        progressListener.snapshotCompleted();
                    }
                }
                else {
                    break;
                }
            }
            emitWindowClose();
            jdbcConnection.commit();
        }
        catch (SQLException e) {
            throw new DebeziumException("Database error while executing incremental snapshot", e);
        }
    }

    @SuppressWarnings("unchecked")
    public void addDataCollectionNamesToSnapshot(List<String> dataCollectionIds, OffsetContext offsetContext) throws InterruptedException {
        context = (IncrementalSnapshotContext<T>) offsetContext.getIncrementalSnapshotContext();
        boolean shouldReadChunk = false;
        if (!context.snapshotRunning()) {
            shouldReadChunk = true;
        }
        final List<T> newDataCollectionIds = context.addDataCollectionNamesToSnapshot(dataCollectionIds);
        if (shouldReadChunk) {
            progressListener.snapshotStarted();
            progressListener.monitoredDataCollectionsDetermined(newDataCollectionIds);
            readChunk();
        }
    }

    protected void addKeyColumnsToCondition(Table table, StringBuilder sql, String predicate) {
        for (Iterator<Column> i = table.primaryKeyColumns().iterator(); i.hasNext();) {
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
    private void createDataEventsForTable() throws InterruptedException {
        long exportStart = clock.currentTimeInMillis();
        LOGGER.debug("Exporting data chunk from table '{}' (total {} tables)", currentTable.id(), context.tablesToBeSnapshottedCount());

        final String selectStatement = buildChunkQuery(currentTable);
        LOGGER.debug("\t For table '{}' using select statement: '{}', key: '{}', maximum key: '{}'", currentTable.id(),
                selectStatement, context.chunkEndPosititon(), context.maximumKey().get());

        final TableSchema tableSchema = databaseSchema.schemaFor(currentTable.id());

        try (PreparedStatement statement = readTableChunkStatement(selectStatement);
                ResultSet rs = statement.executeQuery()) {

            final ColumnUtils.ColumnArray columnArray = ColumnUtils.toArray(rs, currentTable);
            long rows = 0;
            Timer logTimer = getTableScanLogTimer();

            Object[] lastRow = null;
            Object[] firstRow = null;
            while (rs.next()) {
                rows++;
                final Object[] row = rowToArray(currentTable, rs, columnArray);
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
            context.nextChunkPosition(lastKey);
            progressListener.currentChunk(context.currentChunkId(), firstKey, lastKey);
            if (lastRow != null) {
                LOGGER.debug("\t Next window will resume from '{}'", context.chunkEndPosititon());
            }

            LOGGER.debug("\t Finished exporting {} records for window of table table '{}'; total duration '{}'", rows,
                    currentTable.id(), Strings.duration(clock.currentTimeInMillis() - exportStart));
            incrementTableRowsScanned(rows);
        }
        catch (SQLException e) {
            throw new DebeziumException("Snapshotting of table " + currentTable.id() + " failed", e);
        }
    }

    private void incrementTableRowsScanned(long rows) {
        totalRowsScanned += rows;
        progressListener.rowsScanned(currentTable.id(), totalRowsScanned);
    }

    private void tableScanCompleted() {
        progressListener.dataCollectionSnapshotCompleted(currentTable.id(), totalRowsScanned);
        totalRowsScanned = 0;
    }

    // Extract to JdbcConnection, same as in RelationalSnapshotChangeEventSource
    protected Object[] rowToArray(Table table, ResultSet rs, ColumnUtils.ColumnArray columnArray) throws SQLException {
        final Object[] row = new Object[columnArray.getGreatestColumnPosition()];
        for (int i = 0; i < columnArray.getColumns().length; i++) {
            row[columnArray.getColumns()[i].position() - 1] = jdbcConnection.getColumnValue(rs, i + 1,
                    columnArray.getColumns()[i], table, databaseSchema);
        }
        return row;
    }

    protected PreparedStatement readTableChunkStatement(String sql) throws SQLException {
        final PreparedStatement statement = jdbcConnection.readTablePreparedStatement(connectorConfig, sql,
                OptionalLong.empty());
        if (context.isNonInitialChunk()) {
            final Object[] maximumKey = context.maximumKey().get();
            final Object[] chunkEndPosition = context.chunkEndPosititon();
            for (int i = 0; i < chunkEndPosition.length; i++) {
                statement.setObject(i + 1, chunkEndPosition[i]);
                statement.setObject(i + 1 + chunkEndPosition.length, chunkEndPosition[i]);
                statement.setObject(i + 1 + 2 * chunkEndPosition.length, maximumKey[i]);
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
        final List<Column> keyColumns = currentTable.primaryKeyColumns();
        final Object[] key = new Object[keyColumns.size()];
        for (int i = 0; i < keyColumns.size(); i++) {
            key[i] = row[keyColumns.get(i).position() - 1];
        }
        return key;
    }

    protected void setContext(IncrementalSnapshotContext<T> context) {
        this.context = context;
    }
}
