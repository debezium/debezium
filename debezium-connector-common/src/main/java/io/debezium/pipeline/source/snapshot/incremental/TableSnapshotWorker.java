/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot.incremental;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;
import io.debezium.relational.Column;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.RelationalDatabaseSchema;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchema;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.util.ColumnUtils;

/**
 * Worker that processes a complete incremental snapshot for a single table.
 *
 * <p>Runs in dedicated thread, processes all chunks of a table independently.
 * Uses isolated TableSnapshotContext to avoid conflicts with main thread.
 *
 * @author Ivan Senyk
 * @param <P> partition type
 * @param <T> data collection identifier type
 */
public class TableSnapshotWorker<P extends Partition, T extends DataCollectionId> implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(TableSnapshotWorker.class);

    private final TableSnapshotContext<T> context;
    private final JdbcConnection connection;
    private final RelationalDatabaseConnectorConfig connectorConfig;
    private final RelationalDatabaseSchema databaseSchema;
    private final ChunkQueryBuilder<T> chunkQueryBuilder;
    private final EventDispatcher<P, T> dispatcher;
    private final Map<Struct, Object[]> windowBuffer;
    private final IncrementalSnapshotRetryPolicy retryPolicy;
    private final P partition;
    private final WatermarkCallback watermarkCallback;
    private final OffsetContext offsetContext;
    private final List<SnapshotTableCompletionHandler> completionHandlers;

    private static volatile RecordTransformer RECORD_TRANSFORMER = null;

    public static void registerRecordTransformer(RecordTransformer transformer) {
        LoggerFactory.getLogger(TableSnapshotWorker.class)
                .info("Registered RecordTransformer for snapshot events");
        RECORD_TRANSFORMER = transformer;
    }

    public static void unregisterRecordTransformer() {
        RECORD_TRANSFORMER = null;
    }

    private Table currentTable;
    private long totalRowsRead = 0;
    private volatile boolean cancelled = false;

    // Buffer for snapshot data (used if handlers are registered)
    private final List<SourceRecord> recordBuffer = new ArrayList<>();

    // Flush buffer every N rows to avoid memory accumulation (configurable)
    private final int batchFlushSize;

    /**
     * Callback for watermark operations (connector-specific)
     */
    public interface WatermarkCallback {
        void openWindow();

        void closeWindow();
    }

    public TableSnapshotWorker(
                               TableSnapshotContext<T> context,
                               JdbcConnection connection,
                               RelationalDatabaseConnectorConfig connectorConfig,
                               RelationalDatabaseSchema databaseSchema,
                               ChunkQueryBuilder<T> chunkQueryBuilder,
                               EventDispatcher<P, T> dispatcher,
                               Map<Struct, Object[]> windowBuffer,
                               IncrementalSnapshotRetryPolicy retryPolicy,
                               P partition,
                               WatermarkCallback watermarkCallback,
                               OffsetContext offsetContext) {

        this.context = context;
        this.connection = connection;
        this.connectorConfig = connectorConfig;
        this.databaseSchema = databaseSchema;
        this.chunkQueryBuilder = chunkQueryBuilder;
        this.dispatcher = dispatcher;
        this.windowBuffer = windowBuffer;
        this.retryPolicy = retryPolicy;
        this.partition = partition;
        this.watermarkCallback = watermarkCallback;
        this.offsetContext = offsetContext;

        // Read configurable batch flush size (default 20K, was hardcoded 100K)
        this.batchFlushSize = connectorConfig.getIncrementalSnapshotBatchFlushSize();
        LOGGER.info("[{}] Batch flush size: {} rows", Thread.currentThread().getName(), batchFlushSize);

        // Load SPI handlers for table completion notifications
        this.completionHandlers = new ArrayList<>();
        ServiceLoader<SnapshotTableCompletionHandler> loader = ServiceLoader.load(SnapshotTableCompletionHandler.class);
        loader.forEach(handler -> {
            completionHandlers.add(handler);
            LOGGER.info("[{}] Registered SnapshotTableCompletionHandler: {}",
                    Thread.currentThread().getName(), handler.getClass().getName());
        });
    }

    @Override
    public void run() {
        final TableId tableId = (TableId) context.currentDataCollectionId().getId();

        try {
            LOGGER.info("[{}] Starting table snapshot for '{}'",
                    Thread.currentThread().getName(), tableId);

            // Get table schema
            currentTable = refreshTableSchema(tableId);
            if (currentTable == null) {
                LOGGER.warn("[{}] Table '{}' schema not found, skipping",
                        Thread.currentThread().getName(), tableId);
                context.markCompleted();
                return;
            }

            // Prepare table for chunk query builder
            currentTable = chunkQueryBuilder.prepareTable(context, currentTable);

            final List<Column> keyColumns = chunkQueryBuilder.getQueryColumns(context, currentTable);
            if (keyColumns == null || keyColumns.isEmpty()) {
                LOGGER.info("[{}] Table '{}' has no key columns, reading as single chunk",
                        Thread.currentThread().getName(), tableId);
                readKeylessTable();
                context.markCompleted();
                return;
            }

            // Get maximum key
            Object[] maximumKey = getMaximumKey();
            if (maximumKey == null) {
                LOGGER.info("[{}] Table '{}' is empty, skipping",
                        Thread.currentThread().getName(), tableId);
                context.markCompleted();
                return;
            }

            context.maximumKey(maximumKey);
            LOGGER.info("[{}] Table '{}' has data, starting chunk reads",
                    Thread.currentThread().getName(), tableId);

            // Process all chunks
            int chunkCount = 0;
            while (!cancelled && !isComplete()) {
                boolean hasData = readNextChunk();
                chunkCount++;

                if (!hasData) {
                    break;
                }

                if (chunkCount % 10 == 0) {
                    LOGGER.info("[{}] Progress: {} chunks completed for table '{}', {} total rows",
                            Thread.currentThread().getName(), chunkCount, tableId, totalRowsRead);
                }
            }

            context.markCompleted();
            LOGGER.info("[{}] Completed table snapshot for '{}': {} chunks, {} total rows",
                    Thread.currentThread().getName(), tableId, chunkCount, totalRowsRead);

            // Flush any remaining buffered data
            if (!completionHandlers.isEmpty() && !recordBuffer.isEmpty()) {
                LOGGER.info("[{}] Flushing final batch for table '{}' ({} records)",
                        Thread.currentThread().getName(),
                        tableId,
                        recordBuffer.size());
                flushRecordBuffer();
            }

            // Signal to handlers that this table is fully done (no more chunks)
            if (!completionHandlers.isEmpty()) {
                final String tableIdStr = tableId.toString();
                for (SnapshotTableCompletionHandler handler : completionHandlers) {
                    if (handler.shouldHandle(tableIdStr)) {
                        try {
                            handler.onTableSnapshotFinished(tableIdStr);
                        }
                        catch (Exception ex) {
                            LOGGER.error("[{}] Handler {} failed onTableSnapshotFinished for '{}': {}",
                                    Thread.currentThread().getName(),
                                    handler.getClass().getSimpleName(),
                                    tableId,
                                    ex.getMessage(),
                                    ex);
                        }
                    }
                }
                LOGGER.info("[{}] Notified {} handlers of table '{}' completion",
                        Thread.currentThread().getName(), completionHandlers.size(), tableId);
            }
        }
        catch (Exception e) {
            LOGGER.error("[{}] Error processing table '{}': {}",
                    Thread.currentThread().getName(), tableId, e.getMessage(), e);
            throw new DebeziumException("Failed to snapshot table " + tableId, e);
        }
    }

    private void flushRecordBuffer() {
        if (recordBuffer.isEmpty()) {
            return;
        }

        final TableId tableId = (TableId) context.currentDataCollectionId().getId();

        LOGGER.debug("[{}] Flushing record buffer for '{}' ({} records)",
                Thread.currentThread().getName(), tableId, recordBuffer.size());

        List<SourceRecord> batchCopy = new ArrayList<>(recordBuffer);

        for (SnapshotTableCompletionHandler handler : completionHandlers) {
            if (handler.shouldHandle(tableId.toString())) {
                try {
                    handler.onTableSnapshotCompleted(tableId.toString(), batchCopy);
                }
                catch (Exception ex) {
                    LOGGER.error("[{}] Handler {} failed for partial batch of '{}': {}",
                            Thread.currentThread().getName(),
                            handler.getClass().getSimpleName(),
                            tableId,
                            ex.getMessage(),
                            ex);
                }
            }
        }

        recordBuffer.clear();
        LOGGER.debug("[{}] Cleared record buffer for '{}'",
                Thread.currentThread().getName(), tableId);
    }

    private Table refreshTableSchema(TableId tableId) {
        LOGGER.debug("[{}] Refreshing table '{}' schema",
                Thread.currentThread().getName(), tableId);
        return databaseSchema.tableFor(tableId);
    }

    private Object[] getMaximumKey() throws SQLException {
        final TableId tableId = (TableId) context.currentDataCollectionId().getId();

        try {
            return retryPolicy.executeWithRetry(
                    () -> connection.queryAndMap(
                            chunkQueryBuilder.buildMaxPrimaryKeyQuery(context, currentTable,
                                    context.currentDataCollectionId().getAdditionalCondition()),
                            rs -> {
                                if (!rs.next()) {
                                    return null;
                                }
                                return keyFromRow(connection.rowToArray(currentTable, rs,
                                        ColumnUtils.toArray(rs, currentTable)));
                            }),
                    "read maximum key for table " + tableId);
        }
        catch (Exception e) {
            LOGGER.error("[{}] Failed to read maximum key for table {}",
                    Thread.currentThread().getName(), tableId, e);
            return null;
        }
    }

    private void readKeylessTable() throws SQLException {
        final TableId tableId = (TableId) context.currentDataCollectionId().getId();
        final TableSchema tableSchema = databaseSchema.schemaFor(tableId);

        final StringBuilder sql = new StringBuilder("SELECT * FROM ")
                .append(connection.quotedTableIdString(tableId));

        context.currentDataCollectionId().getAdditionalCondition()
                .ifPresent(cond -> sql.append(" WHERE ").append(cond));

        LOGGER.info("[{}] Keyless table '{}' query: {}", Thread.currentThread().getName(), tableId, sql);

        try (PreparedStatement statement = connection.connection().prepareStatement(sql.toString());
             ResultSet rs = statement.executeQuery()) {

            final ColumnUtils.ColumnArray columnArray = ColumnUtils.toArray(rs, currentTable);
            while (rs.next()) {
                final Object[] row = connection.rowToArray(currentTable, rs, columnArray);

                if (!completionHandlers.isEmpty()) {
                    SourceRecord record = SnapshotRecordBuilder.buildFlatRecord(
                            row, currentTable.id(), tableSchema, offsetContext);
                    if (record != null) {
                        RecordTransformer transformer = RECORD_TRANSFORMER;
                        if (transformer != null) {
                            record = transformer.transform(record);
                        }
                        if (record != null) {
                            recordBuffer.add(record);
                        }
                    }
                    if (recordBuffer.size() >= batchFlushSize) {
                        flushRecordBuffer();
                    }
                }
                else {
                    final Struct keyStruct = tableSchema.keyFromColumnData(row);
                    windowBuffer.put(keyStruct, row);
                }
                totalRowsRead++;
            }
        }

        if (!recordBuffer.isEmpty()) {
            flushRecordBuffer();
        }

        LOGGER.info("[{}] Keyless table '{}' read {} rows", Thread.currentThread().getName(), tableId, totalRowsRead);
    }

    private boolean readNextChunk() throws SQLException {
        final TableId tableId = (TableId) context.currentDataCollectionId().getId();

        // Open watermark window
        watermarkCallback.openWindow();

        try {
            connection.commit();
            context.startNewChunk();

            // Build and execute chunk query
            final String selectStatement = chunkQueryBuilder.buildChunkQuery(
                    context, currentTable, context.currentDataCollectionId().getAdditionalCondition());

            LOGGER.trace("[{}] Chunk query for table '{}': {}",
                    Thread.currentThread().getName(), tableId, selectStatement);

            return retryPolicy.executeWithRetry(
                    () -> executeChunkQuery(selectStatement),
                    "read chunk for table " + tableId);
        }
        finally {
            // Close watermark window
            watermarkCallback.closeWindow();
        }
    }

    private boolean executeChunkQuery(String selectStatement) throws SQLException {
        final TableSchema tableSchema = databaseSchema.schemaFor(currentTable.id());
        boolean hasData = false;
        Object[] lastKey = null;

        try (PreparedStatement statement = chunkQueryBuilder.readTableChunkStatement(
                context, currentTable, selectStatement)) {

            try (ResultSet rs = statement.executeQuery()) {
                final ColumnUtils.ColumnArray columnArray = ColumnUtils.toArray(rs, currentTable);

                while (rs.next()) {
                    hasData = true;
                    final Object[] row = connection.rowToArray(currentTable, rs, columnArray);
                    final Object[] key = keyFromRow(row);

                    lastKey = key;

                    // If completion handlers registered, accumulate in local buffer for flush
                    // Otherwise use shared window buffer (original behavior)
                    if (!completionHandlers.isEmpty()) {
                        SourceRecord record = SnapshotRecordBuilder.buildFlatRecord(
                                row, currentTable.id(), tableSchema, offsetContext);
                        if (record != null) {
                            RecordTransformer transformer = RECORD_TRANSFORMER;
                            if (transformer != null) {
                                record = transformer.transform(record);
                            }
                            if (record != null) {
                                recordBuffer.add(record);
                            }
                        }

                        if (recordBuffer.size() >= batchFlushSize) {
                            flushRecordBuffer();
                        }
                    }
                    else {
                        // Add to window buffer (thread-safe) - original behavior
                        final Struct keyStruct = tableSchema.keyFromColumnData(row);
                        windowBuffer.put(keyStruct, row);
                    }

                    totalRowsRead++;
                }
            }
        }

        if (lastKey != null) {
            context.nextChunkPosition(lastKey);
        }

        return hasData;
    }

    private Object[] keyFromRow(Object[] row) {
        final int numKeys = currentTable.primaryKeyColumnNames().size();
        final Object[] key = new Object[numKeys];

        for (int i = 0; i < numKeys; i++) {
            key[i] = row[i];
        }

        return key;
    }

    private boolean isComplete() {
        Object[] currentPos = context.chunkEndPosititon();
        if (currentPos == null || !context.maximumKey().isPresent()) {
            return false;
        }

        Object[] maxKey = context.maximumKey().get();

        // Check if current position >= maximum key
        for (int i = 0; i < currentPos.length; i++) {
            @SuppressWarnings("unchecked")
            final Comparable<Object> curr = (Comparable<Object>) currentPos[i];
            final Object max = maxKey[i];

            final int comparison = curr.compareTo(max);
            if (comparison < 0) {
                return false;
            }
            if (comparison > 0) {
                return true;
            }
        }

        return true;
    }

    public void cancel() {
        cancelled = true;
        LOGGER.info("[{}] Cancelling snapshot for table '{}'",
                Thread.currentThread().getName(), context.currentDataCollectionId().getId());
    }

    public TableSnapshotContext<T> getContext() {
        return context;
    }

    public long getTotalRowsRead() {
        return totalRowsRead;
    }
}
