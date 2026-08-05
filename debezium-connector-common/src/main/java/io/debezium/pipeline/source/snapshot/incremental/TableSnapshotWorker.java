/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot.incremental;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.spi.Partition;
import io.debezium.relational.Column;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.RelationalDatabaseSchema;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchema;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.util.ColumnUtils;
import io.debezium.util.RetryExecutor;

/**
 * Per-table state holder and chunk reader for parallel incremental snapshots.
 *
 * <p>Each worker owns one {@link TableSnapshotContext} (chunk position, maximum
 * key, schema reference) and writes into one per-table window buffer. The
 * {@code readOneChunk} method advances the table by exactly one chunk per
 * invocation, mirroring the upstream sequential pattern: each call corresponds
 * to one DBLog watermark window. The {@link
 * io.debezium.pipeline.EventDispatcher} drains the buffer at close-window and
 * re-invokes {@code readChunk} on the source for the next round.
 *
 * @author Ivan Senyk
 * @param <P> partition type
 * @param <T> data collection identifier type
 */
public class TableSnapshotWorker<P extends Partition, T extends DataCollectionId> {

    private static final Logger LOGGER = LoggerFactory.getLogger(TableSnapshotWorker.class);

    private final TableSnapshotContext<T> context;
    private final RelationalDatabaseConnectorConfig connectorConfig;
    private final RelationalDatabaseSchema databaseSchema;
    private final ChunkQueryBuilder<T> chunkQueryBuilder;
    private final Map<Struct, Object[]> windowBuffer;
    private final RetryExecutor retryPolicy;

    private Table currentTable;
    private long totalRowsRead = 0;

    public TableSnapshotWorker(
                               TableSnapshotContext<T> context,
                               RelationalDatabaseConnectorConfig connectorConfig,
                               RelationalDatabaseSchema databaseSchema,
                               ChunkQueryBuilder<T> chunkQueryBuilder,
                               Map<Struct, Object[]> windowBuffer,
                               RetryExecutor retryPolicy) {
        this.context = context;
        this.connectorConfig = connectorConfig;
        this.databaseSchema = databaseSchema;
        this.chunkQueryBuilder = chunkQueryBuilder;
        this.windowBuffer = windowBuffer;
        this.retryPolicy = retryPolicy;
    }

    /**
     * Reads the next chunk of this worker's table using the supplied connection
     * and returns whether the table still has more data after this chunk.
     */
    public boolean readOneChunk(JdbcConnection conn) throws SQLException {
        final TableId tableId = (TableId) context.currentDataCollectionId().getId();

        if (currentTable == null) {
            currentTable = databaseSchema.tableFor(tableId);
            if (currentTable == null) {
                LOGGER.warn("Table '{}' schema not found, skipping", tableId);
                return false;
            }
            currentTable = chunkQueryBuilder.prepareTable(context, currentTable);
        }

        final List<Column> keyColumns = chunkQueryBuilder.getQueryColumns(context, currentTable);
        if (keyColumns == null || keyColumns.isEmpty()) {
            throw new DebeziumException(
                    "Table '" + tableId + "' has no primary key; incremental snapshot requires a key "
                            + "for the DBLog watermark window. Restrict this table to initial snapshot, "
                            + "add a PK, or declare a synthetic key via 'message.key.columns'.");
        }

        if (context.maximumKey().isEmpty()) {
            final Object[] maximumKey = readMaximumKey(conn);
            if (maximumKey == null) {
                LOGGER.info("Table '{}' is empty, skipping", tableId);
                return false;
            }
            context.maximumKey(maximumKey);
            LOGGER.info("Table '{}' has data, starting chunk reads (max key set)", tableId);
        }

        if (isComplete()) {
            return false;
        }

        conn.commit();
        context.startNewChunk();

        final String selectStatement = chunkQueryBuilder.buildChunkQuery(
                context, currentTable, context.currentDataCollectionId().getAdditionalCondition());

        LOGGER.trace("Chunk query for table '{}': {}", tableId, selectStatement);

        final boolean hasData = retryPolicy.executeWithRetry(
                () -> executeChunkQuery(selectStatement, conn),
                "read chunk for table " + tableId);

        return hasData && !isComplete();
    }

    private Object[] readMaximumKey(JdbcConnection conn) throws SQLException {
        final TableId tableId = (TableId) context.currentDataCollectionId().getId();
        try {
            return retryPolicy.executeWithRetry(
                    () -> conn.queryAndMap(
                            chunkQueryBuilder.buildMaxPrimaryKeyQuery(context, currentTable,
                                    context.currentDataCollectionId().getAdditionalCondition()),
                            rs -> rs.next()
                                    ? keyFromRow(conn.rowToArray(currentTable, rs,
                                            ColumnUtils.toArray(rs, currentTable)))
                                    : null),
                    "read maximum key for table " + tableId);
        }
        catch (Exception e) {
            LOGGER.error("Failed to read maximum key for table {}", tableId, e);
            return null;
        }
    }

    private boolean executeChunkQuery(String selectStatement, JdbcConnection conn) throws SQLException {
        final TableSchema tableSchema = databaseSchema.schemaFor(currentTable.id());
        boolean hasData = false;
        Object[] lastKey = null;

        try (PreparedStatement statement = chunkQueryBuilder.readTableChunkStatement(
                context, currentTable, selectStatement, conn);
                ResultSet rs = statement.executeQuery()) {
            final ColumnUtils.ColumnArray columnArray = ColumnUtils.toArray(rs, currentTable);
            while (rs.next()) {
                hasData = true;
                final Object[] row = conn.rowToArray(currentTable, rs, columnArray);
                lastKey = keyFromRow(row);
                final Struct keyStruct = tableSchema.keyFromColumnData(row);
                windowBuffer.put(keyStruct, row);
                totalRowsRead++;
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
        if (context.maximumKey().isEmpty()) {
            return false;
        }
        return isChunkPositionComplete(context.chunkEndPosititon(), context.maximumKey().get());
    }

    public static boolean isChunkPositionComplete(Object[] currentPos, Object[] maxKey) {
        if (currentPos == null || maxKey == null) {
            return false;
        }
        return Arrays.equals(currentPos, maxKey);
    }

    public void invalidateSchemaCache() {
        this.currentTable = null;
    }

    public TableSnapshotContext<T> getContext() {
        return context;
    }

    public T getDataCollectionId() {
        return context.currentDataCollectionId().getId();
    }

    public long getTotalRowsRead() {
        return totalRowsRead;
    }
}
