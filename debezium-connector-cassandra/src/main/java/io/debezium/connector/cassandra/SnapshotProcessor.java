/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import java.io.IOException;
import java.time.Duration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.kafka.connect.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.querybuilder.BuiltStatement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;

import io.debezium.DebeziumException;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.cassandra.exceptions.CassandraConnectorTaskException;
import io.debezium.connector.cassandra.transforms.CassandraTypeDeserializer;
import io.debezium.time.Conversions;
import io.debezium.util.Collect;

/**
 * This reader is responsible for initial bootstrapping of a table,
 * which entails converting each row into a change event and enqueueing
 * that event to the {@link ChangeEventQueue}.
 *
 * IMPORTANT: Currently, only when a snapshot is completed will the OffsetWriter
 * record the table in the offset.properties file (with filename "" and position
 * -1). This means if the SnapshotProcessor is terminated midway, upon restart
 * it will skip all the tables that are already recorded in offset.properties
 */
public class SnapshotProcessor extends AbstractProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(SnapshotProcessor.class);

    private static final String NAME = "Snapshot Processor";
    private static final String CASSANDRA_NOW_UNIXTIMESTAMP = "UNIXTIMESTAMPOF(NOW())";
    private static final String EXECUTION_TIME_ALIAS = "execution_time";
    private static final Set<DataType.Name> collectionTypes = Collect.unmodifiableSet(DataType.Name.LIST, DataType.Name.SET, DataType.Name.MAP);

    private final CassandraClient cassandraClient;
    private final ChangeEventQueue<Event> queue;
    private final OffsetWriter offsetWriter;
    private final SchemaHolder schemaHolder;
    private final RecordMaker recordMaker;
    private final CassandraConnectorConfig.SnapshotMode snapshotMode;
    private final ConsistencyLevel consistencyLevel;
    private final Set<String> startedTableNames = new HashSet<>();
    private final SnapshotProcessorMetrics metrics = new SnapshotProcessorMetrics();
    private boolean initial = true;

    public SnapshotProcessor(CassandraConnectorContext context) {
        super(NAME, context.getCassandraConnectorConfig().snapshotPollIntervalMs().toMillis());
        cassandraClient = context.getCassandraClient();
        queue = context.getQueue();
        offsetWriter = context.getOffsetWriter();
        schemaHolder = context.getSchemaHolder();
        recordMaker = new RecordMaker(context.getCassandraConnectorConfig().tombstonesOnDelete(),
                new Filters(context.getCassandraConnectorConfig().fieldExcludeList()),
                context.getCassandraConnectorConfig());
        snapshotMode = context.getCassandraConnectorConfig().snapshotMode();
        consistencyLevel = context.getCassandraConnectorConfig().snapshotConsistencyLevel();
    }

    @Override
    public void initialize() {
        metrics.registerMetrics();
    }

    @Override
    public void destroy() {
        metrics.unregisterMetrics();
    }

    @Override
    public void process() {
        if (snapshotMode == CassandraConnectorConfig.SnapshotMode.ALWAYS) {
            snapshot();
        }
        else if (snapshotMode == CassandraConnectorConfig.SnapshotMode.INITIAL && initial) {
            snapshot();
            initial = false;
        }
        else {
            LOGGER.debug("Skipping snapshot [mode: {}]", snapshotMode);
        }
    }

    /**
     * Fetch for all new tables that have not yet been snapshotted, and then iterate through the
     * tables to snapshot each one of them.
     */
    synchronized void snapshot() {
        try {
            Set<TableMetadata> tables = getTablesToSnapshot();
            if (!tables.isEmpty()) {
                String[] tableArr = tables.stream().map(SnapshotProcessor::tableName).toArray(String[]::new);
                LOGGER.info("Found {} tables to snapshot: {}", tables.size(), tableArr);
                long startTime = System.currentTimeMillis();
                metrics.setTableCount(tables.size());
                metrics.startSnapshot();
                for (TableMetadata table : tables) {
                    if (isRunning()) {
                        String tableName = tableName(table);
                        LOGGER.info("Snapshotting table {}", tableName);
                        startedTableNames.add(tableName);
                        takeTableSnapshot(table);
                        metrics.completeTable();
                    }
                }
                metrics.stopSnapshot();
                long endTime = System.currentTimeMillis();
                long durationInSeconds = Duration.ofMillis(endTime - startTime).getSeconds();
                LOGGER.info("Snapshot completely queued in {} seconds for tables: {}", durationInSeconds, tableArr);
            }
            else {
                LOGGER.info("No tables to snapshot");
            }
        }
        catch (IOException e) {
            throw new CassandraConnectorTaskException(e);
        }
    }

    /**
     * Return a set of {@link TableMetadata} for tables that have not been snapshotted but have CDC enabled.
     */
    private Set<TableMetadata> getTablesToSnapshot() {
        return schemaHolder.getCdcEnabledTableMetadataSet().stream()
                .filter(tm -> !offsetWriter.isOffsetProcessed(tableName(tm), OffsetPosition.defaultOffsetPosition().serialize(), true))
                .filter(tm -> !startedTableNames.contains(tableName(tm)))
                .collect(Collectors.toSet());
    }

    /**
     * Runs a SELECT query on a given table and process each row in the result set
     * by converting the row into a record and enqueue it to {@link ChangeRecord}
     */
    private void takeTableSnapshot(TableMetadata tableMetadata) throws IOException {
        try {
            BuiltStatement statement = generateSnapshotStatement(tableMetadata);
            statement.setConsistencyLevel(consistencyLevel);
            LOGGER.info("Executing snapshot query '{}' with consistency level {}", statement.getQueryString(), statement.getConsistencyLevel());
            ResultSet resultSet = cassandraClient.execute(statement);
            processResultSet(tableMetadata, resultSet);
            LOGGER.debug("The snapshot of table '{}' has been taken", tableName(tableMetadata));
        }
        catch (IOException e) {
            throw e;
        }
        catch (Exception e) {
            throw new DebeziumException(String.format("Failed to snapshot table %s in keyspace %s", tableMetadata.getName(), tableMetadata.getKeyspace().getName()), e);
        }
    }

    /**
     * Build the SELECT query statement for execution. For every non-primary-key column, the TTL, WRITETIME, and execution
     * time are also queried.
     *
     * For example, a table t with columns a, b, and c, where A is the partition key, B is the clustering key, and C is a
     * regular column, looks like the following:
     * <pre>
     *     {@code SELECT now() as execution_time, a, b, c, TTL(c) as c_ttl, WRITETIME(c) as c_writetime FROM t;}
     * </pre>
     */
    private static BuiltStatement generateSnapshotStatement(TableMetadata tableMetadata) {
        List<String> allCols = tableMetadata.getColumns().stream().map(ColumnMetadata::getName).collect(Collectors.toList());
        Set<String> primaryCols = tableMetadata.getPrimaryKey().stream().map(ColumnMetadata::getName).collect(Collectors.toSet());
        List<String> collectionCols = tableMetadata.getColumns().stream().filter(cm -> collectionTypes.contains(cm.getType().getName()))
                .map(ColumnMetadata::getName).collect(Collectors.toList());

        Select.Selection selection = QueryBuilder.select().raw(CASSANDRA_NOW_UNIXTIMESTAMP).as(EXECUTION_TIME_ALIAS);
        for (String col : allCols) {
            selection.column(withQuotes(col));

            if (!primaryCols.contains(col) && !collectionCols.contains(col)) {
                selection.ttl(withQuotes(col)).as(ttlAlias(col));
            }
        }
        return selection.from(tableMetadata.getKeyspace().getName(), tableMetadata.getName());
    }

    /**
     * Process the result set from the query. Each row is converted into a {@link ChangeRecord}
     * and enqueued to the {@link ChangeEventQueue}.
     */
    private void processResultSet(TableMetadata tableMetadata, ResultSet resultSet) throws IOException {
        String tableName = tableName(tableMetadata);
        KeyspaceTable keyspaceTable = new KeyspaceTable(tableMetadata);
        SchemaHolder.KeyValueSchema keyValueSchema = schemaHolder.getOrUpdateKeyValueSchema(keyspaceTable);
        Schema keySchema = keyValueSchema.keySchema();
        Schema valueSchema = keyValueSchema.valueSchema();

        Set<String> partitionKeyNames = tableMetadata.getPartitionKey().stream().map(ColumnMetadata::getName).collect(Collectors.toSet());
        Set<String> clusteringKeyNames = tableMetadata.getClusteringColumns().stream().map(ColumnMetadata::getName).collect(Collectors.toSet());

        Iterator<Row> rowIter = resultSet.iterator();
        long rowNum = 0L;
        // mark snapshot complete immediately if table is empty
        if (!rowIter.hasNext()) {
            offsetWriter.markOffset(tableName, OffsetPosition.defaultOffsetPosition().serialize(), true);
            offsetWriter.flush();
        }

        while (rowIter.hasNext()) {
            if (isRunning()) {
                Row row = rowIter.next();
                Object executionTime = readExecutionTime(row);
                RowData after = extractRowData(row, tableMetadata.getColumns(), partitionKeyNames, clusteringKeyNames, executionTime);
                // only mark offset if there are no more rows left
                boolean markOffset = !rowIter.hasNext();
                recordMaker.insert(DatabaseDescriptor.getClusterName(), OffsetPosition.defaultOffsetPosition(),
                        keyspaceTable, true, Conversions.toInstantFromMicros(TimeUnit.MICROSECONDS.convert((long) executionTime, TimeUnit.MILLISECONDS)),
                        after, keySchema, valueSchema, markOffset, queue::enqueue);
                rowNum++;
                if (rowNum % 10_000 == 0) {
                    LOGGER.info("Queued {} snapshot records from table {}", rowNum, tableName);
                    metrics.setRowsScanned(tableName, rowNum);
                }
            }
            else {
                LOGGER.warn("Terminated snapshot processing while table {} is in progress", tableName);
                metrics.setRowsScanned(tableName, rowNum);
                return;
            }
        }
        metrics.setRowsScanned(tableName, rowNum);
    }

    /**
     * This function extracts the relevant row data from {@link Row} and updates the maximum writetime for each row.
     */
    private static RowData extractRowData(Row row, List<ColumnMetadata> columns, Set<String> partitionKeyNames, Set<String> clusteringKeyNames, Object executionTime) {
        RowData rowData = new RowData();

        for (ColumnMetadata columnMetadata : columns) {
            String name = columnMetadata.getName();
            Object value = readCol(row, name, columnMetadata);
            Object deletionTs = null;
            CellData.ColumnType type = getType(name, partitionKeyNames, clusteringKeyNames);

            if (type == CellData.ColumnType.REGULAR && value != null && !collectionTypes.contains(columnMetadata.getType().getName())) {
                Object ttl = readColTtl(row, name);
                if (ttl != null && executionTime != null) {
                    deletionTs = calculateDeletionTs(executionTime, ttl);
                }
            }

            CellData cellData = new CellData(name, value, deletionTs, type);
            rowData.addCell(cellData);
        }

        return rowData;
    }

    private static CellData.ColumnType getType(String name, Set<String> partitionKeyNames, Set<String> clusteringKeyNames) {
        if (partitionKeyNames.contains(name)) {
            return CellData.ColumnType.PARTITION;
        }
        else if (clusteringKeyNames.contains(name)) {
            return CellData.ColumnType.CLUSTERING;
        }
        else {
            return CellData.ColumnType.REGULAR;
        }
    }

    private static Object readExecutionTime(Row row) {
        return CassandraTypeDeserializer.deserialize(DataType.bigint(), row.getBytesUnsafe(EXECUTION_TIME_ALIAS));
    }

    private static Object readCol(Row row, String col, ColumnMetadata cm) {
        return CassandraTypeDeserializer.deserialize(cm.getType(), row.getBytesUnsafe(col));
    }

    private static Object readColTtl(Row row, String col) {
        return CassandraTypeDeserializer.deserialize(DataType.cint(), row.getBytesUnsafe(ttlAlias(col)));
    }

    /**
     * it is not possible to query deletion time via cql, so instead calculate it from execution time (in milliseconds) + ttl (in seconds)
     */
    private static long calculateDeletionTs(Object executionTime, Object ttl) {
        return TimeUnit.MICROSECONDS.convert((long) executionTime, TimeUnit.MILLISECONDS) + TimeUnit.MICROSECONDS.convert((int) ttl, TimeUnit.SECONDS);
    }

    private static String ttlAlias(String colName) {
        return colName + "_ttl";
    }

    private static String withQuotes(String s) {
        return "\"" + s + "\"";
    }

    private static String tableName(TableMetadata tm) {
        return tm.getKeyspace().getName() + "." + tm.getName();
    }
}
