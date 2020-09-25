/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import static io.debezium.connector.cassandra.CommitLogReadHandlerImpl.RowType.DELETE;
import static io.debezium.connector.cassandra.CommitLogReadHandlerImpl.RowType.INSERT;
import static io.debezium.connector.cassandra.CommitLogReadHandlerImpl.RowType.UPDATE;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.commitlog.CommitLogDescriptor;
import org.apache.cassandra.db.commitlog.CommitLogReadHandler;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.kafka.connect.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.TableMetadata;

import io.debezium.DebeziumException;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.cassandra.exceptions.CassandraConnectorSchemaException;
import io.debezium.connector.cassandra.exceptions.CassandraConnectorTaskException;
import io.debezium.connector.cassandra.transforms.CassandraTypeDeserializer;
import io.debezium.time.Conversions;

/**
 * Handler that implements {@link CommitLogReadHandler} interface provided by Cassandra source code.
 *
 * This handler implementation processes each {@link Mutation} and invokes one of the registered partition handler
 * for each {@link PartitionUpdate} in the {@link Mutation} (a mutation could have multiple partitions if it is a batch update),
 * which in turn makes one or more record via the {@link RecordMaker} and enqueue the record into the {@link ChangeEventQueue}.
 */
public class CommitLogReadHandlerImpl implements CommitLogReadHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(CommitLogReadHandlerImpl.class);

    private static final boolean MARK_OFFSET = true;

    private final ChangeEventQueue<Event> queue;
    private final RecordMaker recordMaker;
    private final OffsetWriter offsetWriter;
    private final SchemaHolder schemaHolder;
    private final CommitLogProcessorMetrics metrics;

    CommitLogReadHandlerImpl(SchemaHolder schemaHolder,
                             ChangeEventQueue<Event> queue,
                             OffsetWriter offsetWriter,
                             RecordMaker recordMaker,
                             CommitLogProcessorMetrics metrics) {
        this.queue = queue;
        this.offsetWriter = offsetWriter;
        this.recordMaker = recordMaker;
        this.schemaHolder = schemaHolder;
        this.metrics = metrics;
    }

    /**
     *  A PartitionType represents the type of a PartitionUpdate.
     */
    enum PartitionType {
        /**
         * a partition-level deletion where partition key = primary key (no clustering key)
         */
        PARTITION_KEY_ROW_DELETION,

        /**
         *  a partition-level deletion where partition key + clustering key = primary key
         */
        PARTITION_AND_CLUSTERING_KEY_ROW_DELETION,

        /**
         * a row-level modification
         */
        ROW_LEVEL_MODIFICATION,

        /**
         * an update on materialized view
         */
        MATERIALIZED_VIEW,

        /**
         * an update on secondary index
         */
        SECONDARY_INDEX,

        /**
         * an update on a table that contains counter data type
         */
        COUNTER;

        static final Set<PartitionType> supportedPartitionTypes = new HashSet<>(Arrays.asList(PARTITION_KEY_ROW_DELETION, ROW_LEVEL_MODIFICATION));

        public static PartitionType getPartitionType(PartitionUpdate pu) {
            if (pu.metadata().isCounter()) {
                return COUNTER;
            }
            else if (pu.metadata().isView()) {
                return MATERIALIZED_VIEW;
            }
            else if (pu.metadata().isIndex()) {
                return SECONDARY_INDEX;
            }
            else if (isPartitionDeletion(pu) && hasClusteringKeys(pu)) {
                return PARTITION_AND_CLUSTERING_KEY_ROW_DELETION;
            }
            else if (isPartitionDeletion(pu) && !hasClusteringKeys(pu)) {
                return PARTITION_KEY_ROW_DELETION;
            }
            else {
                return ROW_LEVEL_MODIFICATION;
            }
        }

        public static boolean isValid(PartitionType type) {
            return supportedPartitionTypes.contains(type);
        }

        public static boolean hasClusteringKeys(PartitionUpdate pu) {
            return !pu.metadata().clusteringColumns().isEmpty();
        }

        public static boolean isPartitionDeletion(PartitionUpdate pu) {
            return pu.partitionLevelDeletion().markedForDeleteAt() > LivenessInfo.NO_TIMESTAMP;
        }
    }

    /**
     *  A RowType represents different types of {@link Row}-level modifications in a Cassandra table.
     */
    enum RowType {
        /**
         * Single-row insert
         */
        INSERT,

        /**
         * Single-row update
         */
        UPDATE,

        /**
         * Single-row delete
         */
        DELETE,

        /**
         * A row-level deletion that deletes a range of keys.
         * For example: DELETE * FROM table WHERE partition_key = 1 AND clustering_key > 0;
         */
        RANGE_TOMBSTONE,

        /**
         * Unknown row-level operation
         */
        UNKNOWN;

        static final Set<RowType> supportedRowTypes = new HashSet<>(Arrays.asList(INSERT, UPDATE, DELETE));

        public static RowType getRowType(Unfiltered unfiltered) {
            if (unfiltered.isRangeTombstoneMarker()) {
                return RANGE_TOMBSTONE;
            }
            else if (unfiltered.isRow()) {
                Row row = (Row) unfiltered;
                if (isDelete(row)) {
                    return DELETE;
                }
                else if (isInsert(row)) {
                    return INSERT;
                }
                else if (isUpdate(row)) {
                    return UPDATE;
                }
            }
            return UNKNOWN;
        }

        public static boolean isValid(RowType rowType) {
            return supportedRowTypes.contains(rowType);
        }

        public static boolean isDelete(Row row) {
            return row.deletion().time().markedForDeleteAt() > LivenessInfo.NO_TIMESTAMP;
        }

        public static boolean isInsert(Row row) {
            return row.primaryKeyLivenessInfo().timestamp() > LivenessInfo.NO_TIMESTAMP;
        }

        public static boolean isUpdate(Row row) {
            return row.primaryKeyLivenessInfo().timestamp() == LivenessInfo.NO_TIMESTAMP;
        }
    }

    @Override
    public void handleMutation(Mutation mutation, int size, int entryLocation, CommitLogDescriptor descriptor) {
        if (!mutation.trackedByCDC()) {
            return;
        }

        metrics.setCommitLogPosition(entryLocation);

        for (PartitionUpdate pu : mutation.getPartitionUpdates()) {
            OffsetPosition offsetPosition = new OffsetPosition(descriptor.fileName(), entryLocation);
            KeyspaceTable keyspaceTable = new KeyspaceTable(mutation.getKeyspaceName(), pu.metadata().cfName);

            if (offsetWriter.isOffsetProcessed(keyspaceTable.name(), offsetPosition.serialize(), false)) {
                LOGGER.debug("Mutation at {} for table {} already processed, skipping...", offsetPosition, keyspaceTable);
                return;
            }

            try {
                process(pu, offsetPosition, keyspaceTable);
            }
            catch (Exception e) {
                throw new DebeziumException(String.format("Failed to process PartitionUpdate %s at %s for table %s.",
                        pu.toString(), offsetPosition.toString(), keyspaceTable.name()), e);
            }
        }

        metrics.onSuccess();
    }

    @Override
    public void handleUnrecoverableError(CommitLogReadException exception) throws IOException {
        LOGGER.error("Unrecoverable error when reading commit log", exception);
        metrics.onUnrecoverableError();
    }

    @Override
    public boolean shouldSkipSegmentOnError(CommitLogReadException exception) throws IOException {
        if (exception.permissible) {
            LOGGER.error("Encountered a permissible exception during log replay", exception);
        }
        else {
            LOGGER.error("Encountered a non-permissible exception during log replay", exception);
        }
        return false;
    }

    /**
     * Method which processes a partition update if it's valid (either a single-row partition-level
     * deletion or a row-level modification) or throw an exception if it isn't. The valid partition
     * update is then converted into a {@link Record} and enqueued to the {@link ChangeEventQueue}.
     */
    private void process(PartitionUpdate pu, OffsetPosition offsetPosition, KeyspaceTable keyspaceTable) {
        PartitionType partitionType = PartitionType.getPartitionType(pu);

        if (!PartitionType.isValid(partitionType)) {
            LOGGER.warn("Encountered an unsupported partition type {}, skipping...", partitionType);
            return;
        }

        switch (partitionType) {
            case PARTITION_KEY_ROW_DELETION:
                handlePartitionDeletion(pu, offsetPosition, keyspaceTable);
                break;

            case ROW_LEVEL_MODIFICATION:
                UnfilteredRowIterator it = pu.unfilteredIterator();
                while (it.hasNext()) {
                    Unfiltered rowOrRangeTombstone = it.next();
                    RowType rowType = RowType.getRowType(rowOrRangeTombstone);
                    if (!RowType.isValid(rowType)) {
                        LOGGER.warn("Encountered an unsupported row type {}, skipping...", rowType);
                        continue;
                    }
                    Row row = (Row) rowOrRangeTombstone;

                    handleRowModifications(row, rowType, pu, offsetPosition, keyspaceTable);
                }
                break;

            default:
                throw new CassandraConnectorSchemaException("Unsupported partition type " + partitionType + " should have been skipped");
        }
    }

    /**
     * Handle a valid deletion event resulted from a partition-level deletion by converting Cassandra representation
     * of this event into a {@link Record} object and queue the record to {@link ChangeEventQueue}. A valid deletion
     * event means a partition only has a single row, this implies there are no clustering keys.
     *
     * The steps are:
     *      (1) Populate the "source" field for this event
     *      (2) Fetch the cached key/value schemas from {@link SchemaHolder}
     *      (3) Populate the "after" field for this event
     *          a. populate partition columns
     *          b. populate regular columns with null values
     *      (4) Assemble a {@link Record} object from the populated data and queue the record
     */
    private void handlePartitionDeletion(PartitionUpdate pu, OffsetPosition offsetPosition, KeyspaceTable keyspaceTable) {
        try {

            SchemaHolder.KeyValueSchema keyValueSchema = schemaHolder.getOrUpdateKeyValueSchema(keyspaceTable);
            Schema keySchema = keyValueSchema.keySchema();
            Schema valueSchema = keyValueSchema.valueSchema();

            RowData after = new RowData();

            populatePartitionColumns(after, pu);

            // For partition deletions, the PartitionUpdate only specifies the partition key, it does not
            // contains any info on regular (non-partition) columns, as if they were not modified. In order
            // to differentiate deleted columns from unmodified columns, we populate the deleted columns
            // with null value and timestamps
            TableMetadata tableMetadata = keyValueSchema.tableMetadata();
            List<ColumnMetadata> clusteringColumns = tableMetadata.getClusteringColumns();
            if (!clusteringColumns.isEmpty()) {
                throw new CassandraConnectorSchemaException("Uh-oh... clustering key should not exist for partition deletion");
            }
            List<ColumnMetadata> columns = tableMetadata.getColumns();
            columns.removeAll(tableMetadata.getPartitionKey());
            for (ColumnMetadata cm : columns) {
                String name = cm.getName();
                long deletionTs = pu.deletionInfo().getPartitionDeletion().markedForDeleteAt();
                CellData cellData = new CellData(name, null, deletionTs, CellData.ColumnType.REGULAR);
                after.addCell(cellData);
            }

            recordMaker.delete(DatabaseDescriptor.getClusterName(), offsetPosition, keyspaceTable, false,
                    Conversions.toInstantFromMicros(pu.maxTimestamp()), after, keySchema, valueSchema,
                    MARK_OFFSET, queue::enqueue);
        }
        catch (Exception e) {
            LOGGER.error("Fail to delete partition at {}. Reason: {}", offsetPosition, e);
        }
    }

    /**
     * Handle a valid event resulted from a row-level modification by converting Cassandra representation of
     * this event into a {@link Record} object and queue the record to {@link ChangeEventQueue}. A valid event
     * implies this must be an insert, update, or delete.
     *
     * The steps are:
     *      (1) Populate the "source" field for this event
     *      (2) Fetch the cached key/value schemas from {@link SchemaHolder}
     *      (3) Populate the "after" field for this event
     *          a. populate partition columns
     *          b. populate clustering columns
     *          c. populate regular columns
     *          d. for deletions, populate regular columns with null values
     *      (4) Assemble a {@link Record} object from the populated data and queue the record
     */
    private void handleRowModifications(Row row, RowType rowType, PartitionUpdate pu, OffsetPosition offsetPosition, KeyspaceTable keyspaceTable) {

        SchemaHolder.KeyValueSchema schema = schemaHolder.getOrUpdateKeyValueSchema(keyspaceTable);
        Schema keySchema = schema.keySchema();
        Schema valueSchema = schema.valueSchema();

        RowData after = new RowData();
        populatePartitionColumns(after, pu);
        populateClusteringColumns(after, row, pu);
        populateRegularColumns(after, row, rowType, schema);

        long ts = rowType == DELETE ? row.deletion().time().markedForDeleteAt() : pu.maxTimestamp();

        switch (rowType) {
            case INSERT:
                recordMaker.insert(DatabaseDescriptor.getClusterName(), offsetPosition, keyspaceTable, false,
                        Conversions.toInstantFromMicros(ts), after, keySchema, valueSchema, MARK_OFFSET, queue::enqueue);
                break;

            case UPDATE:
                recordMaker.update(DatabaseDescriptor.getClusterName(), offsetPosition, keyspaceTable, false,
                        Conversions.toInstantFromMicros(ts), after, keySchema, valueSchema, MARK_OFFSET, queue::enqueue);
                break;

            case DELETE:
                recordMaker.delete(DatabaseDescriptor.getClusterName(), offsetPosition, keyspaceTable, false,
                        Conversions.toInstantFromMicros(ts), after, keySchema, valueSchema, MARK_OFFSET, queue::enqueue);
                break;

            default:
                throw new CassandraConnectorTaskException("Unsupported row type " + rowType + " should have been skipped");
        }
    }

    private void populatePartitionColumns(RowData after, PartitionUpdate pu) {
        List<Object> partitionKeys = getPartitionKeys(pu);
        for (ColumnDefinition cd : pu.metadata().partitionKeyColumns()) {
            try {
                String name = cd.name.toString();
                Object value = partitionKeys.get(cd.position());
                CellData cellData = new CellData(name, value, null, CellData.ColumnType.PARTITION);
                after.addCell(cellData);
            }
            catch (Exception e) {
                throw new DebeziumException(String.format("Failed to populate Column %s with Type %s of Table %s in KeySpace %s.",
                        cd.name.toString(), cd.type.toString(), cd.cfName, cd.ksName), e);
            }
        }
    }

    private void populateClusteringColumns(RowData after, Row row, PartitionUpdate pu) {
        for (ColumnDefinition cd : pu.metadata().clusteringColumns()) {
            try {
                String name = cd.name.toString();
                Object value = CassandraTypeDeserializer.deserialize(cd.type, row.clustering().get(cd.position()));
                CellData cellData = new CellData(name, value, null, CellData.ColumnType.CLUSTERING);
                after.addCell(cellData);
            }
            catch (Exception e) {
                throw new DebeziumException(String.format("Failed to populate Column %s with Type %s of Table %s in KeySpace %s.",
                        cd.name.toString(), cd.type.toString(), cd.cfName, cd.ksName), e);
            }
        }
    }

    private void populateRegularColumns(RowData after, Row row, RowType rowType, SchemaHolder.KeyValueSchema schema) {
        if (rowType == INSERT || rowType == UPDATE) {
            for (ColumnDefinition cd : row.columns()) {
                try {
                    Object value;
                    Object deletionTs = null;
                    AbstractType abstractType = cd.type;
                    if (abstractType.isCollection() && abstractType.isMultiCell()) {
                        ComplexColumnData ccd = row.getComplexColumnData(cd);
                        value = CassandraTypeDeserializer.deserialize((CollectionType) abstractType, ccd);
                    }
                    else {
                        org.apache.cassandra.db.rows.Cell cell = row.getCell(cd);
                        value = cell.isTombstone() ? null : CassandraTypeDeserializer.deserialize(abstractType, cell.value());
                        deletionTs = cell.isExpiring() ? TimeUnit.MICROSECONDS.convert(cell.localDeletionTime(), TimeUnit.SECONDS) : null;
                    }
                    String name = cd.name.toString();
                    CellData cellData = new CellData(name, value, deletionTs, CellData.ColumnType.REGULAR);
                    after.addCell(cellData);
                }
                catch (Exception e) {
                    throw new DebeziumException(String.format("Failed to populate Column %s with Type %s of Table %s in KeySpace %s.",
                            cd.name.toString(), cd.type.toString(), cd.cfName, cd.ksName), e);
                }
            }

        }
        else if (rowType == DELETE) {
            // For row-level deletions, row.columns() will result in an empty list and does not contain
            // the column definitions for the deleted columns. In order to differentiate deleted columns from
            // unmodified columns, we populate the deleted columns with null value and timestamps.
            TableMetadata tableMetadata = schema.tableMetadata();
            List<ColumnMetadata> columns = tableMetadata.getColumns();
            columns.removeAll(tableMetadata.getPrimaryKey());
            for (ColumnMetadata cm : columns) {
                String name = cm.getName();
                long deletionTs = row.deletion().time().markedForDeleteAt();
                CellData cellData = new CellData(name, null, deletionTs, CellData.ColumnType.REGULAR);
                after.addCell(cellData);
            }
        }
    }

    /**
     * Given a PartitionUpdate, deserialize the partition key byte buffer
     * into a list of partition key values.
     */
    @SuppressWarnings("checkstyle:magicnumber")
    private static List<Object> getPartitionKeys(PartitionUpdate pu) {
        List<Object> values = new ArrayList<>();

        List<ColumnDefinition> columnDefinitions = pu.metadata().partitionKeyColumns();

        // simple partition key
        if (columnDefinitions.size() == 1) {
            ByteBuffer bb = pu.partitionKey().getKey();
            ColumnSpecification cs = columnDefinitions.get(0);
            AbstractType<?> type = cs.type;
            try {
                Object value = CassandraTypeDeserializer.deserialize(type, bb);
                values.add(value);
            }
            catch (Exception e) {
                throw new DebeziumException(String.format("Failed to deserialize Column %s with Type %s in Table %s and KeySpace %s.",
                        cs.name.toString(), cs.type.toString(), cs.cfName, cs.ksName), e);
            }

            // composite partition key
        }
        else {
            ByteBuffer keyBytes = pu.partitionKey().getKey().duplicate();

            // 0xFFFF is reserved to encode "static column", skip if it exists at the start
            if (keyBytes.remaining() >= 2) {
                int header = ByteBufferUtil.getShortLength(keyBytes, keyBytes.position());
                if ((header & 0xFFFF) == 0xFFFF) {
                    ByteBufferUtil.readShortLength(keyBytes);
                }
            }

            // the encoding of columns in the partition key byte buffer is
            // <col><col><col>...
            // where <col> is:
            // <length of value><value><end-of-component byte>
            // <length of value> is a 2 bytes unsigned short (excluding 0xFFFF used to encode "static columns")
            // <end-of-component byte> should always be 0 for columns (1 for query bounds)
            // this section reads the bytes for each column and deserialize into objects based on each column type
            int i = 0;
            while (keyBytes.remaining() > 0 && i < columnDefinitions.size()) {
                ColumnSpecification cs = columnDefinitions.get(i);
                AbstractType<?> type = cs.type;
                ByteBuffer bb = ByteBufferUtil.readBytesWithShortLength(keyBytes);
                try {
                    Object value = CassandraTypeDeserializer.deserialize(type, bb);
                    values.add(value);
                }
                catch (Exception e) {
                    throw new DebeziumException(String.format("Failed to deserialize Column %s with Type %s in Table %s and KeySpace %s",
                            cs.name.toString(), cs.type.toString(), cs.cfName, cs.ksName), e);
                }
                byte b = keyBytes.get();
                if (b != 0) {
                    break;
                }
                ++i;
            }
        }

        return values;
    }
}
