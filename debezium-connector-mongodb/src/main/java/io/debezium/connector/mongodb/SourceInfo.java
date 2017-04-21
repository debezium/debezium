/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.datapipeline.base.DpConstants;

import org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.dp.internal.bean.DataSourceSchemaMappingExemption;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.types.BSONTimestamp;

import io.debezium.annotation.Immutable;
import io.debezium.annotation.NotThreadSafe;
import io.debezium.util.AvroValidator;
import io.debezium.util.Collect;

/**
 * Information about the source of information, which includes the partitions and offsets within
 * those partitions. The MongoDB connector considers each MongoDB database a separate "partitions"
 * (in Kafka Connect terminology), and each partition has an "offset" (if Kafka Connect terminology)
 * that defines the position within that partition/database where the connector finds a particular
 * record. As the connector uses the Kafka Connect framework to process each record in a partition,
 * Kafka Connect keeps track of the most recent offset for that partition. <p> The {@link
 * #partition(String) source partition} information identifies the particular MongoDB replica set
 * and the connector's logical name of the MongoDB server. A JSON-like representation of the source
 * partition for a database named "customers" hosted in a MongoDB replica set named "myMongoServer"
 * is as follows:
 *
 * <pre>
 * {
 *     "server_id" : "myMongoServer",
 *     "replicaSetName" : "rs0"
 * }
 * </pre>
 *
 * <p> The {@link #lastOffset(String) source offset} information describes the position within a
 * particular partition of each record. Since each event in MongoDB's oplog is identified by a
 * {@link BSONTimestamp} that tracks the time and the order of the event for that particular time
 * (e.g., multiple events that occur at the same time will have unique orders), the offset includes
 * the BSONTimetamp representation. (The event's {@code h} field is the unique ID for the operation,
 * so this is also included in the offset.) And, if an initial sync is in progress, the offset will
 * include the {@code initsync} field. <p> Here's a JSON-like representation of an example
 * timestamp:
 *
 * <pre>
 * {
 *         "sec" = 1422998530,
 *         "ord" = 0,
 *         "h" = 398278732173914406,
 *         "initsync" = true
 * }
 * </pre>
 *
 * @author Randall Hauch
 */
@NotThreadSafe
public final class SourceInfo {

    public static final int SCHEMA_VERSION = 1;

    public static final String SERVER_ID_KEY = "server_id";
    public static final String SERVER_NAME = "name";
    public static final String REPLICA_SET_NAME = "rs";
    public static final String NAMESPACE = "ns";
    public static final String TIMESTAMP = DpConstants.DATA_KEY_BINLOG_TS;
    public static final String ORDER = "ord";
    public static final String OPERATION_ID = "h";
    public static final String INITIAL_SYNC = "initsync";
    public static final String FINISHED_COLLECTIONS = "fc";
    public static final String ONGOING_COLLECTION = "oc";
    public static final String DP_TASK_ID = "dp_id";

    private static final BsonTimestamp INITIAL_TIMESTAMP = new BsonTimestamp();
    private static final Position INITIAL_POSITION = new Position(INITIAL_TIMESTAMP, null, false);

    /**
     * A {@link Schema} definition for a {@link Struct} used to store the {@link #partition(String)}
     * and {@link #lastOffset} information.
     */
    private final Schema SOURCE_SCHEMA = SchemaBuilder.struct()
        .name(AvroValidator.defaultValidator().validate("io.debezium.connector.mongo.Source"))
        .version(SCHEMA_VERSION)
        .field(SERVER_NAME, Schema.STRING_SCHEMA)
        .field(REPLICA_SET_NAME, Schema.STRING_SCHEMA)
        .field(NAMESPACE, Schema.STRING_SCHEMA)
        .field(TIMESTAMP, Schema.INT32_SCHEMA)
        .field(ORDER, Schema.INT32_SCHEMA)
        .field(OPERATION_ID, Schema.OPTIONAL_INT64_SCHEMA)
        .field(INITIAL_SYNC, Schema.OPTIONAL_BOOLEAN_SCHEMA)
        .field(FINISHED_COLLECTIONS, Schema.OPTIONAL_STRING_SCHEMA)
        .field(ONGOING_COLLECTION, Schema.OPTIONAL_STRING_SCHEMA)
        .field(DpConstants.RECORD_OFFSET_ENTITY_KEY, Schema.OPTIONAL_STRING_SCHEMA)
        .field(DpConstants.RECORD_OFFSET_TOTAL_SIZE_KEY, Schema.OPTIONAL_INT32_SCHEMA)
        .field(DpConstants.SNAPSHOT_LASTONE_KEY, Schema.OPTIONAL_BOOLEAN_SCHEMA)
        .field(DpConstants.RECORD_OFFSET_INDEX_KEY, Schema.OPTIONAL_INT64_SCHEMA)
        .build();

    private final ConcurrentMap<String, Map<String, String>> sourcePartitionsByReplicaSetName = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Position> positionsByReplicaSetName = new ConcurrentHashMap<>();
    private final Set<String> initialSyncReplicaSets = Collections.newSetFromMap(new ConcurrentHashMap<>());

    @Immutable
    protected static final class Position {
        private final Long opId;
        private final BsonTimestamp ts;
        private final Boolean initialSync;
        private final Set<String> finishedCollections;
        private final String ongoingCollection;
        private final long expectedNumDocs;
        private final boolean isLastRecord;
        private final long index;

        public Position(BsonTimestamp ts, Long opId, Boolean initialSync) {
            this(ts, opId, initialSync, new HashSet<>(), null, 0, 0, false);
        }

        public Position(int ts, int order, Long opId, Boolean initialSync) {
            this(new BsonTimestamp(ts, order), opId, initialSync, new HashSet<>(), null, 0, 0, false);
        }

        public Position(int ts, int order, Long opId, Boolean initialSync, Set<String> finishedCollections,
                        String ongoingCollection, long expectedNumDocs, long index, boolean isLastRecord) {
            this(new BsonTimestamp(ts, order), opId, initialSync, finishedCollections,
                ongoingCollection, expectedNumDocs, index, isLastRecord);
        }

        public Position(BsonTimestamp ts, Long opId, Boolean initialSync,
                        Set<String> finishedCollections, String ongoingCollection, long expectedNumDocs,
                        long index, boolean isLastRecord) {
            this.ts = ts;
            this.opId = opId;
            this.initialSync = initialSync;
            this.finishedCollections = finishedCollections;
            this.ongoingCollection = ongoingCollection;
            this.expectedNumDocs = expectedNumDocs;
            this.index = index;
            this.isLastRecord = isLastRecord;
            assert this.ts != null;
        }

        public BsonTimestamp getTimestamp() {
            return this.ts;
        }

        public int getTime() {
            return this.ts.getTime();
        }

        public int getInc() {
            return this.ts.getInc();
        }

        public Long getOperationId() {
            return this.opId;
        }

        public Boolean getInitialSync() {
            return initialSync;
        }

        public Set<String> getFinishedCollections() {
            return finishedCollections;
        }

        public String getOngoingCollection() {
            return ongoingCollection;
        }

        public long getExpectedNumDocs() {
            return expectedNumDocs;
        }

        public boolean isLastRecord() {
            return isLastRecord;
        }

        public long getIndex() {
            return index;
        }
    }

    /**
     * Get the replica set name for the given partition.
     *
     * @param partition the partition map
     * @return the replica set name (when the partition is valid), or {@code null} if the partition
     * is null or has no replica set name entry
     */
    public static String replicaSetNameForPartition(Map<String, ?> partition) {
        return partition != null ? (String) partition.get(REPLICA_SET_NAME) : null;
    }

    private final String serverName;
    private final MongoDBSchemaCache schemaCache;
    private final List<DataSourceSchemaMappingExemption> schemaMappingExemptions;
    private final String dpTaskId;


    public SourceInfo(String dpTaskId, String serverName, MongoDBSchemaCache schemaCache,
                      List<DataSourceSchemaMappingExemption> schemaMappingExemptions) {
        this.dpTaskId = dpTaskId;
        this.serverName = serverName;
        assert this.serverName != null;
        this.schemaCache = schemaCache;
        assert this.schemaCache != null;
        this.schemaMappingExemptions = schemaMappingExemptions;
        assert this.schemaMappingExemptions != null;
    }

    public MongoDBSchemaCache getMongoDBSchemaCache() {
        return schemaCache;
    }

    public List<DataSourceSchemaMappingExemption> getSchemaMappingExemptions() {
        return schemaMappingExemptions;
    }

    public String getDpTaskId() {
        return dpTaskId;
    }

    /**
     * Get a {@link Schema} representation of the source {@link #partition(String) partition} and
     * {@link #lastOffset(String) offset} information.
     *
     * @return the source partition and offset {@link Schema}; never null
     * @see #offsetStructForEvent(String, Document)
     */
    public Schema schema() {
        return SOURCE_SCHEMA;
    }

    /**
     * Get the Kafka Connect detail about the source "partition" for the given database in the
     * replica set. If the database is not known, this method records the new partition.
     *
     * @param replicaSetName the name of the replica set name for which the partition is to be
     * obtained; may not be null
     * @return the source partition information; never null
     */
    public Map<String, String> partition(String replicaSetName) {
        if (replicaSetName == null) {
            throw new IllegalArgumentException("Replica set name may not be null");
        }
        String partitionKey = getPartitionKey(replicaSetName, dpTaskId);
        return sourcePartitionsByReplicaSetName.computeIfAbsent(partitionKey, rsName -> {
            return Collect.hashMapOf(SERVER_ID_KEY, serverName, REPLICA_SET_NAME, rsName, DP_TASK_ID, dpTaskId );
        });
    }

    /**
     * Get the MongoDB timestamp of the last offset position for the replica set.
     *
     * @param replicaSetName the name of the replica set name for which the new offset is to be
     * obtained; may not be null
     * @return the timestamp of the last offset, or the beginning of time if there is none
     */
    public BsonTimestamp lastOffsetTimestamp(String replicaSetName) {
        Position existing = positionsByReplicaSetName.get(getPartitionKey(replicaSetName, dpTaskId));
        return existing != null ? existing.ts : INITIAL_TIMESTAMP;
    }


    public boolean lastOffsetInitialSync(String replicaSetName) {
        Position existing = positionsByReplicaSetName.get(getPartitionKey(replicaSetName, dpTaskId));
        if (existing == null) {
            return false;
        } else {
            return existing.getInitialSync().booleanValue();
        }
    }

    public Set<String> lastOffsetFinishedCollection(String replicaSetName) {
        Position existing = positionsByReplicaSetName.get(getPartitionKey(replicaSetName, dpTaskId));
        return existing == null ? null : existing.getFinishedCollections();
    }


    public String lastOffsetOngoingCollection(String replicaSetName) {
        Position existing = positionsByReplicaSetName.get(getPartitionKey(replicaSetName, dpTaskId));
        return existing == null ? null : existing.getOngoingCollection();
    }

    public long lastOffsetIndex(String replicaSetName){
        Position existing = positionsByReplicaSetName.get(getPartitionKey(replicaSetName, dpTaskId));
        return existing == null ? 0 : existing.getIndex();
    }

    /**
     * Get the Kafka Connect detail about the source "offset" for the named database, which
     * describes the given position in the database where we have last read. If the database has not
     * yet been seen, this records the starting position for that database. However, if there is a
     * position for the database, the offset representation is returned.
     *
     * @param replicaSetName the name of the replica set name for which the new offset is to be
     * obtained; may not be null
     * @return a copy of the current offset for the database; never null
     */
    public Map<String, ?> lastOffset(String replicaSetName) {
        Position existing = positionsByReplicaSetName.get(getPartitionKey(replicaSetName, dpTaskId));
        if (existing == null) {
            existing = INITIAL_POSITION;
        }
        Map offset = new HashMap();
        offset.put(TIMESTAMP, new Integer(existing.getTime()));
        offset.put(ORDER, new Integer(existing.getInc()));
        offset.put(OPERATION_ID, existing.getOperationId());
        offset.put(FINISHED_COLLECTIONS, StringUtils.join(existing.getFinishedCollections(), ','));
        offset.put(ONGOING_COLLECTION, existing.getOngoingCollection());
        offset.put(DpConstants.RECORD_OFFSET_INDEX_KEY, existing.getIndex());
        offset.put(DpConstants.SNAPSHOT_LASTONE_KEY, existing.isLastRecord());
        if (isInitialSyncOngoing(replicaSetName)) {
            offset.put(INITIAL_SYNC, true);
        }
        return offset;
    }

    /**
     * Get a {@link Struct} representation of the source {@link #partition(String) partition} and
     * {@link #lastOffset(String) offset} information where we have last read. The Struct complies
     * with the {@link #schema} for the MongoDB connector.
     *
     * @param replicaSetName the name of the replica set name for which the new offset is to be
     * obtained; may not be null
     * @param collectionId the event's collection identifier; may not be null
     * @return the source partition and offset {@link Struct}; never null
     * @see #schema()
     */
    public Struct lastOffsetStruct(String replicaSetName, CollectionId collectionId,
                                   String objectId, long expectedNumDocs,
                                   long index, boolean isLastRecord) {
        Position position = positionsByReplicaSetName.get(getPartitionKey(replicaSetName, dpTaskId));
        String ongoingCollection = getOngoingCollection(collectionId, objectId);
        if (position != null) {
            position = new Position(position.getTimestamp(), position.getOperationId(), isInitialSyncOngoing(replicaSetName),
                position.getFinishedCollections(), ongoingCollection, expectedNumDocs, index, isLastRecord);
        } else {
            position = new Position(INITIAL_TIMESTAMP, null,
                isInitialSyncOngoing(replicaSetName), new HashSet<>(), ongoingCollection, expectedNumDocs, index, isLastRecord);
        }
        positionsByReplicaSetName.put(getPartitionKey(replicaSetName, dpTaskId), position);
        return offsetStructFor(replicaSetName, collectionId.namespace(), position);
    }

    public Struct updatePosition(String replicaSetName, CollectionId collectionId) {
        Position position = positionsByReplicaSetName.get(getPartitionKey(replicaSetName, dpTaskId));
        Set<String> finishedCollections = position.getFinishedCollections();
        finishedCollections.add(collectionId.namespace());
        position = new Position(position.getTimestamp(), position.getOperationId(),
            position.getInitialSync(), finishedCollections,
            position.getOngoingCollection(), position.getExpectedNumDocs(),
            position.getIndex(), position.isLastRecord());
        positionsByReplicaSetName.put(getPartitionKey(replicaSetName, dpTaskId), position);
        return offsetStructFor(replicaSetName, collectionId.namespace(), position);
    }

    /**
     * Get a {@link Struct} representation of the source {@link #partition(String) partition} and
     * {@link #lastOffset(String) offset} information. The Struct complies with the {@link #schema}
     * for the MongoDB connector.
     *
     * @param replicaSetName the name of the replica set name for which the new offset is to be
     * obtained; may not be null
     * @param oplogEvent the replica set oplog event that was last read; may be null if the position
     * is the start of the oplog
     * @return the source partition and offset {@link Struct}; never null
     * @see #schema()
     */
    public Struct offsetStructForEvent(String replicaSetName, Document oplogEvent) {
        Position position = positionsByReplicaSetName.get(getPartitionKey(replicaSetName, dpTaskId));
        if (position == null) {
            position = INITIAL_POSITION;
        }
        String namespace = "";
        if (oplogEvent != null) {
            BsonTimestamp ts = extractEventTimestamp(oplogEvent);
            Long opId = oplogEvent.getLong("h");
            position = new Position(ts, opId, isInitialSyncOngoing(replicaSetName), position.getFinishedCollections(),
                position.getOngoingCollection(), position.getExpectedNumDocs(), position.getIndex(), false);
            namespace = oplogEvent.getString("ns");
        }
        positionsByReplicaSetName.put(getPartitionKey(replicaSetName, dpTaskId), position);
        return offsetStructFor(replicaSetName, namespace, position);
    }

    /**
     * Utility to extract the {@link BsonTimestamp timestamp} value from the event.
     *
     * @param oplogEvent the event
     * @return the timestamp, or null if the event is null or there is no {@code ts} field
     */
    protected static BsonTimestamp extractEventTimestamp(Document oplogEvent) {
        return oplogEvent != null ? oplogEvent.get("ts", BsonTimestamp.class) : null;
    }

    private Struct offsetStructFor(String replicaSetName, String namespace, Position position) {
        if (position == null) {
            position = INITIAL_POSITION;
        }
        Struct result = new Struct(SOURCE_SCHEMA);
        result.put(SERVER_NAME, serverName);
        result.put(REPLICA_SET_NAME, replicaSetName);
        result.put(NAMESPACE, namespace);
        result.put(TIMESTAMP, position.getTime());
        result.put(ORDER, position.getInc());
        result.put(OPERATION_ID, position.getOperationId());
        int delimIndex = namespace.indexOf('.');
        if (delimIndex > 0) {
            String collectionName = namespace.substring(delimIndex + 1);
            result.put(DpConstants.RECORD_OFFSET_ENTITY_KEY, collectionName);
        }
        result.put(DpConstants.RECORD_OFFSET_TOTAL_SIZE_KEY, (int) position.getExpectedNumDocs());
        if (position.getInitialSync() == Boolean.TRUE) {
            result.put(INITIAL_SYNC, true);
        }
        result.put(DpConstants.RECORD_OFFSET_INDEX_KEY, position.getIndex());
        result.put(DpConstants.SNAPSHOT_LASTONE_KEY, position.isLastRecord());
        return result;
    }

    /**
     * Determine whether we have previously recorded a MongoDB timestamp for the replica set.
     *
     * @param replicaSetName the name of the replica set name; may not be null
     * @return {@code true} if an offset has been recorded for the replica set, or {@code false} if
     * the replica set has not yet been seen
     */
    public boolean hasOffset(String replicaSetName) {
        return positionsByReplicaSetName.containsKey(getPartitionKey(replicaSetName, dpTaskId));
    }

    /**
     * Set the source offset, as read from Kafka Connect, for the given replica set. This method
     * does nothing if the supplied map is null.
     *
     * @param replicaSetName the name of the replica set name for which the new offset is to be
     * obtained; may not be null
     * @param sourceOffset the previously-recorded Kafka Connect source offset; may be null
     * @return {@code true} if the offset was recorded, or {@code false} if the source offset is
     * null
     * @throws ConnectException if any offset parameter values are missing, invalid, or of the wrong
     * type
     */
    public boolean setOffsetFor(String replicaSetName, Map<String, ?> sourceOffset) {
        if (replicaSetName == null) {
            throw new IllegalArgumentException("The replica set name may not be null");
        }
        if (sourceOffset == null) {
            return false;
        }
        // We have previously recorded at least one offset for this database ...
        int time = intOffsetValue(sourceOffset, TIMESTAMP);
        int order = intOffsetValue(sourceOffset, ORDER);
        Long operationId = longOffsetValue(sourceOffset, OPERATION_ID);
        boolean initialSync = booleanOffsetValue(sourceOffset, INITIAL_SYNC);
        Set<String> finishedCollections = setoffsetValue(sourceOffset, FINISHED_COLLECTIONS);
        String ongoingCollections = String.valueOf(sourceOffset.get(ONGOING_COLLECTION));
        long index = longOffsetValue(sourceOffset, DpConstants.RECORD_OFFSET_INDEX_KEY);
        positionsByReplicaSetName.put(replicaSetName, new Position(time, order, operationId,
            initialSync, finishedCollections, ongoingCollections, 0, index, false));
        return true;
    }

    /**
     * Set the source offset, as read from Kafka Connect, for the given replica set. This method
     * does nothing if the supplied map is null.
     *
     * @param partition the partition information; may not be null
     * @param sourceOffset the previously-recorded Kafka Connect source offset; may be null
     * @return {@code true} if the offset was recorded, or {@code false} if the source offset is
     * null
     * @throws ConnectException if any offset parameter values are missing, invalid, or of the wrong
     * type
     */
    public boolean setOffsetFor(Map<String, String> partition, Map<String, ?> sourceOffset) {
        String replicaSetName = partition.get(REPLICA_SET_NAME);
        return setOffsetFor(replicaSetName, sourceOffset);
    }

    /**
     * Record that an initial sync has started for the given replica set.
     *
     * @param replicaSetName the name of the replica set; never null
     */
    public void startInitialSync(String replicaSetName) {
        initialSyncReplicaSets.add(replicaSetName);
    }

    /**
     * Record that an initial sync has stopped for the given replica set.
     *
     * @param replicaSetName the name of the replica set; never null
     */
    public void stopInitialSync(String replicaSetName) {
        initialSyncReplicaSets.remove(replicaSetName);
    }


    private String getOngoingCollection(CollectionId id, String objectId) {
        return id.namespace() + ":" + objectId;
    }

    /**
     * Determine if the initial sync for the given replica set is still ongoing.
     *
     * @param replicaSetName the name of the replica set; never null
     * @return {@code true} if the initial sync for this replica is still ongoing or was not
     * completed before restarting, or {@code false} if there is currently no initial sync operation
     * for this replica set
     */
    public boolean isInitialSyncOngoing(String replicaSetName) {
        return initialSyncReplicaSets.contains(replicaSetName);
    }

    private static int intOffsetValue(Map<String, ?> values, String key) {
        Object obj = values.get(key);
        if (obj == null) {
            return 0;
        }
        if (obj instanceof Number) {
            return ((Number) obj).intValue();
        }
        try {
            return Integer.parseInt(obj.toString());
        } catch (NumberFormatException e) {
            throw new ConnectException("Source offset '" + key + "' parameter value " + obj + " could not be converted to an integer");
        }
    }

    private static long longOffsetValue(Map<String, ?> values, String key) {
        Object obj = values.get(key);
        if (obj == null) {
            return 0;
        }
        if (obj instanceof Number) {
            return ((Number) obj).longValue();
        }
        try {
            return Long.parseLong(obj.toString());
        } catch (NumberFormatException e) {
            throw new ConnectException("Source offset '" + key + "' parameter value " + obj + " could not be converted to a long");
        }
    }

    private static boolean booleanOffsetValue(Map<String, ?> values, String key) {
        Object obj = values.get(key);
        if (obj == null) {
            return false;
        }
        return Boolean.parseBoolean(obj.toString());
    }

    private static Set<String> setoffsetValue(Map<String, ?> values, String key) {
        Set<String> result = new HashSet<>();
        Object obj = values.get(key);
        if (obj == null) {
            return null;
        }
        String[] splits = String.valueOf(obj).split(",");
        for (String split : splits) {
            result.add(split);
        }
        return result;
    }


    public String getPartitionKey(String rsName, String dataPipelineId){
        return rsName+"_"+dataPipelineId;
    }
}
