/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.types.BSONTimestamp;

import com.mongodb.client.model.changestream.ChangeStreamDocument;

import io.debezium.annotation.Immutable;
import io.debezium.annotation.NotThreadSafe;
import io.debezium.connector.SnapshotRecord;
import io.debezium.connector.common.BaseSourceInfo;
import io.debezium.util.Collect;

/**
 * Information about the source of information, which includes the partitions and offsets within those partitions. The MongoDB
 * connector considers each MongoDB database a separate "partitions" (in Kafka Connect terminology), and each partition has
 * an "offset" (if Kafka Connect terminology) that defines the position within that partition/database where the connector
 * finds a particular record. As the connector uses the Kafka Connect framework to process each record in a partition, Kafka
 * Connect keeps track of the most recent offset for that partition.
 * <p>
 * The {@link #partition(String) source partition} information identifies the particular MongoDB replica set and the connector's
 * logical name of the MongoDB server. A JSON-like representation of the source partition for a database named "customers" hosted
 * in a MongoDB replica set named "myMongoServer" is as follows:
 *
 * <pre>
 * {
 *     "server_id" : "myMongoServer",
 *     "replicaSetName" : "rs0"
 * }
 * </pre>
 *
 * <p>
 * The {@link #lastOffset(String) source offset} information describes the position within a particular partition of each record.
 * Since each event in MongoDB's oplog is identified by a {@link BSONTimestamp} that tracks the time and the order of the
 * event for that particular time (e.g., multiple events that occur at the same time will have unique orders), the offset
 * includes the BSONTimetamp representation. (The event's {@code h} field is the unique ID for the operation, so this is also
 * included in the offset.) And, if an initial sync is in progress, the offset will include the {@code initsync} field.
 * <p>
 * Here's a JSON-like representation of an example timestamp:
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
public final class SourceInfo extends BaseSourceInfo {

    private static final String RESUME_TOKEN = "resume_token";

    public static final int SCHEMA_VERSION = 1;

    public static final String SERVER_ID_KEY = "server_id";
    public static final String REPLICA_SET_NAME = "rs";
    public static final String NAMESPACE = "ns";
    public static final String TIMESTAMP = "sec";
    public static final String ORDER = "ord";
    public static final String OPERATION_ID = "h";
    public static final String TX_ORD = "tord";
    public static final String SESSION_TXN_ID = "stxnid";
    public static final String INITIAL_SYNC = "initsync";
    public static final String COLLECTION = "collection";
    public static final String LSID = "lsid";
    public static final String TXN_NUMBER = "txnNumber";

    public static final String WALL_TIME = "wallTime";

    public static final String STRIPE_AUDIT = "stripeAudit";

    // Change Stream fields

    private static final BsonTimestamp INITIAL_TIMESTAMP = new BsonTimestamp();
    private static final Position INITIAL_POSITION = new Position(INITIAL_TIMESTAMP, null, 0, null, null, null);

    private final ConcurrentMap<String, Map<String, String>> sourcePartitionsByReplicaSetName = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Position> positionsByReplicaSetName = new ConcurrentHashMap<>();
    private final Set<String> initialSyncReplicaSets = Collections.newSetFromMap(new ConcurrentHashMap<>());

    private String replicaSetName;

    /**
     * Id of collection the current event applies to. May be {@code null} after noop events,
     * after which the recorded offset may be retrieved but not the source struct.
     */
    private CollectionId collectionId;
    private Position position;

    private long wallTime;

    private String stripeAudit;

    @Immutable
    protected static final class Position {
        private final Long opId;
        private final BsonTimestamp ts;
        private final long txOrder;
        private final String oplogSessionTxnId;
        private final SessionTransactionId changeStreamSessionTxnId;
        private final String resumeToken;

        public Position(int ts, int order, Long opId, long txOrder, String oplogSessionTxnId,
                        SessionTransactionId changeStreamsSessionTxnId, String resumeToken) {
            this(new BsonTimestamp(ts, order), opId, txOrder, oplogSessionTxnId, changeStreamsSessionTxnId,
                    resumeToken);
        }

        private Position(BsonTimestamp ts, Long opId, long txOrder, String oplogSessionTxnId,
                         SessionTransactionId changeStreamSessionTxnId, String resumeToken) {
            this.ts = ts;
            this.opId = opId;
            this.txOrder = txOrder;
            this.oplogSessionTxnId = oplogSessionTxnId;
            this.changeStreamSessionTxnId = changeStreamSessionTxnId;
            this.resumeToken = resumeToken;
            assert this.ts != null;
        }

        public static Position oplogPosition(BsonTimestamp ts, Long opId, long txOrder, String sessionTxnId) {
            return new Position(ts, opId, txOrder, sessionTxnId, null, null);
        }

        public static Position changeStreamPosition(BsonTimestamp ts, String resumeToken, SessionTransactionId sessionTxnId) {
            return new Position(ts, null, 0, null, sessionTxnId, resumeToken);
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

        public String getOplogSessionTxnId() {
            return oplogSessionTxnId;
        }

        public SessionTransactionId getChangeStreamSessionTxnId() {
            return changeStreamSessionTxnId;
        }

        public OptionalLong getTxOrder() {
            return txOrder == 0 ? OptionalLong.empty() : OptionalLong.of(txOrder);
        }

        public Optional<String> getResumeToken() {
            return Optional.ofNullable(resumeToken);
        }
    }

    static final class SessionTransactionId {

        public final String lsid;
        public final Long txnNumber;

        public SessionTransactionId(String lsid, Long txnNumber) {
            super();
            this.txnNumber = txnNumber;
            this.lsid = lsid;
        }
    }

    /**
     * Get the replica set name for the given partition.
     *
     * @param partition the partition map
     * @return the replica set name (when the partition is valid), or {@code null} if the partition is null or has no replica
     * set name entry
     */
    public static String replicaSetNameForPartition(Map<String, ?> partition) {
        return partition != null ? (String) partition.get(REPLICA_SET_NAME) : null;
    }

    public SourceInfo(MongoDbConnectorConfig connectorConfig) {
        super(connectorConfig);
    }

    CollectionId collectionId() {
        return collectionId;
    }

    Position position() {
        return position;
    }

    /**
     * @return server id
     */
    public String serverId() {
        return serverName();
    }

    /**
     * Get the Kafka Connect detail about the source "partition" for the given database in the replica set. If the database is
     * not known, this method records the new partition.
     *
     * @param replicaSetName the name of the replica set name for which the partition is to be obtained; may not be null
     * @return the source partition information; never null
     */
    public Map<String, String> partition(String replicaSetName) {
        if (replicaSetName == null) {
            throw new IllegalArgumentException("Replica set name may not be null");
        }
        return sourcePartitionsByReplicaSetName.computeIfAbsent(replicaSetName, rsName -> {
            return Collect.hashMapOf(SERVER_ID_KEY, serverName(), REPLICA_SET_NAME, rsName);
        });
    }

    /**
     * Get the MongoDB timestamp of the last offset position for the replica set.
     *
     * @param replicaSetName the name of the replica set name for which the new offset is to be obtained; may not be null
     * @return the timestamp of the last offset, or the beginning of time if there is none
     */
    public BsonTimestamp lastOffsetTimestamp(String replicaSetName) {
        Position existing = positionsByReplicaSetName.get(replicaSetName);
        return existing != null ? existing.ts : INITIAL_TIMESTAMP;
    }

    /**
     * Get the MongoDB transaction order of the last offset position for the replica set.
     *
     * @param replicaSetName the name of the replica set name for which the new offset is to be obtained; may not be null
     * @return the tx order of the transaction in progress or 0 in case of non-transactional event
     */
    public OptionalLong lastOffsetTxOrder(String replicaSetName) {
        Position existing = positionsByReplicaSetName.get(replicaSetName);
        return existing != null ? existing.getTxOrder() : OptionalLong.empty();
    }

    public String lastResumeToken(String replicaSetName) {
        Position existing = positionsByReplicaSetName.get(replicaSetName);
        return existing != null ? existing.resumeToken : null;
    }

    public Position lastPosition(String replicaSetName) {
        return positionsByReplicaSetName.get(replicaSetName);
    }

    /**
     * Get the Kafka Connect detail about the source "offset" for the named database, which describes the given position in the
     * database where we have last read. If the database has not yet been seen, this records the starting position
     * for that database. However, if there is a position for the database, the offset representation is returned.
     *
     * @param replicaSetName the name of the replica set name for which the new offset is to be obtained; may not be null
     * @return a copy of the current offset for the database; never null
     */
    public Map<String, ?> lastOffset(String replicaSetName) {
        Position existing = positionsByReplicaSetName.get(replicaSetName);
        if (existing == null) {
            existing = INITIAL_POSITION;
        }
        if (isInitialSyncOngoing(replicaSetName)) {
            return addSessionTxnIdToOffset(existing, Collect.hashMapOf(TIMESTAMP, Integer.valueOf(existing.getTime()),
                    ORDER, Integer.valueOf(existing.getInc()),
                    OPERATION_ID, existing.getOperationId(),
                    INITIAL_SYNC, true));
        }
        Map<String, Object> offset = Collect.hashMapOf(TIMESTAMP, Integer.valueOf(existing.getTime()),
                ORDER, Integer.valueOf(existing.getInc()),
                OPERATION_ID, existing.getOperationId());

        existing.getTxOrder().ifPresent(txOrder -> offset.put(TX_ORD, txOrder));
        existing.getResumeToken().ifPresent(resumeToken -> offset.put(RESUME_TOKEN, resumeToken));

        return addSessionTxnIdToOffset(existing, offset);
    }

    private Map<String, ?> addSessionTxnIdToOffset(Position position, Map<String, Object> offset) {
        if (position.getOplogSessionTxnId() != null) {
            offset.put(SESSION_TXN_ID, position.getOplogSessionTxnId());
        }
        if (position.getChangeStreamSessionTxnId() != null) {
            offset.put(LSID, position.getChangeStreamSessionTxnId().lsid);
            offset.put(TXN_NUMBER, position.getChangeStreamSessionTxnId().txnNumber);
        }
        return offset;
    }

    /**
     * Get a {@link Struct} representation of the source {@link #partition(String) partition} and {@link #lastOffset(String)
     * offset} information where we have last read. The Struct complies with the {@link #schema} for the MongoDB connector.
     *
     * @param replicaSetName the name of the replica set name for which the new offset is to be obtained; may not be null
     * @param collectionId   the event's collection identifier; may not be null
     * @return the source partition and offset {@link Struct}; never null
     * @see #schema()
     */
    public void collectionEvent(String replicaSetName, CollectionId collectionId, long wallTime) {
        onEvent(replicaSetName, collectionId, positionsByReplicaSetName.get(replicaSetName), wallTime, stripeAudit);
    }

    /**
     * Get a {@link Struct} representation of the source {@link #partition(String) partition} and {@link #lastOffset(String)
     * offset} information. The Struct complies with the {@link #schema} for the MongoDB connector.
     *
     * @param replicaSetName the name of the replica set name for which the new offset is to be obtained; may not be null
     * @param oplogEvent     the replica set oplog event that was last read; may be null if the position is the start of
     *                       the oplog
     * @param masterEvent    the replica set oplog event that contains event metadata; same as oplogEvent for non-transactional changes
     * @param orderInTx      order in transaction batch, 0 for non-transactional events
     * @see #schema()
     */
    public void opLogEvent(String replicaSetName, BsonDocument oplogEvent, BsonDocument masterEvent, long orderInTx) {
        Position position = INITIAL_POSITION;
        String namespace = "";
        long wallTime = 0L;
        String stripeAudit = null;
        if (oplogEvent != null) {
            BsonTimestamp ts = extractEventTimestamp(masterEvent);
            Long opId = masterEvent.containsKey("h") ? masterEvent.getInt64("h").getValue() : null;
            String sessionTxnId = extractSessionTxnId(masterEvent);
            position = Position.oplogPosition(ts, opId, orderInTx, sessionTxnId);
            namespace = oplogEvent.getString("ns").getValue();
            if (oplogEvent.containsKey("wall")) {
                wallTime = oplogEvent.getDateTime("wall").getValue();
            }
            else if (masterEvent.containsKey("wall")) {
                wallTime = masterEvent.getDateTime("wall").getValue();
            }
            if (oplogEvent.containsKey(STRIPE_AUDIT)) {
                stripeAudit = oplogEvent.getString(STRIPE_AUDIT).getValue();
            }
        }
        positionsByReplicaSetName.put(replicaSetName, position);

        onEvent(replicaSetName, CollectionId.parse(replicaSetName, namespace), position, wallTime, stripeAudit);
    }

    public void changeStreamEvent(String replicaSetName, ChangeStreamDocument<BsonDocument> changeStreamEvent, long orderInTx) {
        Position position = INITIAL_POSITION;
        String namespace = "";
        long wallTime = 0L;
        String stripeAudit = null;
        if (changeStreamEvent != null) {
            BsonTimestamp ts = changeStreamEvent.getClusterTime();
            position = Position.changeStreamPosition(ts, changeStreamEvent.getResumeToken().getString("_data").getValue(),
                    MongoUtil.getChangeStreamSessionTransactionId(changeStreamEvent));
            namespace = changeStreamEvent.getNamespace().getFullName();
            if (changeStreamEvent.getWallTime() != null) {
                wallTime = changeStreamEvent.getWallTime().getValue();
            }
            stripeAudit = extractStripeAudit(changeStreamEvent);
        }
        positionsByReplicaSetName.put(replicaSetName, position);

        onEvent(replicaSetName, CollectionId.parse(replicaSetName, namespace), position, wallTime, stripeAudit);
    }

    /**
     * Get a {@link Struct} representation of the source {@link #partition(String) partition} and {@link #lastOffset(String)
     * offset} information. The Struct complies with the {@link #schema} for the MongoDB connector.
     *
     * @param replicaSetName the name of the replica set name for which the new offset is to be obtained; may not be null
     * @param oplogEvent     the replica set oplog event that was last read; may be null if the position is the start of
     *                       the oplog
     * @see #schema()
     */
    public void opLogEvent(String replicaSetName, BsonDocument oplogEvent) {
        opLogEvent(replicaSetName, oplogEvent, oplogEvent, 0);
    }

    /**
     * Utility to extract the {@link BsonTimestamp timestamp} value from the event.
     *
     * @param oplogEvent the event
     * @return the timestamp, or null if the event is null or there is no {@code ts} field
     */
    protected static BsonTimestamp extractEventTimestamp(BsonDocument oplogEvent) {
        return oplogEvent != null ? oplogEvent.getTimestamp("ts") : null;
    }

    /**
     * Utility to extract the {@link String unique transaction id} value from the event.
     *
     * @param oplogEvent the event
     * @return the session transaction id or null
     */
    protected static String extractSessionTxnId(BsonDocument oplogEvent) {
        // In MongoDB prior to 4.2, the h field is populated.
        // For backward compatibility if h is not present or contains a zero value, then proeeed to extract
        // the session transaction unique identifier value.
        if (!oplogEvent.containsKey("h")) {
            // For MongoDB 4.2+, the h field no longer has a non-zero value.
            // In this case, the lsid and the associated txnNumber fields must be extracted and combined to
            // represent a unique identifier for the individual operation. Therefore, the return value will
            // carry the same semantics as h did for MongoDB platforms prior to 4.2.
            return MongoUtil.getOplogSessionTransactionId(oplogEvent);
        }
        return null;
    }

    protected static String extractStripeAudit(ChangeStreamDocument<BsonDocument> document) {
        BsonDocument extra = document.getExtraElements();
        if (extra != null) {
            BsonString stripAudit = extra.getString(SourceInfo.STRIPE_AUDIT);

            if (stripAudit != null) {
                return stripAudit.getValue();
            }
        }
        return null;
    }

    private void onEvent(String replicaSetName, CollectionId collectionId, Position position, long wallTime, String stripeAudit) {
        this.replicaSetName = replicaSetName;
        this.position = (position == null) ? INITIAL_POSITION : position;
        this.collectionId = collectionId;
        this.wallTime = wallTime;
        this.stripeAudit = stripeAudit;
    }

    /**
     * Determine whether we have previously recorded a MongoDB timestamp for the replica set.
     *
     * @param replicaSetName the name of the replica set name; may not be null
     * @return {@code true} if an offset has been recorded for the replica set, or {@code false} if the replica set has not
     * yet been seen
     */
    public boolean hasOffset(String replicaSetName) {
        return positionsByReplicaSetName.containsKey(replicaSetName);
    }

    /**
     * Set the source offset, as read from Kafka Connect, for the given replica set. This method does nothing if the supplied map
     * is null.
     *
     * @param replicaSetName the name of the replica set name for which the new offset is to be obtained; may not be null
     * @param sourceOffset   the previously-recorded Kafka Connect source offset; may be null
     * @return {@code true} if the offset was recorded, or {@code false} if the source offset is null
     * @throws ConnectException if any offset parameter values are missing, invalid, or of the wrong type
     */
    public boolean setOffsetFor(String replicaSetName, Map<String, ?> sourceOffset) {
        if (replicaSetName == null) {
            throw new IllegalArgumentException("The replica set name may not be null");
        }
        if (sourceOffset == null) {
            return false;
        }
        // We have previously recorded at least one offset for this database ...
        boolean initSync = booleanOffsetValue(sourceOffset, INITIAL_SYNC);
        if (initSync) {
            return false;
        }
        int time = intOffsetValue(sourceOffset, TIMESTAMP);
        int order = intOffsetValue(sourceOffset, ORDER);
        long operationId = longOffsetValue(sourceOffset, OPERATION_ID);
        long txOrder = longOffsetValue(sourceOffset, TX_ORD);
        String oplogSessionTxnId = stringOffsetValue(sourceOffset, SESSION_TXN_ID);
        String changeStreamLsid = stringOffsetValue(sourceOffset, LSID);
        Long changeStreamTxnNumber = longOffsetValue(sourceOffset, TXN_NUMBER);
        SessionTransactionId changeStreamTxnId = null;
        if (changeStreamLsid != null || changeStreamTxnNumber != null) {
            changeStreamTxnId = new SessionTransactionId(changeStreamLsid, changeStreamTxnNumber);
        }
        String resumeToken = stringOffsetValue(sourceOffset, RESUME_TOKEN);
        positionsByReplicaSetName.put(replicaSetName,
                new Position(time, order, operationId, txOrder, oplogSessionTxnId, changeStreamTxnId, resumeToken));
        return true;
    }

    /**
     * Set the source offset, as read from Kafka Connect, for the given replica set. This method does nothing if the supplied map
     * is null.
     *
     * @param partition    the partition information; may not be null
     * @param sourceOffset the previously-recorded Kafka Connect source offset; may be null
     * @return {@code true} if the offset was recorded, or {@code false} if the source offset is null
     * @throws ConnectException if any offset parameter values are missing, invalid, or of the wrong type
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

    /**
     * Determine if the initial sync for the given replica set is still ongoing.
     *
     * @param replicaSetName the name of the replica set; never null
     * @return {@code true} if the initial sync for this replica is still ongoing or was not completed before restarting, or
     * {@code false} if there is currently no initial sync operation for this replica set
     */
    public boolean isInitialSyncOngoing(String replicaSetName) {
        return initialSyncReplicaSets.contains(replicaSetName);
    }

    /**
     * Returns whether any replica sets are still running a snapshot.
     */
    public boolean isSnapshotRunning() {
        return !initialSyncReplicaSets.isEmpty();
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
        }
        catch (NumberFormatException e) {
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
        }
        catch (NumberFormatException e) {
            throw new ConnectException("Source offset '" + key + "' parameter value " + obj + " could not be converted to a long");
        }
    }

    private static String stringOffsetValue(Map<String, ?> values, String key) {
        Object obj = values.get(key);
        if (obj == null) {
            return null;
        }
        return (String) obj;
    }

    private static boolean booleanOffsetValue(Map<String, ?> values, String key) {
        Object obj = values.get(key);
        if (obj != null && obj instanceof Boolean) {
            return ((Boolean) obj).booleanValue();
        }
        return false;
    }

    @Override
    protected Instant timestamp() {
        return Instant.ofEpochSecond(position().getTime());
    }

    @Override
    protected SnapshotRecord snapshot() {
        return isInitialSyncOngoing(replicaSetName) ? SnapshotRecord.TRUE
                : snapshotRecord == SnapshotRecord.INCREMENTAL ? SnapshotRecord.INCREMENTAL : SnapshotRecord.FALSE;
    }

    @Override
    protected String database() {
        return collectionId != null ? collectionId.dbName() : null;
    }

    String replicaSetName() {
        return replicaSetName;
    }

    long wallTime() {
        return wallTime;
    }

    String stripeAudit() {
        return stripeAudit;
    }

    protected OptionalLong transactionPosition() {
        return position.getTxOrder();
    }
}
