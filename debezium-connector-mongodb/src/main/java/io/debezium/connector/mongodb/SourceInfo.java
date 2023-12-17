/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.time.Instant;
import java.util.Optional;

import org.apache.kafka.connect.data.Struct;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.types.BSONTimestamp;

import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.model.changestream.ChangeStreamDocument;

import io.debezium.annotation.Immutable;
import io.debezium.annotation.NotThreadSafe;
import io.debezium.connector.SnapshotRecord;
import io.debezium.connector.common.BaseSourceInfo;
import io.debezium.connector.mongodb.events.BufferingChangeStreamCursor.ResumableChangeStreamEvent;

/**
 * Information about the source of information, which includes the partitions and offsets within those partitions. The MongoDB
 * connector considers each MongoDB database a separate "partitions" (in Kafka Connect terminology), and each partition has
 * an "offset" (if Kafka Connect terminology) that defines the position within that partition/database where the connector
 * finds a particular record. As the connector uses the Kafka Connect framework to process each record in a partition, Kafka
 * Connect keeps track of the most recent offset for that partition.
 * <p>
 * The {@link #partition() source partition} information identifies the particular MongoDB replica set and the connector's
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
 * The {@link #lastOffset() source offset} information describes the position within a particular partition of each record.
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

    public static final String REPLICA_SET_NAME = "rs";
    public static final String RESUME_TOKEN = "resume_token";
    public static final String TIMESTAMP = "sec";
    public static final String ORDER = "ord";
    public static final String INITIAL_SYNC = "initsync";
    public static final String COLLECTION = "collection";
    public static final String LSID = "lsid";
    public static final String TXN_NUMBER = "txnNumber";

    public static final String WALL_TIME = "wallTime";

    // Change Stream fields
    private static final BsonTimestamp INITIAL_TIMESTAMP = new BsonTimestamp();
    private static final Position INITIAL_POSITION = new Position(INITIAL_TIMESTAMP, null, null);
    public boolean initialSync = false;
    private final MongoDbConnectorConfig connectorConfig;

    /**
     * Id of collection the current event applies to. May be {@code null} after noop events,
     * after which the recorded offset may be retrieved but not the source struct.
     */
    private CollectionId collectionId;
    private Position position = null;

    private long wallTime;

    @Immutable
    protected static final class Position {
        private final BsonTimestamp ts;
        private final SessionTransactionId changeStreamSessionTxnId;
        private final String resumeToken;

        public Position(BsonTimestamp ts, SessionTransactionId changeStreamsSessionTxnId, String resumeToken) {
            this.ts = ts;
            this.changeStreamSessionTxnId = changeStreamsSessionTxnId;
            this.resumeToken = resumeToken;
        }

        public static Position changeStreamPosition(BsonTimestamp ts, String resumeToken, SessionTransactionId sessionTxnId) {
            return new Position(ts, sessionTxnId, resumeToken);
        }

        public BsonTimestamp getTimestamp() {
            return this.ts;
        }

        public int getTime() {
            return (this.ts != null) ? this.ts.getTime() : 0;
        }

        public int getInc() {
            return (this.ts != null) ? this.ts.getInc() : -1;
        }

        public SessionTransactionId getChangeStreamSessionTxnId() {
            return changeStreamSessionTxnId;
        }

        public Optional<String> getResumeToken() {
            return Optional.ofNullable(resumeToken);
        }

        @Override
        public String toString() {
            return "Position [ts=" + ts + ", changeStreamSessionTxnId=" + changeStreamSessionTxnId + ", resumeToken="
                    + resumeToken + "]";
        }
    }

    static final class SessionTransactionId {

        public final String lsid;
        public final Long txnNumber;

        SessionTransactionId(String lsid, Long txnNumber) {
            super();
            this.txnNumber = txnNumber;
            this.lsid = lsid;
        }
    }

    public SourceInfo(MongoDbConnectorConfig connectorConfig) {
        super(connectorConfig);
        this.connectorConfig = connectorConfig;
    }

    CollectionId collectionId() {
        return collectionId;
    }

    Position position() {
        return position != null ? position : INITIAL_POSITION;
    }

    public String lastResumeToken() {
        return position != null ? position.resumeToken : null;
    }

    public BsonTimestamp lastTimestamp() {
        return position != null ? position.getTimestamp() : null;
    }

    /**
     * Get a {@link Struct} representation of the source partition and offset
     * information where we have last read. The Struct complies with the {@link #schema} for the MongoDB connector.
     *
     * @param collectionId the event's collection identifier; may not be null
     * @see #schema()
     */
    public void collectionEvent(CollectionId collectionId, long wallTime) {
        onEvent(collectionId, position, wallTime);
    }

    public void initEvent(MongoChangeStreamCursor<ChangeStreamDocument<BsonDocument>> cursor) {
        if (cursor == null) {
            return;
        }

        ChangeStreamDocument<BsonDocument> result = cursor.tryNext();
        if (result == null) {
            noEvent(cursor);
        }
        else {
            changeStreamEvent(result);
        }
    }

    public void noEvent(ResumableChangeStreamEvent<BsonDocument> event) {
        if (event.hasDocument()) {
            return;
        }
        noEvent(ResumeTokens.getDataString(event.resumeToken));
    }

    public void noEvent(MongoChangeStreamCursor<?> cursor) {
        if (cursor == null || cursor.getResumeToken() == null) {
            return;
        }
        noEvent(ResumeTokens.getDataString(cursor.getResumeToken()));
    }

    public void noEvent(BsonTimestamp timestamp) {
        if (timestamp == null) {
            return;
        }
        Position position = Position.changeStreamPosition(timestamp, null, null);
        noEvent(position);
    }

    private void noEvent(String resumeToken) {
        if (resumeToken == null) {
            return;
        }
        Position position = Position.changeStreamPosition(null, resumeToken, null);
        noEvent(position);
    }

    private void noEvent(Position position) {
        String namespace = "";
        long wallTime = 0L;
        onEvent(CollectionId.parse(connectorConfig.getReplicaSetName(), namespace), position, wallTime);
    }

    public void changeStreamEvent(ChangeStreamDocument<BsonDocument> changeStreamEvent) {
        Position position = INITIAL_POSITION;
        String namespace = "";
        long wallTime = 0L;
        if (changeStreamEvent != null) {
            String resumeToken = ResumeTokens.getDataString(changeStreamEvent.getResumeToken());
            BsonTimestamp ts = changeStreamEvent.getClusterTime();
            position = Position.changeStreamPosition(ts, resumeToken, MongoUtil.getChangeStreamSessionTransactionId(changeStreamEvent));
            namespace = changeStreamEvent.getNamespace().getFullName();
            if (changeStreamEvent.getWallTime() != null) {
                wallTime = changeStreamEvent.getWallTime().getValue();
            }
        }

        onEvent(CollectionId.parse(connectorConfig.getReplicaSetName(), namespace), position, wallTime);
    }

    private void onEvent(CollectionId collectionId, Position position, long wallTime) {
        this.position = (position == null) ? INITIAL_POSITION : position;
        this.collectionId = collectionId;
        this.wallTime = wallTime;
    }

    /**
     * Determine whether we have previously recorded a MongoDB timestamp for the replica set.
     *
     * @return {@code true} if an offset has been recorded for the replica set, or {@code false} if the replica set has not
     *         yet been seen
     */
    public boolean hasPosition() {
        return position != null;
    }

    public void setPosition(Position position) {
        this.position = position;
    }

    /**
     * Record that an initial sync has started for the given replica set.
     */
    public void startInitialSync() {
        this.initialSync = true;
    }

    /**
     * Record that an initial sync has stopped for the given replica set.
     */
    public void stopInitialSync() {
        this.initialSync = false;
    }

    /**
     * Returns whether any replica sets are still running a snapshot.
     */
    public boolean isSnapshotRunning() {
        return initialSync;
    }

    @Override
    protected Instant timestamp() {
        return Instant.ofEpochSecond(position().getTime());
    }

    @Override
    public SnapshotRecord snapshot() {
        return isSnapshotRunning() ? SnapshotRecord.TRUE
                : snapshotRecord == SnapshotRecord.INCREMENTAL ? SnapshotRecord.INCREMENTAL : SnapshotRecord.FALSE;
    }

    @Override
    protected String database() {
        return collectionId != null ? collectionId.dbName() : null;
    }

    String replicaSetName() {
        return connectorConfig.getReplicaSetName();
    }

    long wallTime() {
        return wallTime;
    }

    @Override
    public String toString() {
        return "SourceInfo [initialSync=" + initialSync + ", collectionId=" + collectionId + ", position=" + position + "]";
    }
}
