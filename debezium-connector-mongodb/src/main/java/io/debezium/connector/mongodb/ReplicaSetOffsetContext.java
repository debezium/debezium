/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.time.Instant;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;

import com.mongodb.client.model.changestream.ChangeStreamDocument;

import io.debezium.annotation.ThreadSafe;
import io.debezium.pipeline.CommonOffsetContext;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotContext;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.spi.schema.DataCollectionId;

/**
 * An {@link OffsetContext} implementation that is specific to a single {@link ReplicaSet}.
 *
 * The mongodb connector operates multiple threads during snapshot and streaming modes where each {@link ReplicaSet}
 * is processed individually and the offsets that pertain to that {@link ReplicaSet} should be maintained in such a
 * way that is considered thread-safe.  This implementation offers such safety.
 *
 * @author Chris Cranford
 */
@ThreadSafe
public class ReplicaSetOffsetContext extends CommonOffsetContext<SourceInfo> {

    private final MongoDbOffsetContext offsetContext;
    private final String replicaSetName;
    private final IncrementalSnapshotContext<CollectionId> incrementalSnapshotContext;

    public ReplicaSetOffsetContext(MongoDbOffsetContext offsetContext, ReplicaSet replicaSet, SourceInfo sourceInfo,
                                   IncrementalSnapshotContext<CollectionId> incrementalSnapshotContext) {
        super(sourceInfo);
        this.offsetContext = offsetContext;
        this.replicaSetName = replicaSet.replicaSetName();
        this.incrementalSnapshotContext = incrementalSnapshotContext;
    }

    @Override
    public Map<String, ?> getOffset() {
        @SuppressWarnings("unchecked")
        Map<String, Object> offsets = (Map<String, Object>) sourceInfo.lastOffset(replicaSetName);
        return isSnapshotOngoing() ? offsets
                : incrementalSnapshotContext.store(offsetContext.getTransactionContext().store(offsets));
    }

    @Override
    public Schema getSourceInfoSchema() {
        return offsetContext.getSourceInfoSchema();
    }

    @Override
    public boolean isSnapshotRunning() {
        return offsetContext.isSnapshotRunning();
    }

    @Override
    public void preSnapshotStart() {
        offsetContext.preSnapshotStart();
    }

    @Override
    public void preSnapshotCompletion() {
        offsetContext.preSnapshotCompletion();
    }

    @Override
    public void event(DataCollectionId collectionId, Instant timestamp) {
        // Not used by the ReplicaSetOffsetContext, see readEvent and oplogEvent
        throw new UnsupportedOperationException();
    }

    @Override
    public TransactionContext getTransactionContext() {
        return offsetContext.getTransactionContext();
    }

    public String getReplicaSetName() {
        return replicaSetName;
    }

    public boolean isSnapshotOngoing() {
        return sourceInfo.isInitialSyncOngoing(replicaSetName);
    }

    public boolean hasOffset() {
        return sourceInfo.hasOffset(replicaSetName);
    }

    public void readEvent(CollectionId collectionId, Instant timestamp) {
        sourceInfo.collectionEvent(replicaSetName, collectionId);
        sourceInfo.lastOffset(replicaSetName);
    }

    public void changeStreamEvent(ChangeStreamDocument<BsonDocument> changeStreamEvent) {
        sourceInfo.changeStreamEvent(replicaSetName, changeStreamEvent);
    }

    public BsonTimestamp lastOffsetTimestamp() {
        return sourceInfo.lastOffsetTimestamp(replicaSetName);
    }

    public String lastResumeToken() {
        return sourceInfo.lastResumeToken(replicaSetName);
    }

    @Override
    public IncrementalSnapshotContext<?> getIncrementalSnapshotContext() {
        return offsetContext.getIncrementalSnapshotContext();
    }
}
