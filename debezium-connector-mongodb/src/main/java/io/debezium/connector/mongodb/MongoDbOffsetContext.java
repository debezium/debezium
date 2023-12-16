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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.model.changestream.ChangeStreamDocument;

import io.debezium.connector.SnapshotRecord;
import io.debezium.connector.mongodb.connection.ConnectionStrings;
import io.debezium.connector.mongodb.events.BufferingChangeStreamCursor;
import io.debezium.connector.mongodb.snapshot.MongoDbIncrementalSnapshotContext;
import io.debezium.pipeline.CommonOffsetContext;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotContext;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.spi.schema.DataCollectionId;

/**
 * A context that facilitates the management of the current offsets across a set of mongodb replica sets.
 *
 * @author Chris Cranford
 */
public class MongoDbOffsetContext extends CommonOffsetContext<SourceInfo> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MongoDbOffsetContext.class);

    private final TransactionContext transactionContext;
    private final IncrementalSnapshotContext<CollectionId> incrementalSnapshotContext;
    private final String replicaSetName;

    public MongoDbOffsetContext(MongoDbTaskContext taskContext, SourceInfo sourceInfo, TransactionContext transactionContext,
                                IncrementalSnapshotContext<CollectionId> incrementalSnapshotContext) {
        super(sourceInfo);
        this.replicaSetName = ConnectionStrings.replicaSetName(taskContext.getConnectionString());
        this.transactionContext = transactionContext;
        this.incrementalSnapshotContext = incrementalSnapshotContext;
    }

    void startReplicaSetSnapshot() {
        sourceInfo.startInitialSync(replicaSetName);
    }

    void stopReplicaSetSnapshot() {
        sourceInfo.stopInitialSync(replicaSetName);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, ?> getOffset() {
        Map<String, Object> offsets = (Map<String, Object>) sourceInfo.lastOffset(replicaSetName);
        return isSnapshotOngoing() ? offsets : incrementalSnapshotContext.store(transactionContext.store(offsets));
    }

    @Override
    public Schema getSourceInfoSchema() {
        return sourceInfo.schema();
    }

    @Override
    public boolean isSnapshotRunning() {
        return sourceInfo.isSnapshot() && sourceInfo.isSnapshotRunning();
    }

    public boolean isSnapshotOngoing() {
        return sourceInfo.isInitialSyncOngoing(replicaSetName);
    }

    @Override
    public void preSnapshotStart() {
        sourceInfo.setSnapshot(SnapshotRecord.TRUE);
    }

    @Override
    public void preSnapshotCompletion() {
    }

    @Override
    public TransactionContext getTransactionContext() {
        return transactionContext;
    }

    @Override
    public IncrementalSnapshotContext<?> getIncrementalSnapshotContext() {
        return incrementalSnapshotContext;
    }

    @Override
    public void event(DataCollectionId collectionId, Instant timestamp) {
        // Not used by the mongodb connector, see readEvent and oplogEvent
        throw new UnsupportedOperationException();
    }

    public void readEvent(CollectionId collectionId, Instant timestamp) {
        sourceInfo.collectionEvent(replicaSetName, collectionId, 0L);
        sourceInfo.lastOffset(replicaSetName);
    }

    public void initEvent(MongoChangeStreamCursor<ChangeStreamDocument<BsonDocument>> cursor) {
        sourceInfo.initEvent(replicaSetName, cursor);
    }

    public void initFromOpTimeIfNeeded(BsonTimestamp timestamp) {
        if (lastResumeToken() != null) {
            return;
        }
        LOGGER.info("Initializing offset for replica-set {} from operation time", replicaSetName);
        sourceInfo.noEvent(replicaSetName, timestamp);
    }

    public void noEvent(BufferingChangeStreamCursor.ResumableChangeStreamEvent<BsonDocument> event) {
        sourceInfo.noEvent(replicaSetName, event);
    }

    public void changeStreamEvent(ChangeStreamDocument<BsonDocument> changeStreamEvent) {
        sourceInfo.changeStreamEvent(replicaSetName, changeStreamEvent);
    }

    public String lastResumeToken() {
        return sourceInfo.lastResumeToken(replicaSetName);
    }

    public BsonDocument lastResumeTokenDoc() {
        final String data = sourceInfo.lastResumeToken(replicaSetName);
        return (data == null) ? null : ResumeTokens.fromData(data);
    }

    public BsonTimestamp lastTimestamp() {
        return sourceInfo.lastTimestamp(replicaSetName);
    }

    public boolean hasOffset() {
        return sourceInfo.hasOffset(replicaSetName);
    }

    public static class Loader implements OffsetContext.Loader<MongoDbOffsetContext> {

        private final SourceInfo sourceInfo;
        private final MongoDbTaskContext taskContext;
        private final String replicaSetName;

        public Loader(MongoDbTaskContext taskContext) {
            this.sourceInfo = new SourceInfo(taskContext.getConnectorConfig());
            this.replicaSetName = ConnectionStrings.replicaSetName(taskContext.getConnectionString());
            this.taskContext = taskContext;
        }

        @Override
        public MongoDbOffsetContext load(Map<String, ?> offset) {
            sourceInfo.setOffsetFor(replicaSetName, offset);
            return new MongoDbOffsetContext(taskContext, sourceInfo, new TransactionContext(), MongoDbIncrementalSnapshotContext.load(offset, false));
        }
    }

    @Override
    public String toString() {
        return "MongoDbOffsetContext [sourceInfo=" + sourceInfo + "]";
    }
}
