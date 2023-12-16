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

    public MongoDbOffsetContext(MongoDbTaskContext taskContext, SourceInfo sourceInfo, TransactionContext transactionContext,
                                IncrementalSnapshotContext<CollectionId> incrementalSnapshotContext) {
        super(sourceInfo);
        this.transactionContext = transactionContext;
        this.incrementalSnapshotContext = incrementalSnapshotContext;
    }

    void startInitialSync() {
        sourceInfo.startInitialSync();
    }

    void stopInitialSync() {
        sourceInfo.stopInitialSync();
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, ?> getOffset() {
        Map<String, Object> offsets = (Map<String, Object>) sourceInfo.lastOffset();

        return isSnapshotRunning() ? offsets : incrementalSnapshotContext.store(transactionContext.store(offsets));
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
        return sourceInfo.isSnapshotRunning();
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
        sourceInfo.collectionEvent(collectionId, 0L);
        sourceInfo.lastOffset();
    }

    public void initEvent(MongoChangeStreamCursor<ChangeStreamDocument<BsonDocument>> cursor) {
        sourceInfo.initEvent(cursor);
    }

    public void initFromOpTimeIfNeeded(BsonTimestamp timestamp) {
        if (lastResumeToken() != null) {
            return;
        }
        LOGGER.info("Initializing offset from operation time");
        sourceInfo.noEvent(timestamp);
    }

    public void noEvent(BufferingChangeStreamCursor.ResumableChangeStreamEvent<BsonDocument> event) {
        sourceInfo.noEvent(event);
    }

    public void changeStreamEvent(ChangeStreamDocument<BsonDocument> changeStreamEvent) {
        sourceInfo.changeStreamEvent(changeStreamEvent);
    }

    public String lastResumeToken() {
        return sourceInfo.lastResumeToken();
    }

    public BsonDocument lastResumeTokenDoc() {
        final String data = sourceInfo.lastResumeToken();
        return (data == null) ? null : ResumeTokens.fromData(data);
    }

    public BsonTimestamp lastTimestamp() {
        return sourceInfo.lastTimestamp();
    }

    public boolean hasOffset() {
        return sourceInfo.hasOffset();
    }

    public static class Loader implements OffsetContext.Loader<MongoDbOffsetContext> {

        private final SourceInfo sourceInfo;
        private final MongoDbTaskContext taskContext;
        private final String replicaSetName;

        public Loader(MongoDbTaskContext taskContext) {
            this.replicaSetName = ConnectionStrings.replicaSetName(taskContext.getConnectionString());
            this.sourceInfo = new SourceInfo(taskContext.getConnectorConfig(), replicaSetName);
            this.taskContext = taskContext;
        }

        @Override
        public MongoDbOffsetContext load(Map<String, ?> offset) {
            sourceInfo.setOffset(offset);
            return new MongoDbOffsetContext(taskContext, sourceInfo, new TransactionContext(), MongoDbIncrementalSnapshotContext.load(offset, false));
        }
    }

    @Override
    public String toString() {
        return "MongoDbOffsetContext [sourceInfo=" + sourceInfo + "]";
    }
}
