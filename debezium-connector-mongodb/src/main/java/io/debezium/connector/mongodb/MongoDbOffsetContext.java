/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static io.debezium.connector.common.OffsetUtils.booleanOffsetValue;
import static io.debezium.connector.common.OffsetUtils.longOffsetValue;
import static io.debezium.connector.common.OffsetUtils.stringOffsetValue;
import static io.debezium.connector.mongodb.SourceInfo.INITIAL_SYNC;
import static io.debezium.connector.mongodb.SourceInfo.LSID;
import static io.debezium.connector.mongodb.SourceInfo.ORDER;
import static io.debezium.connector.mongodb.SourceInfo.RESUME_TOKEN;
import static io.debezium.connector.mongodb.SourceInfo.TIMESTAMP;
import static io.debezium.connector.mongodb.SourceInfo.TXN_NUMBER;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.connect.data.Schema;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.model.changestream.ChangeStreamDocument;

import io.debezium.DebeziumException;
import io.debezium.connector.SnapshotRecord;
import io.debezium.connector.mongodb.events.BufferingChangeStreamCursor;
import io.debezium.connector.mongodb.snapshot.MongoDbIncrementalSnapshotContext;
import io.debezium.pipeline.CommonOffsetContext;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotContext;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.util.Collect;

/**
 * A context that facilitates the management of the current offsets across a set of mongodb replica sets.
 *
 * @author Chris Cranford
 */
public class MongoDbOffsetContext extends CommonOffsetContext<SourceInfo> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MongoDbOffsetContext.class);

    private final TransactionContext transactionContext;
    private final IncrementalSnapshotContext<CollectionId> incrementalSnapshotContext;

    public MongoDbOffsetContext(SourceInfo sourceInfo, TransactionContext transactionContext,
                                IncrementalSnapshotContext<CollectionId> incrementalSnapshotContext) {
        super(sourceInfo);
        this.transactionContext = transactionContext;
        this.incrementalSnapshotContext = incrementalSnapshotContext;
    }

    void startInitialSnapshot() {
        sourceInfo.startInitialSnapshot();
    }

    void stopInitialSnapshot() {
        sourceInfo.stopInitialSnapshot();
    }

    @Override
    public Map<String, ?> getOffset() {
        SourceInfo.Position position = sourceInfo.position();
        Map<String, Object> offset = Collect.hashMapOf(
                TIMESTAMP, position.getTime(),
                ORDER, position.getInc());
        if (isSnapshotRunning()) {
            offset.put(INITIAL_SYNC, true);
        }

        addSessionTxnIdToOffset(position, offset);
        addResumeTokenToOffset(position, offset);

        return isSnapshotRunning() ? offset : incrementalSnapshotContext.store(transactionContext.store(offset));
    }

    private Map<String, Object> addSessionTxnIdToOffset(SourceInfo.Position position, Map<String, Object> offset) {
        if (position.getChangeStreamSessionTxnId() != null) {
            offset.put(LSID, position.getChangeStreamSessionTxnId().lsid);
            offset.put(TXN_NUMBER, position.getChangeStreamSessionTxnId().txnNumber);
        }
        return offset;
    }

    private Map<String, Object> addResumeTokenToOffset(SourceInfo.Position position, Map<String, Object> offset) {
        position.getResumeToken().ifPresent(resumeToken -> offset.put(RESUME_TOKEN, resumeToken));
        return offset;
    }

    @Override
    public Schema getSourceInfoSchema() {
        return sourceInfo.schema();
    }

    @Override
    public boolean isSnapshotRunning() {
        return sourceInfo.isSnapshot() && sourceInfo.isSnapshotRunning();
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

    public SourceInfo sourceInfo() {
        return sourceInfo;
    }

    @Override
    public void event(DataCollectionId collectionId, Instant timestamp) {
        // Not used by the mongodb connector, see readEvent and oplogEvent
        throw new UnsupportedOperationException();
    }

    public void readEvent(CollectionId collectionId, Instant timestamp) {
        sourceInfo.collectionEvent(collectionId, 0L);
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
        if (data == null) {
            return null;
        }
        try {
            return ResumeTokens.fromBase64(data);
        }
        catch (Exception e) {
            LOGGER.info("Old resume token format detected, attempting to parse as string " + data);
            return ResumeTokens.fromData(data);
        }
    }

    public BsonTimestamp lastTimestamp() {
        return sourceInfo.lastTimestamp();
    }

    public BsonTimestamp lastTimestampOrTokenTime() {
        return Optional.of(lastResumeTokenDoc())
                .map(ResumeTokens::getTimestamp)
                .orElseGet(this::lastTimestamp);
    }

    public boolean hasOffset() {
        return sourceInfo.hasPosition();
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
            throw new DebeziumException("Source offset '" + key + "' parameter value " + obj + " could not be converted to an integer");
        }
    }

    public static MongoDbOffsetContext empty(MongoDbConnectorConfig connectorConfig) {
        return new MongoDbOffsetContext(
                new SourceInfo(connectorConfig),
                new TransactionContext(),
                new MongoDbIncrementalSnapshotContext<>(false));
    }

    public static class Loader implements OffsetContext.Loader<MongoDbOffsetContext> {

        private final MongoDbConnectorConfig connectorConfig;

        public Loader(MongoDbConnectorConfig connectorConfig) {
            this.connectorConfig = connectorConfig;
        }

        @Override
        public MongoDbOffsetContext load(Map<String, ?> offset) {
            var sourceInfo = new SourceInfo(connectorConfig);

            if (!booleanOffsetValue(offset, INITIAL_SYNC)) {
                var position = positionFromOffset(offset);
                sourceInfo.setPosition(position);
            }

            return new MongoDbOffsetContext(
                    sourceInfo,
                    new TransactionContext(),
                    MongoDbIncrementalSnapshotContext.load(offset, false));
        }

        private SourceInfo.Position positionFromOffset(Map<String, ?> offset) {
            int time = intOffsetValue(offset, TIMESTAMP);
            int order = intOffsetValue(offset, ORDER);
            long changeStreamTxnNumber = longOffsetValue(offset, TXN_NUMBER);
            String changeStreamLsid = stringOffsetValue(offset, LSID);
            SourceInfo.SessionTransactionId changeStreamTxnId = null;
            if (changeStreamLsid != null) {
                changeStreamTxnId = new SourceInfo.SessionTransactionId(changeStreamLsid, changeStreamTxnNumber);
            }
            String resumeToken = stringOffsetValue(offset, RESUME_TOKEN);

            return new SourceInfo.Position(new BsonTimestamp(time, order), changeStreamTxnId, resumeToken);
        }
    }

    @Override
    public String toString() {
        return "MongoDbOffsetContext [sourceInfo=" + sourceInfo + "]";
    }
}
