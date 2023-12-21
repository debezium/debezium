/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.bson.BsonDocument;

import io.debezium.connector.SnapshotRecord;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotContext;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.schema.DataCollectionId;

/**
 * A context that facilitates the management of the current offsets across a set of mongodb replica sets.
 *
 * @author Chris Cranford
 */
public class MongoDbOffsetContext implements OffsetContext {

    private final SourceInfo sourceInfo;
    private final TransactionContext transactionContext;
    private final Map<ReplicaSet, ReplicaSetPartition> replicaSetPartitions = new ConcurrentHashMap<>();
    private final Map<ReplicaSet, ReplicaSetOffsetContext> replicaSetOffsetContexts = new ConcurrentHashMap<>();
    private final IncrementalSnapshotContext<CollectionId> incrementalSnapshotContext;

    public MongoDbOffsetContext(SourceInfo sourceInfo, TransactionContext transactionContext,
                                IncrementalSnapshotContext<CollectionId> incrementalSnapshotContext) {
        this.sourceInfo = sourceInfo;
        this.transactionContext = transactionContext;
        this.incrementalSnapshotContext = incrementalSnapshotContext;
    }

    public MongoDbOffsetContext(SourceInfo sourceInfo, TransactionContext transactionContext,
                                IncrementalSnapshotContext<CollectionId> incrementalSnapshotContext, Map<ReplicaSet, BsonDocument> offsets) {
        this(sourceInfo, transactionContext, incrementalSnapshotContext);
        offsets.forEach((replicaSet, document) -> sourceInfo.opLogEvent(replicaSet.replicaSetName(), document, document, 0));
    }

    void startReplicaSetSnapshot(String replicaSetName) {
        sourceInfo.startInitialSync(replicaSetName);
    }

    void stopReplicaSetSnapshot(String replicaSetName) {
        sourceInfo.stopInitialSync(replicaSetName);
    }

    @Override
    public Map<String, ?> getOffset() {
        // Any common framework API that needs to call this function should be provided with a ReplicaSetOffsetContext
        throw new UnsupportedOperationException();
    }

    @Override
    public Schema getSourceInfoSchema() {
        return sourceInfo.schema();
    }

    @Override
    public Struct getSourceInfo() {
        return sourceInfo.struct();
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
    public void postSnapshotCompletion() {
        sourceInfo.setSnapshot(SnapshotRecord.FALSE);
    }

    @Override
    public void markLastSnapshotRecord() {
        sourceInfo.setSnapshot(SnapshotRecord.LAST);
    }

    @Override
    public TransactionContext getTransactionContext() {
        return transactionContext;
    }

    @Override
    public void incrementalSnapshotEvents() {
        sourceInfo.setSnapshot(SnapshotRecord.INCREMENTAL);
    }

    @Override
    public IncrementalSnapshotContext<?> getIncrementalSnapshotContext() {
        return incrementalSnapshotContext;
    }

    @Override
    public void event(DataCollectionId collectionId, Instant timestamp) {
        // Not used by the mongodb connector
        // see ReplicaSetOffsetContext readEvent and oplogEvent methods
        throw new UnsupportedOperationException();
    }

    public ReplicaSetPartition getReplicaSetPartition(ReplicaSet replicaSet) {
        return replicaSetPartitions.computeIfAbsent(replicaSet, rs -> new ReplicaSetPartition(sourceInfo.serverId(), rs.replicaSetName(), false, -1, -1));
    }

    /**
     * Get a {@link ReplicaSetPartition} instance for a given {@link ReplicaSet}.
     *
     * @param replicaSet the replica set; must not be null.
     * @return a replica set partition; never null.
     */
    public ReplicaSetPartition getReplicaSetPartition(ReplicaSet replicaSet, boolean multiTaskEnabled, int taskId, int multiTaskGen) {
        // here is where we configure the offset key
        return replicaSetPartitions.computeIfAbsent(replicaSet,
                rs -> new ReplicaSetPartition(sourceInfo.serverId(), rs.replicaSetName(), multiTaskEnabled, taskId, multiTaskGen));
    }

    /**
     * Get a {@link ReplicaSetOffsetContext} instance for a given {@link ReplicaSet}.
     *
     * @param replicaSet the replica set; must not be null.
     * @return a replica set offset context; never null.
     */
    public ReplicaSetOffsetContext getReplicaSetOffsetContext(ReplicaSet replicaSet) {
        return replicaSetOffsetContexts.computeIfAbsent(replicaSet, rs -> new ReplicaSetOffsetContext(this, rs, sourceInfo, incrementalSnapshotContext));
    }

    public static class Loader {

        private final ReplicaSets replicaSets;
        private final SourceInfo sourceInfo;

        public Loader(MongoDbConnectorConfig connectorConfig, ReplicaSets replicaSets, int taskId) {
            this.sourceInfo = new SourceInfo(connectorConfig, taskId);
            this.replicaSets = replicaSets;
        }

        public Collection<Map<String, String>> getPartitions() {
            // todo: DBZ-1726 - follow-up by removing partition management from SourceInfo
            final Collection<Map<String, String>> partitions = new ArrayList<>();
            replicaSets.onEachReplicaSet(replicaSet -> {
                final String name = replicaSet.replicaSetName(); // may be null for standalone servers
                if (name != null) {
                    Map<String, String> partition = sourceInfo.partition(name);
                    partitions.add(partition);
                }
            });
            return partitions;
        }

        public MongoDbOffsetContext loadOffsets(Map<Map<String, String>, Map<String, Object>> offsets) {
            // todo: DBZ-1726 - follow-up by removing offset management from SourceInfo
            offsets.forEach(sourceInfo::setOffsetFor);
            return new MongoDbOffsetContext(sourceInfo, new TransactionContext(),
                    MongoDbIncrementalSnapshotContext.load(offsets.values().iterator().next(), false));
        }
    }

    @Override
    public String toString() {
        return "MongoDbOffsetContext [sourceInfo=" + sourceInfo + "]";
    }
}
