/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.errors.ConnectException;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.CursorType;
import com.mongodb.ServerAddress;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;

import io.debezium.DebeziumException;
import io.debezium.connector.mongodb.ConnectionContext.MongoPrimary;
import io.debezium.data.Envelope.Operation;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;
import io.debezium.util.Threads;

/**
 *
 * @author Chris Cranford
 */
public class MongoDbStreamingChangeEventSource implements StreamingChangeEventSource<MongoDbPartition, MongoDbOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MongoDbStreamingChangeEventSource.class);

    private static final String AUTHORIZATION_FAILURE_MESSAGE = "Command failed with error 13";

    private static final String OPERATION_FIELD = "op";
    private static final String OBJECT_FIELD = "o";
    private static final String OPERATION_CONTROL = "c";
    private static final String TX_OPS = "applyOps";

    private final MongoDbConnectorConfig connectorConfig;
    private final EventDispatcher<CollectionId> dispatcher;
    private final ErrorHandler errorHandler;
    private final Clock clock;
    private final ConnectionContext connectionContext;
    private final ReplicaSets replicaSets;
    private final MongoDbTaskContext taskContext;

    public MongoDbStreamingChangeEventSource(MongoDbConnectorConfig connectorConfig, MongoDbTaskContext taskContext,
                                             ReplicaSets replicaSets,
                                             EventDispatcher<CollectionId> dispatcher, ErrorHandler errorHandler, Clock clock) {
        this.connectorConfig = connectorConfig;
        this.connectionContext = taskContext.getConnectionContext();
        this.dispatcher = dispatcher;
        this.errorHandler = errorHandler;
        this.clock = clock;
        this.replicaSets = replicaSets;
        this.taskContext = taskContext;
    }

    @Override
    public void execute(ChangeEventSourceContext context, MongoDbPartition partition, MongoDbOffsetContext offsetContext)
            throws InterruptedException {
        final List<ReplicaSet> validReplicaSets = replicaSets.validReplicaSets();

        if (offsetContext == null) {
            offsetContext = initializeOffsets(connectorConfig, replicaSets);
        }

        try {
            if (validReplicaSets.size() == 1) {
                // Streams the replica-set changes in the current thread
                streamChangesForReplicaSet(context, validReplicaSets.get(0), offsetContext);
            }
            else if (validReplicaSets.size() > 1) {
                // Starts a thread for each replica-set and executes the streaming process
                streamChangesForReplicaSets(context, validReplicaSets, offsetContext);
            }
        }
        finally {
            taskContext.getConnectionContext().shutdown();
        }
    }

    private void streamChangesForReplicaSet(ChangeEventSourceContext context, ReplicaSet replicaSet,
                                            MongoDbOffsetContext offsetContext) {
        MongoPrimary primaryClient = null;
        try {
            primaryClient = establishConnectionToPrimary(replicaSet);
            if (primaryClient != null) {
                final AtomicReference<MongoPrimary> primaryReference = new AtomicReference<>(primaryClient);
                primaryClient.execute("read from oplog on '" + replicaSet + "'", primary -> {
                    if (taskContext.getCaptureMode().isChangeStreams()) {
                        readChangeStream(primary, primaryReference.get(), replicaSet, context, offsetContext);
                    }
                    else {
                        readOplog(primary, primaryReference.get(), replicaSet, context, offsetContext);
                    }
                });
            }
        }
        catch (Throwable t) {
            LOGGER.error("Streaming for replica set {} failed", replicaSet.replicaSetName(), t);
            errorHandler.setProducerThrowable(t);
        }
        finally {
            if (primaryClient != null) {
                primaryClient.stop();
            }
        }
    }

    private void streamChangesForReplicaSets(ChangeEventSourceContext context, List<ReplicaSet> replicaSets,
                                             MongoDbOffsetContext offsetContext) {
        final int threads = replicaSets.size();
        final ExecutorService executor = Threads.newFixedThreadPool(MongoDbConnector.class, taskContext.serverName(), "replicator-streaming", threads);
        final CountDownLatch latch = new CountDownLatch(threads);

        LOGGER.info("Starting {} thread(s) to stream changes for replica sets: {}", threads, replicaSets);

        replicaSets.forEach(replicaSet -> {
            executor.submit(() -> {
                try {
                    streamChangesForReplicaSet(context, replicaSet, offsetContext);
                }
                finally {
                    latch.countDown();
                }
            });
        });

        // Wait for the executor service to terminate.
        try {
            latch.await();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        executor.shutdown();
    }

    private MongoPrimary establishConnectionToPrimary(ReplicaSet replicaSet) {
        return connectionContext.primaryFor(replicaSet, taskContext.filters(), (desc, error) -> {
            // propagate authorization failures
            if (error.getMessage() != null && error.getMessage().startsWith(AUTHORIZATION_FAILURE_MESSAGE)) {
                throw new ConnectException("Error while attempting to " + desc, error);
            }
            else {
                dispatcher.dispatchConnectorEvent(new DisconnectEvent());
                LOGGER.error("Error while attempting to {}: {}", desc, error.getMessage(), error);
                throw new ConnectException("Error while attempting to " + desc, error);
            }
        });
    }

    private void readOplog(MongoClient primary, MongoPrimary primaryClient, ReplicaSet replicaSet, ChangeEventSourceContext context,
                           MongoDbOffsetContext offsetContext) {
        final ReplicaSetPartition rsPartition = offsetContext.getReplicaSetPartition(replicaSet);
        final ReplicaSetOffsetContext rsOffsetContext = offsetContext.getReplicaSetOffsetContext(replicaSet);

        final BsonTimestamp oplogStart = rsOffsetContext.lastOffsetTimestamp();
        final OptionalLong txOrder = rsOffsetContext.lastOffsetTxOrder();

        final ServerAddress primaryAddress = MongoUtil.getPrimaryAddress(primary);
        LOGGER.info("Reading oplog for '{}' primary {} starting at {}", replicaSet, primaryAddress, oplogStart);

        // Include none of the cluster-internal operations and only those events since the previous timestamp
        MongoCollection<Document> oplog = primary.getDatabase("local").getCollection("oplog.rs");

        // DBZ-3331 Verify that the start position is in the oplog; throw exception if not.
        if (!isStartPositionInOplog(oplogStart, oplog)) {
            throw new DebeziumException("Failed to find starting position '" + oplogStart + "' in oplog");
        }

        ReplicaSetOplogContext oplogContext = new ReplicaSetOplogContext(rsPartition, rsOffsetContext, primaryClient, replicaSet);

        Bson filter = null;
        if (!txOrder.isPresent()) {
            LOGGER.info("The last event processed was not transactional, resuming at the oplog event after '{}'", oplogStart);
            filter = Filters.and(Filters.gt("ts", oplogStart), // start just after our last position
                    Filters.exists("fromMigrate", false)); // skip internal movements across shards
        }
        else {
            LOGGER.info("The last event processed was transactional, resuming at the oplog event '{}', expecting to skip '{}' events",
                    oplogStart, txOrder.getAsLong());
            filter = Filters.and(Filters.gte("ts", oplogStart), Filters.exists("fromMigrate", false));
            oplogContext.setIncompleteEventTimestamp(oplogStart);
            oplogContext.setIncompleteTxOrder(txOrder.getAsLong());
        }

        Bson operationFilter = getOplogSkippedOperationsFilter();
        if (operationFilter != null) {
            filter = Filters.and(filter, operationFilter);
        }

        FindIterable<Document> results = oplog.find(filter)
                .sort(new Document("$natural", 1))
                .oplogReplay(true)
                .cursorType(CursorType.TailableAwait)
                .noCursorTimeout(true);

        if (connectorConfig.getCursorMaxAwaitTime() > 0) {
            results = results.maxAwaitTime(connectorConfig.getCursorMaxAwaitTime(), TimeUnit.MILLISECONDS);
        }

        try (MongoCursor<Document> cursor = results.iterator()) {
            // In Replicator, this used cursor.hasNext() but this is a blocking call and I observed that this can
            // delay the shutdown of the connector by up to 15 seconds or longer. By introducing a Metronome, we
            // can respond to the stop request much faster and without much overhead.
            Metronome pause = Metronome.sleeper(Duration.ofMillis(500), clock);
            while (context.isRunning()) {
                // Use tryNext which will return null if no document is yet available from the cursor.
                // In this situation if not document is available, we'll pause.
                final Document event = cursor.tryNext();
                if (event != null) {
                    if (!handleOplogEvent(primaryAddress, event, event, 0, oplogContext, context)) {
                        // Something happened and we are supposed to stop reading
                        return;
                    }

                    try {
                        dispatcher.dispatchHeartbeatEvent(oplogContext.getPartition(), oplogContext.getOffset());
                    }
                    catch (InterruptedException e) {
                        LOGGER.info("Replicator thread is interrupted");
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
                else {
                    try {
                        pause.pause();
                    }
                    catch (InterruptedException e) {
                        break;
                    }
                }
            }
        }
    }

    private List<String> getChangeStreamSkippedOperationsFilter() {
        final Set<Operation> skippedOperations = taskContext.getConnectorConfig().getSkippedOperations();
        final List<String> includedOperations = new ArrayList<>();

        if (!skippedOperations.contains(Operation.CREATE)) {
            includedOperations.add("insert");
        }

        if (!skippedOperations.contains(Operation.UPDATE)) {
            // TODO Check that replace is tested
            includedOperations.add("update");
            includedOperations.add("replace");
        }
        if (!skippedOperations.contains(Operation.DELETE)) {
            includedOperations.add("delete");
        }
        return includedOperations;
    }

    private void readChangeStream(MongoClient primary, MongoPrimary primaryClient, ReplicaSet replicaSet, ChangeEventSourceContext context,
                                  MongoDbOffsetContext offsetContext) {
        final ReplicaSetPartition rsPartition = offsetContext.getReplicaSetPartition(replicaSet);
        final ReplicaSetOffsetContext rsOffsetContext = offsetContext.getReplicaSetOffsetContext(replicaSet);

        final BsonTimestamp oplogStart = rsOffsetContext.lastOffsetTimestamp();
        final OptionalLong txOrder = rsOffsetContext.lastOffsetTxOrder();

        ReplicaSetOplogContext oplogContext = new ReplicaSetOplogContext(rsPartition, rsOffsetContext, primaryClient, replicaSet);

        final ServerAddress primaryAddress = MongoUtil.getPrimaryAddress(primary);
        LOGGER.info("Reading change stream for '{}' primary {} starting at {}", replicaSet, primaryAddress, oplogStart);

        Bson filters = Filters.in("operationType", getChangeStreamSkippedOperationsFilter());
        if (rsOffsetContext.lastResumeToken() == null) {
            // After snapshot the oplogStart points to the last change snapshotted
            // It must be filtered-out
            filters = Filters.and(filters, Filters.ne("clusterTime", oplogStart));
        }
        final ChangeStreamIterable<Document> rsChangeStream = primary.watch(
                Arrays.asList(Aggregates.match(filters)));
        if (taskContext.getCaptureMode().isFullUpdate()) {
            rsChangeStream.fullDocument(FullDocument.UPDATE_LOOKUP);
        }
        if (rsOffsetContext.lastResumeToken() != null) {
            LOGGER.info("Resuming streaming from token '{}'", rsOffsetContext.lastResumeToken());

            final BsonDocument doc = new BsonDocument();
            doc.put("_data", new BsonString(rsOffsetContext.lastResumeToken()));
            rsChangeStream.resumeAfter(doc);
        }
        else {
            LOGGER.info("Resume token not available, starting streaming from time '{}'", oplogStart);
            rsChangeStream.startAtOperationTime(oplogStart);
        }

        if (connectorConfig.getCursorMaxAwaitTime() > 0) {
            rsChangeStream.maxAwaitTime(connectorConfig.getCursorMaxAwaitTime(), TimeUnit.MILLISECONDS);
        }

        try (MongoCursor<ChangeStreamDocument<Document>> cursor = rsChangeStream.iterator()) {
            // In Replicator, this used cursor.hasNext() but this is a blocking call and I observed that this can
            // delay the shutdown of the connector by up to 15 seconds or longer. By introducing a Metronome, we
            // can respond to the stop request much faster and without much overhead.
            Metronome pause = Metronome.sleeper(Duration.ofMillis(500), clock);
            while (context.isRunning()) {
                // Use tryNext which will return null if no document is yet available from the cursor.
                // In this situation if not document is available, we'll pause.
                final ChangeStreamDocument<Document> event = cursor.tryNext();
                if (event != null) {
                    LOGGER.trace("Arrived Change Stream event: {}", event);

                    if (!taskContext.filters().databaseFilter().test(event.getDatabaseName())) {
                        LOGGER.debug("Skipping the event for database '{}' based on database include/exclude list", event.getDatabaseName());
                    }
                    else {
                        oplogContext.getOffset().changeStreamEvent(event, txOrder);
                        oplogContext.getOffset().getOffset();
                        CollectionId collectionId = new CollectionId(
                                replicaSet.replicaSetName(),
                                event.getNamespace().getDatabaseName(),
                                event.getNamespace().getCollectionName());

                        if (taskContext.filters().collectionFilter().test(collectionId)) {
                            try {
                                dispatcher.dispatchDataChangeEvent(
                                        collectionId,
                                        new MongoDbChangeStreamChangeRecordEmitter(
                                                oplogContext.getPartition(),
                                                oplogContext.getOffset(),
                                                clock,
                                                event));
                            }
                            catch (Exception e) {
                                errorHandler.setProducerThrowable(e);
                                return;
                            }
                        }
                    }

                    try {
                        dispatcher.dispatchHeartbeatEvent(oplogContext.getPartition(), oplogContext.getOffset());
                    }
                    catch (InterruptedException e) {
                        LOGGER.info("Replicator thread is interrupted");
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
                else {
                    try {
                        pause.pause();
                    }
                    catch (InterruptedException e) {
                        break;
                    }
                }
            }
        }
    }

    private boolean isStartPositionInOplog(BsonTimestamp startTime, MongoCollection<Document> oplog) {
        final MongoCursor<Document> iterator = oplog.find().iterator();
        if (!iterator.hasNext()) {
            return false;
        }

        final BsonTimestamp timestamp = iterator.next().get("ts", BsonTimestamp.class);
        if (timestamp == null) {
            return false;
        }

        return timestamp.compareTo(startTime) <= 0;
    }

    private Bson getOplogSkippedOperationsFilter() {
        Set<Operation> skippedOperations = taskContext.getConnectorConfig().getSkippedOperations();

        if (skippedOperations.isEmpty()) {
            return null;
        }

        Bson skippedOperationsFilter = null;

        for (Operation operation : skippedOperations) {
            Bson skippedOperationFilter = Filters.ne("op", operation.code());

            if (skippedOperationsFilter == null) {
                skippedOperationsFilter = skippedOperationFilter;
            }
            else {
                skippedOperationsFilter = Filters.or(skippedOperationsFilter, skippedOperationFilter);
            }
        }

        return skippedOperationsFilter;
    }

    private boolean handleOplogEvent(ServerAddress primaryAddress, Document event, Document masterEvent, long txOrder, ReplicaSetOplogContext oplogContext,
                                     ChangeEventSourceContext context) {
        String ns = event.getString("ns");
        Document object = event.get(OBJECT_FIELD, Document.class);
        if (Objects.isNull(object)) {
            if (LOGGER.isWarnEnabled()) {
                LOGGER.warn("Missing 'o' field in event, so skipping {}", event.toJson());
            }
            return true;
        }

        if (Objects.isNull(ns) || ns.isEmpty()) {
            // These are considered replica set events
            String msg = object.getString("msg");
            if ("new primary".equals(msg)) {
                AtomicReference<ServerAddress> address = new AtomicReference<>();
                try {
                    oplogContext.getPrimary().executeBlocking("conn", mongoClient -> {
                        ServerAddress currentPrimary = MongoUtil.getPrimaryAddress(mongoClient);
                        address.set(currentPrimary);
                    });
                }
                catch (InterruptedException e) {
                    LOGGER.error("Get current primary executeBlocking", e);
                }

                ServerAddress serverAddress = address.get();
                if (Objects.nonNull(serverAddress) && !serverAddress.equals(primaryAddress)) {
                    LOGGER.info("Found new primary event in oplog, so stopping use of {} to continue with new primary {}",
                            primaryAddress, serverAddress);
                }
                else {
                    LOGGER.info("Found new primary event in oplog, current {} is new primary. " +
                            "Continue to process oplog event.", primaryAddress);
                }

                dispatcher.dispatchConnectorEvent(new PrimaryElectionEvent(serverAddress));
            }
            // Otherwise ignore
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Skipping event with no namespace: {}", event.toJson());
            }
            return true;
        }

        final List<Document> txChanges = transactionChanges(event);
        if (!txChanges.isEmpty()) {
            if (Objects.nonNull(oplogContext.getIncompleteEventTimestamp())) {
                if (oplogContext.getIncompleteEventTimestamp().equals(SourceInfo.extractEventTimestamp(event))) {
                    for (Document change : txChanges) {
                        txOrder++;
                        if (txOrder <= oplogContext.getIncompleteTxOrder()) {
                            LOGGER.debug("Skipping record as it is expected to be already processed: {}", change);
                            continue;
                        }
                        final boolean r = handleOplogEvent(primaryAddress, change, event, txOrder, oplogContext, context);
                        if (!r) {
                            return false;
                        }
                    }
                }
                oplogContext.setIncompleteEventTimestamp(null);
                return true;
            }
            try {
                dispatcher.dispatchTransactionStartedEvent(oplogContext.getPartition(), getTransactionId(event), oplogContext.getOffset());
                for (Document change : txChanges) {
                    final boolean r = handleOplogEvent(primaryAddress, change, event, ++txOrder, oplogContext, context);
                    if (!r) {
                        return false;
                    }
                }
                dispatcher.dispatchTransactionCommittedEvent(oplogContext.getPartition(), oplogContext.getOffset());
            }
            catch (InterruptedException e) {
                LOGGER.error("Streaming transaction changes for replica set '{}' was interrupted", oplogContext.getReplicaSetName());
                throw new ConnectException("Streaming of transaction changes was interrupted for replica set " + oplogContext.getReplicaSetName(), e);
            }
            return true;
        }

        final String operation = event.getString(OPERATION_FIELD);
        if (!MongoDbChangeSnapshotOplogRecordEmitter.isValidOperation(operation)) {
            LOGGER.debug("Skipping event with \"op={}\"", operation);
            return true;
        }

        int delimIndex = ns.indexOf('.');
        if (delimIndex > 0) {
            assert (delimIndex + 1) < ns.length();

            final String dbName = ns.substring(0, delimIndex);
            final String collectionName = ns.substring(delimIndex + 1);
            if ("$cmd".equals(collectionName)) {
                // This is a command on the database
                // TODO: Probably want to handle some of these when we track creation/removal of collections
                LOGGER.debug("Skipping database command event: {}", event.toJson());
                return true;
            }

            // Otherwise it is an event on a document in a collection
            if (!taskContext.filters().databaseFilter().test(dbName)) {
                LOGGER.debug("Skipping the event for database '{}' based on database include/exclude list", dbName);
                return true;
            }

            oplogContext.getOffset().oplogEvent(event, masterEvent, txOrder);
            oplogContext.getOffset().getOffset();

            CollectionId collectionId = new CollectionId(oplogContext.getReplicaSetName(), dbName, collectionName);
            if (taskContext.filters().collectionFilter().test(collectionId)) {
                try {
                    return dispatcher.dispatchDataChangeEvent(
                            collectionId,
                            new MongoDbChangeSnapshotOplogRecordEmitter(
                                    oplogContext.getPartition(),
                                    oplogContext.getOffset(),
                                    clock,
                                    event,
                                    false));
                }
                catch (Exception e) {
                    errorHandler.setProducerThrowable(e);
                    return false;
                }
            }
        }

        return true;
    }

    private List<Document> transactionChanges(Document event) {
        final String op = event.getString(OPERATION_FIELD);
        final Document o = event.get(OBJECT_FIELD, Document.class);
        if (!(OPERATION_CONTROL.equals(op) && Objects.nonNull(o) && o.containsKey(TX_OPS))) {
            return Collections.emptyList();
        }
        return o.get(TX_OPS, List.class);
    }

    protected MongoDbOffsetContext initializeOffsets(MongoDbConnectorConfig connectorConfig, ReplicaSets replicaSets) {
        final Map<ReplicaSet, Document> positions = new LinkedHashMap<>();
        replicaSets.onEachReplicaSet(replicaSet -> {
            LOGGER.info("Determine Snapshot Offset for replica-set {}", replicaSet.replicaSetName());
            MongoPrimary primaryClient = establishConnectionToPrimary(replicaSet);
            if (primaryClient != null) {
                try {
                    primaryClient.execute("get oplog position", primary -> {
                        MongoCollection<Document> oplog = primary.getDatabase("local").getCollection("oplog.rs");
                        Document last = oplog.find().sort(new Document("$natural", -1)).limit(1).first(); // may be null
                        positions.put(replicaSet, last);
                    });
                }
                finally {
                    LOGGER.info("Stopping primary client");
                    primaryClient.stop();
                }
            }
        });

        return new MongoDbOffsetContext(new SourceInfo(connectorConfig), new TransactionContext(),
                new MongoDbIncrementalSnapshotContext<>(false), positions);
    }

    private static String getTransactionId(Document event) {
        final Long operationId = event.getLong(SourceInfo.OPERATION_ID);
        if (operationId != null && operationId != 0L) {
            return Long.toString(operationId);
        }

        return MongoUtil.getOplogSessionTransactionId(event);
    }

    /**
     * A context associated with a given replica set oplog read operation.
     */
    private class ReplicaSetOplogContext {
        private final ReplicaSetPartition partition;
        private final ReplicaSetOffsetContext offset;
        private final MongoPrimary primary;
        private final ReplicaSet replicaSet;

        private BsonTimestamp incompleteEventTimestamp;
        private long incompleteTxOrder = 0;

        ReplicaSetOplogContext(ReplicaSetPartition partition, ReplicaSetOffsetContext offsetContext,
                               MongoPrimary primary, ReplicaSet replicaSet) {
            this.partition = partition;
            this.offset = offsetContext;
            this.primary = primary;
            this.replicaSet = replicaSet;
        }

        ReplicaSetPartition getPartition() {
            return partition;
        }

        ReplicaSetOffsetContext getOffset() {
            return offset;
        }

        MongoPrimary getPrimary() {
            return primary;
        }

        String getReplicaSetName() {
            return replicaSet.replicaSetName();
        }

        BsonTimestamp getIncompleteEventTimestamp() {
            return incompleteEventTimestamp;
        }

        public void setIncompleteEventTimestamp(BsonTimestamp incompleteEventTimestamp) {
            this.incompleteEventTimestamp = incompleteEventTimestamp;
        }

        public long getIncompleteTxOrder() {
            return incompleteTxOrder;
        }

        public void setIncompleteTxOrder(long incompleteTxOrder) {
            this.incompleteTxOrder = incompleteTxOrder;
        }
    }
}
