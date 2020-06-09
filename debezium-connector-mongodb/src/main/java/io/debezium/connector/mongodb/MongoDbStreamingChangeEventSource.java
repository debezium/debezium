/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.errors.ConnectException;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.CursorType;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;

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
public class MongoDbStreamingChangeEventSource implements StreamingChangeEventSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(MongoDbStreamingChangeEventSource.class);

    private static final String AUTHORIZATION_FAILURE_MESSAGE = "Command failed with error 13";

    private static final String OPERATION_FIELD = "op";
    private static final String OBJECT_FIELD = "o";
    private static final String OPERATION_CONTROL = "c";
    private static final String TX_OPS = "applyOps";

    private final EventDispatcher<CollectionId> dispatcher;
    private final ErrorHandler errorHandler;
    private final Clock clock;
    private final MongoDbOffsetContext offsetContext;
    private final ConnectionContext connectionContext;
    private final ReplicaSets replicaSets;
    private final MongoDbTaskContext taskContext;

    public MongoDbStreamingChangeEventSource(MongoDbConnectorConfig connectorConfig, MongoDbTaskContext taskContext,
                                             ReplicaSets replicaSets, MongoDbOffsetContext offsetContext,
                                             EventDispatcher<CollectionId> dispatcher, ErrorHandler errorHandler, Clock clock) {
        this.connectionContext = taskContext.getConnectionContext();
        this.dispatcher = dispatcher;
        this.errorHandler = errorHandler;
        this.clock = clock;
        this.replicaSets = replicaSets;
        this.taskContext = taskContext;
        this.offsetContext = (offsetContext != null) ? offsetContext : initializeOffsets(connectorConfig, replicaSets);
    }

    @Override
    public void execute(ChangeEventSourceContext context) throws InterruptedException {
        // Starts a thread for each replica-set and executes the streaming process
        final int threads = replicaSets.replicaSetCount();
        final ExecutorService executor = Threads.newFixedThreadPool(MongoDbConnector.class, taskContext.serverName(), "replicator-streaming", threads);
        final CountDownLatch latch = new CountDownLatch(threads);

        LOGGER.info("Starting {} thread(s) to stream changes for replica sets: {}", threads, replicaSets);
        replicaSets.validReplicaSets().forEach(replicaSet -> {
            executor.submit(() -> {
                MongoPrimary primaryClient = null;
                try {
                    primaryClient = establishConnectionToPrimary(replicaSet);
                    if (primaryClient != null) {
                        final AtomicReference<MongoPrimary> primaryReference = new AtomicReference<>(primaryClient);
                        primaryClient.execute("read from oplog on '" + replicaSet + "'", primary -> {
                            readOplog(primary, primaryReference.get(), replicaSet, context);
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

        // Shutdown the executor and cleanup connections
        try {
            executor.shutdown();
        }
        finally {
            taskContext.getConnectionContext().shutdown();
        }
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

    private void readOplog(MongoClient primary, MongoPrimary primaryClient, ReplicaSet replicaSet, ChangeEventSourceContext context) {
        final ReplicaSetOffsetContext rsOffsetContext = offsetContext.getReplicaSetOffsetContext(replicaSet);

        final BsonTimestamp oplogStart = rsOffsetContext.lastOffsetTimestamp();
        final OptionalLong txOrder = rsOffsetContext.lastOffsetTxOrder();

        final ServerAddress primaryAddress = primary.getAddress();
        LOGGER.info("Reading oplog for '{}' primary {} starting at {}", replicaSet, primaryAddress, oplogStart);

        // Include none of the cluster-internal operations and only those events since the previous timestamp
        MongoCollection<Document> oplog = primary.getDatabase("local").getCollection("oplog.rs");

        ReplicaSetOplogContext oplogContext = new ReplicaSetOplogContext(rsOffsetContext, primaryClient, replicaSet);

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

        Bson operationFilter = getSkippedOperationsFilter();
        if (operationFilter != null) {
            filter = Filters.and(filter, operationFilter);
        }

        final FindIterable<Document> results = oplog.find(filter)
                .sort(new Document("$natural", 1))
                .oplogReplay(true)
                .cursorType(CursorType.TailableAwait);

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
                        dispatcher.dispatchHeartbeatEvent(oplogContext.getOffset());
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

    private Bson getSkippedOperationsFilter() {
        Set<Operation> skippedOperations = taskContext.getConnectorConfig().getSkippedOps();

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
                        ServerAddress currentPrimary = mongoClient.getAddress();
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
                final Long operationId = event.getLong(SourceInfo.OPERATION_ID);
                dispatcher.dispatchTransactionStartedEvent(Long.toString(operationId), oplogContext.getOffset());
                for (Document change : txChanges) {
                    final boolean r = handleOplogEvent(primaryAddress, change, event, ++txOrder, oplogContext, context);
                    if (!r) {
                        return false;
                    }
                }
                dispatcher.dispatchTransactionCommittedEvent(oplogContext.getOffset());
            }
            catch (InterruptedException e) {
                LOGGER.error("Streaming transaction changes for replica set '{}' was interrupted", oplogContext.getReplicaSetName());
                throw new ConnectException("Streaming of transaction changes was interrupted for replica set " + oplogContext.getReplicaSetName(), e);
            }
            return true;
        }

        final String operation = event.getString(OPERATION_FIELD);
        if (!MongoDbChangeRecordEmitter.isValidOperation(operation)) {
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
                LOGGER.debug("Skipping the event for database {} based on database.whitelist", dbName);
                return true;
            }

            oplogContext.getOffset().oplogEvent(event, masterEvent, txOrder);
            oplogContext.getOffset().getOffset();

            CollectionId collectionId = new CollectionId(oplogContext.getReplicaSetName(), dbName, collectionName);
            if (taskContext.filters().collectionFilter().test(collectionId)) {
                try {
                    return dispatcher.dispatchDataChangeEvent(
                            collectionId,
                            new MongoDbChangeRecordEmitter(
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

        return new MongoDbOffsetContext(new SourceInfo(connectorConfig), new TransactionContext(), positions);
    }

    /**
     * A context associated with a given replica set oplog read operation.
     */
    private class ReplicaSetOplogContext {
        private final ReplicaSetOffsetContext offset;
        private final MongoPrimary primary;
        private final ReplicaSet replicaSet;

        private BsonTimestamp incompleteEventTimestamp;
        private long incompleteTxOrder = 0;

        ReplicaSetOplogContext(ReplicaSetOffsetContext offsetContext, MongoPrimary primary, ReplicaSet replicaSet) {
            this.offset = offsetContext;
            this.primary = primary;
            this.replicaSet = replicaSet;
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
