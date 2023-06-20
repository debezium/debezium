/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Projections;

import io.debezium.DebeziumException;
import io.debezium.annotation.NotThreadSafe;
import io.debezium.connector.mongodb.ConnectionContext.MongoPrimary;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.AbstractSnapshotChangeEventSource;
import io.debezium.pipeline.source.snapshot.incremental.CloseIncrementalSnapshotWindow;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotChangeEventSource;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotContext;
import io.debezium.pipeline.source.snapshot.incremental.OpenIncrementalSnapshotWindow;
import io.debezium.pipeline.source.spi.DataChangeEventListener;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.ChangeRecordEmitter;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.schema.DataCollectionId;
import io.debezium.util.Clock;
import io.debezium.util.Strings;
import io.debezium.util.Threads;
import io.debezium.util.Threads.Timer;

/**
 * An incremental snapshot change event source that emits events from a MongoDB change stream interleaved with snapshot events.
 */
@NotThreadSafe
public class MongoDbIncrementalSnapshotChangeEventSource
        implements IncrementalSnapshotChangeEventSource<MongoDbPartition, CollectionId> {

    private static final String DOCUMENT_ID = "_id";
    private static final Logger LOGGER = LoggerFactory.getLogger(MongoDbIncrementalSnapshotChangeEventSource.class);
    private static final String AUTHORIZATION_FAILURE_MESSAGE = "Command failed with error 13";

    private final MongoDbConnectorConfig connectorConfig;
    private final Clock clock;
    private final MongoDbSchema collectionSchema;
    private final SnapshotProgressListener<MongoDbPartition> progressListener;
    private final DataChangeEventListener<MongoDbPartition> dataListener;
    private long totalRowsScanned = 0;
    private final ReplicaSets replicaSets;
    private final ConnectionContext connectionContext;
    private final MongoDbTaskContext taskContext;

    private MongoDbCollectionSchema currentCollection;

    protected EventDispatcher<MongoDbPartition, CollectionId> dispatcher;
    protected IncrementalSnapshotContext<CollectionId> context = null;
    protected final Map<Struct, Object[]> window = new ConcurrentHashMap<>();
    private MongoPrimary primary;
    private CollectionId signallingCollectionId;

    private final ExecutorService incrementalSnapshotThreadPool;

    public MongoDbIncrementalSnapshotChangeEventSource(MongoDbConnectorConfig config,
                                                       MongoDbTaskContext taskContext,
                                                       ReplicaSets replicaSets,
                                                       EventDispatcher<MongoDbPartition, CollectionId> dispatcher,
                                                       MongoDbSchema collectionSchema,
                                                       Clock clock,
                                                       SnapshotProgressListener<MongoDbPartition> progressListener,
                                                       DataChangeEventListener<MongoDbPartition> dataChangeEventListener) {
        this.connectorConfig = config;
        this.replicaSets = replicaSets;
        this.taskContext = taskContext;
        this.connectionContext = taskContext.getConnectionContext();
        this.dispatcher = dispatcher;
        this.collectionSchema = collectionSchema;
        this.clock = clock;
        this.progressListener = progressListener;
        this.dataListener = dataChangeEventListener;
        this.signallingCollectionId = connectorConfig.getSignalingDataCollectionId() == null ? null
                : CollectionId.parse("UNUSED", connectorConfig.getSignalingDataCollectionId());
        this.incrementalSnapshotThreadPool = Threads.newFixedThreadPool(MongoDbConnector.class, taskContext.serverName(),
                "incremental-snapshot-" + replicaSets.all().get(0).replicaSetName(), connectorConfig.getIncrementalSnapshotThreads());
    }

    @Override
    @SuppressWarnings("unchecked")
    public void closeWindow(MongoDbPartition partition, String id, OffsetContext offsetContext) throws InterruptedException {
        context = (IncrementalSnapshotContext<CollectionId>) offsetContext.getIncrementalSnapshotContext();
        if (!context.closeWindow(id)) {
            return;
        }
        sendWindowEvents(partition, offsetContext);
        readChunk(partition);
    }

    protected String getSignalCollectionName(String dataCollectionId) {
        return dataCollectionId;
    }

    protected void sendWindowEvents(MongoDbPartition partition, OffsetContext offsetContext) throws InterruptedException {
        LOGGER.debug("Sending {} events from window buffer", window.size());
        offsetContext.incrementalSnapshotEvents();
        for (Object[] row : window.values()) {
            sendEvent(partition, dispatcher, offsetContext, row);
        }
        offsetContext.postSnapshotCompletion();
        window.clear();
    }

    // TODO Used typed dispatcher and offset context
    protected void sendEvent(MongoDbPartition partition, EventDispatcher<MongoDbPartition, CollectionId> dispatcher, OffsetContext offsetContext, Object[] row)
            throws InterruptedException {
        context.sendEvent(keyFromRow(row));
        ((ReplicaSetOffsetContext) offsetContext).readEvent(context.currentDataCollectionId(), clock.currentTimeAsInstant());
        dispatcher.dispatchSnapshotEvent(partition, context.currentDataCollectionId(),
                getChangeRecordEmitter(partition, offsetContext, row),
                dispatcher.getIncrementalSnapshotChangeEventReceiver(dataListener));
    }

    /**
     * Returns a {@link ChangeRecordEmitter} producing the change records for
     * the given table row.
     */
    protected ChangeRecordEmitter<MongoDbPartition> getChangeRecordEmitter(MongoDbPartition partition,
                                                                           OffsetContext offsetContext, Object[] row) {
        return new MongoDbChangeSnapshotOplogRecordEmitter(partition, offsetContext, clock, (BsonDocument) row[0], true, connectorConfig.getEnableRawOplog());
    }

    protected void deduplicateWindow(DataCollectionId dataCollectionId, Object key) {
        if (!context.currentDataCollectionId().equals(dataCollectionId)) {
            return;
        }
        if (key instanceof Struct) {
            if (window.remove((Struct) key) != null) {
                LOGGER.info("Removed '{}' from window", key);
            }
        }
    }

    /**
     * Update low watermark for the incremental snapshot chunk
     */
    protected void emitWindowOpen() throws InterruptedException {
        final CollectionId collectionId = signallingCollectionId;
        final String id = context.currentChunkId() + "-open";
        primary.executeBlocking(
                "emit window open for chunk '" + context.currentChunkId() + "'",
                primary -> {
                    final MongoDatabase database = primary.getDatabase(collectionId.dbName());
                    final MongoCollection<Document> collection = database.getCollection(collectionId.name());

                    LOGGER.trace("Emitting open window for chunk = '{}'", context.currentChunkId());
                    final Document signal = new Document();
                    signal.put(DOCUMENT_ID, id);
                    signal.put("type", OpenIncrementalSnapshotWindow.NAME);
                    signal.put("payload", "");
                    collection.insertOne(signal);
                });
    }

    /**
     * Update high watermark for the incremental snapshot chunk
     */
    protected void emitWindowClose() throws InterruptedException {
        final CollectionId collectionId = signallingCollectionId;
        final String id = context.currentChunkId() + "-close";
        primary.executeBlocking(
                "emit window close for chunk '" + context.currentChunkId() + "'",
                primary -> {
                    final MongoDatabase database = primary.getDatabase(collectionId.dbName());
                    final MongoCollection<Document> collection = database.getCollection(collectionId.name());

                    LOGGER.trace("Emitting close window for chunk = '{}'", context.currentChunkId());
                    final Document signal = new Document();
                    signal.put(DOCUMENT_ID, id);
                    signal.put("type", CloseIncrementalSnapshotWindow.NAME);
                    signal.put("payload", "");
                    collection.insertOne(signal);
                });
    }

    @Override
    @SuppressWarnings("unchecked")
    public void init(MongoDbPartition partition, OffsetContext offsetContext) {
        primary = establishConnectionToPrimary(partition, replicaSets.all().get(0));
        if (offsetContext == null) {
            LOGGER.info("Empty incremental snapshot change event source started, no action needed");
            postIncrementalSnapshotCompleted();
            return;
        }
        context = (IncrementalSnapshotContext<CollectionId>) offsetContext.getIncrementalSnapshotContext();
        if (!context.snapshotRunning()) {
            LOGGER.info("No incremental snapshot in progress, no action needed on start");
            postIncrementalSnapshotCompleted();
            return;
        }
        LOGGER.info("Incremental snapshot in progress, need to read new chunk on start");
        try {
            progressListener.snapshotStarted(partition);
            readChunk(partition);
        }
        catch (InterruptedException e) {
            throw new DebeziumException("Reading of an initial chunk after connector restart has been interrupted");
        }
        LOGGER.info("Incremental snapshot in progress, loading of initial chunk completed");
    }

    protected void readChunk(MongoDbPartition partition) throws InterruptedException {
        if (!context.snapshotRunning()) {
            LOGGER.info("Skipping read chunk because snapshot is not running");
            postIncrementalSnapshotCompleted();
            return;
        }
        try {
            preReadChunk(context);
            context.startNewChunk();
            emitWindowOpen();
            while (context.snapshotRunning()) {
                final CollectionId currentDataCollectionId = context.currentDataCollectionId();
                currentCollection = (MongoDbCollectionSchema) collectionSchema.schemaFor(currentDataCollectionId);
                if (replicaSets.all().size() > 1) {
                    LOGGER.warn("Incremental snapshotting supported only for single result set topology, skipping collection '{}', known collections {}",
                            currentDataCollectionId);
                    nextDataCollection(partition);
                    continue;
                }
                // TODO Collection schema is calculated dynamically, it is necessary to use a different check
                if (currentCollection == null) {
                    LOGGER.warn("Schema not found for collection '{}', known collections {}", currentDataCollectionId, collectionSchema);
                    nextDataCollection(partition);
                    continue;
                }
                // MongoDB collection has always key so it is not necessary to check if it is available
                if (!context.maximumKey().isPresent()) {
                    context.maximumKey(readMaximumKey());
                    if (!context.maximumKey().isPresent()) {
                        LOGGER.info(
                                "No maximum key returned by the query, incremental snapshotting of collection '{}' finished as it is empty",
                                currentDataCollectionId);
                        nextDataCollection(partition);
                        continue;
                    }
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("Incremental snapshot for collection '{}' will end at position {}", currentDataCollectionId,
                                context.maximumKey().orElse(new Object[0]));
                    }
                }
                createDataEventsForDataCollection(partition);
                if (window.isEmpty()) {
                    LOGGER.info("No data returned by the query, incremental snapshotting of table '{}' finished",
                            currentDataCollectionId);
                    collectionScanCompleted(partition);
                    nextDataCollection(partition);
                }
                else {
                    break;
                }
            }
            emitWindowClose();
        }
        // catch (InterruptedException e) {
        // throw new DebeziumException(String.format("Database error while executing incremental snapshot for table '%s'", context.currentDataCollectionId()), e);
        // }
        finally {
            postReadChunk(context);
            if (!context.snapshotRunning()) {
                postIncrementalSnapshotCompleted();
            }
        }
    }

    private void nextDataCollection(MongoDbPartition partition) {
        context.nextDataCollection();
        if (!context.snapshotRunning()) {
            progressListener.snapshotCompleted(partition);
        }
    }

    private Object[] readMaximumKey() throws InterruptedException {
        final CollectionId collectionId = (CollectionId) currentCollection.id();
        final AtomicReference<Object> key = new AtomicReference<>();
        primary.executeBlocking("maximum key for '" + collectionId + "'", primary -> {
            final MongoDatabase database = primary.getDatabase(collectionId.dbName());
            final MongoCollection<Document> collection = database.getCollection(collectionId.name());

            final Document lastDocument = collection.find().sort(new Document(DOCUMENT_ID, -1)).limit(1).first();
            if (lastDocument != null) {
                key.set(lastDocument.get(DOCUMENT_ID));
            }
        });
        return key.get() != null ? new Object[]{ key.get() } : null;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void addDataCollectionNamesToSnapshot(MongoDbPartition partition, List<String> dataCollectionIds, OffsetContext offsetContext) throws InterruptedException {
        context = (IncrementalSnapshotContext<CollectionId>) offsetContext.getIncrementalSnapshotContext();
        final boolean shouldReadChunk = !context.snapshotRunning();
        final String rsName = replicaSets.all().get(0).replicaSetName();
        dataCollectionIds = dataCollectionIds
                .stream()
                .map(x -> rsName + "." + x)
                .collect(Collectors.toList());
        final List<CollectionId> newDataCollectionIds = context.addDataCollectionNamesToSnapshot(dataCollectionIds);
        if (shouldReadChunk) {
            progressListener.snapshotStarted(partition);
            progressListener.monitoredDataCollectionsDetermined(partition, newDataCollectionIds);
            readChunk(partition);
        }
    }

    /**
     * Dispatches the data change events for the records of a single table.
     */
    private void createDataEventsForDataCollection(MongoDbPartition partition) throws InterruptedException {
        final CollectionId collectionId = (CollectionId) currentCollection.id();
        long exportStart = clock.currentTimeInMillis();
        LOGGER.debug("Exporting data chunk from collection '{}' (total {} collections)", currentCollection.id(), context.dataCollectionsToBeSnapshottedCount());

        primary.executeBlocking("chunk query key for '" + currentCollection.id() + "'", primary -> {
            final int theads = connectorConfig.getIncrementalSnapshotThreads();
            final int chunkSize = connectorConfig.getIncrementalSnashotChunkSize();

            final MongoDatabase database = primary.getDatabase(collectionId.dbName());
            final MongoCollection<BsonDocument> collection = database.getCollection(collectionId.name(), BsonDocument.class);

            Document predicate = constructQueryPredicate(context.chunkEndPosititon(), context.maximumKey().get());
            LOGGER.debug("\t For collection '{}' using query: '{}', key: '{}', maximum key: '{}' to get all _id fields", currentCollection.id(),
                    predicate.toJson(), context.chunkEndPosititon(), context.maximumKey().get());

            long rows = 0;
            Object[] lastRow = null;
            Object[] firstRow = null;
            List<Future<?>> futureList = new ArrayList<>();
            Object[] lastChunkKey = context.chunkEndPosititon();

            for (BsonDocument doc : collection.find(predicate).sort(new Document(DOCUMENT_ID, 1)).projection(Projections.include(DOCUMENT_ID))
                    .limit(chunkSize * theads)) {
                rows++;
                final Object[] row = new Object[]{ doc };
                if (firstRow == null) {
                    firstRow = row;
                }
                lastRow = row;

                if (rows % chunkSize == 0) {
                    final Object[] chunkStartKey = lastChunkKey;
                    final Object[] chunkEndKey = keyFromRow(lastRow);
                    futureList.add(this.incrementalSnapshotThreadPool.submit(() -> {
                        queryChunk(collection, chunkStartKey, chunkEndKey);
                    }));
                    lastChunkKey = chunkEndKey;
                }
            }

            // in case the last iteration doesn't have enough data, do it once again for the rest of the rows
            if (rows % chunkSize != 0) {
                final Object[] chunkStartKey = lastChunkKey;
                final Object[] chunkEndKey = keyFromRow(lastRow);
                futureList.add(this.incrementalSnapshotThreadPool.submit(() -> {
                    queryChunk(collection, chunkStartKey, chunkEndKey);
                }));
            }

            try {
                for (Future<?> future : futureList) {
                    future.get(); // Wait for the tasks to complete
                }
            }
            catch (InterruptedException | ExecutionException e) {
                throw new InterruptedException(e.toString());
            }

            final Object[] firstKey = keyFromRow(firstRow);
            final Object[] lastKey = keyFromRow(lastRow);
            if (context.isNonInitialChunk()) {
                progressListener.currentChunk(partition, context.currentChunkId(), firstKey, lastKey);
            }
            else {
                progressListener.currentChunk(partition, context.currentChunkId(), firstKey, lastKey, context.maximumKey().orElse(null));
            }
            context.nextChunkPosition(lastKey);
            if (lastRow != null) {
                LOGGER.debug("\t Next window will resume from {}", (Object) context.chunkEndPosititon());
            }

            LOGGER.debug("\t Finished exporting {} records for window of collection '{}'; total duration '{}'", rows,
                    currentCollection.id(), Strings.duration(clock.currentTimeInMillis() - exportStart));
            incrementTableRowsScanned(rows);
        });
    }

    private void queryChunk(MongoCollection<BsonDocument> collection, Object[] startKey, Object[] endKey) {
        Document predicate = constructQueryPredicate(startKey, endKey);
        LOGGER.debug("\t For collection chunk, '{}' using query: '{}', key: '{}', maximum key: '{}'", currentCollection.id(),
                predicate.toJson(), startKey, endKey);

        long rows = 0;
        long exportStart = clock.currentTimeInMillis();
        Timer logTimer = getTableScanLogTimer();

        for (BsonDocument doc : collection.find(predicate).sort(new Document(DOCUMENT_ID, 1))) {
            rows++;
            final Object[] row = new Object[]{ doc };
            final Struct keyStruct = currentCollection.keyFromDocumentOplog(doc);
            window.put(keyStruct, row);
            if (logTimer.expired()) {
                long stop = clock.currentTimeInMillis();
                LOGGER.debug("\t Exported {} records for collection '{}' after {}", rows, currentCollection.id(),
                        Strings.duration(stop - exportStart));
                logTimer = getTableScanLogTimer();
            }
        }
    }

    private Document constructQueryPredicate(Object[] startKey, Object[] endKey) {
        final Document maxKeyPredicate = new Document();
        final Document maxKeyOp = new Document();

        if (endKey != null) {
            maxKeyOp.put("$lte", endKey[0]);
            maxKeyPredicate.put(DOCUMENT_ID, maxKeyOp);
        }

        Document predicate = maxKeyPredicate;

        if (startKey != null) {
            final Document chunkEndPredicate = new Document();
            final Document chunkEndOp = new Document();
            chunkEndOp.put("$gt", startKey[0]);
            chunkEndPredicate.put(DOCUMENT_ID, chunkEndOp);
            predicate = new Document();
            predicate.put("$and", Arrays.asList(chunkEndPredicate, maxKeyPredicate));
        }

        return predicate;
    }

    private void incrementTableRowsScanned(long rows) {
        totalRowsScanned += rows;
        // TODO This metric is not provided by MongoDB
        // progressListener.rowsScanned(currentCollection.id(), totalRowsScanned);
    }

    private void collectionScanCompleted(MongoDbPartition partition) {
        progressListener.dataCollectionSnapshotCompleted(partition, currentCollection.id(), totalRowsScanned);
        totalRowsScanned = 0;
        // Reset chunk/table information in metrics
        progressListener.currentChunk(null, null, null, null);
    }

    private Timer getTableScanLogTimer() {
        return Threads.timer(clock, AbstractSnapshotChangeEventSource.LOG_INTERVAL);
    }

    private Object[] keyFromRow(Object[] row) {
        if (row == null) {
            return null;
        }
        BsonValue documentId = ((BsonDocument) row[0]).get(DOCUMENT_ID);

        Object key;

        switch (documentId.getBsonType()) {
            case DOUBLE:
                key = documentId.asDouble().getValue();
                break;
            case INT32:
                key = documentId.asInt32().getValue();
                break;
            case INT64:
                key = documentId.asInt64().getValue();
                break;
            case DECIMAL128:
                key = documentId.asDecimal128().getValue();
                break;
            case OBJECT_ID:
                key = documentId.asObjectId().getValue();
                break;
            case STRING:
                key = documentId.asString().getValue();
                break;
            default:
                throw new IllegalStateException("Unsupported type of document id");
        }

        return new Object[]{ key };
    }

    protected void setContext(IncrementalSnapshotContext<CollectionId> context) {
        this.context = context;
    }

    protected void preReadChunk(IncrementalSnapshotContext<CollectionId> context) {
    }

    protected void postReadChunk(IncrementalSnapshotContext<CollectionId> context) {
        // no-op
    }

    protected void postIncrementalSnapshotCompleted() {
        // no-op
    }

    @Override
    public void processMessage(MongoDbPartition partition, DataCollectionId dataCollectionId, Object key,
                               OffsetContext offsetContext)
            throws InterruptedException {
        context = (IncrementalSnapshotContext<CollectionId>) offsetContext.getIncrementalSnapshotContext();
        if (context == null) {
            LOGGER.warn("Context is null, skipping message processing");
            return;
        }
        LOGGER.trace("Checking window for table '{}', key '{}', window contains '{}'", dataCollectionId, key, window);
        if (!window.isEmpty() && context.deduplicationNeeded()) {
            deduplicateWindow(dataCollectionId, key);
        }
    }

    private MongoPrimary establishConnectionToPrimary(MongoDbPartition partition, ReplicaSet replicaSet) {
        return connectionContext.primaryFor(replicaSet, taskContext.filters(), (desc, error) -> {
            // propagate authorization failures
            if (error.getMessage() != null && error.getMessage().startsWith(AUTHORIZATION_FAILURE_MESSAGE)) {
                throw new ConnectException("Error while attempting to " + desc, error);
            }
            else {
                dispatcher.dispatchConnectorEvent(partition, new DisconnectEvent());
                LOGGER.error("Error while attempting to {}: {}", desc, error.getMessage(), error);
                throw new ConnectException("Error while attempting to " + desc, error);
            }
        });
    }
}
