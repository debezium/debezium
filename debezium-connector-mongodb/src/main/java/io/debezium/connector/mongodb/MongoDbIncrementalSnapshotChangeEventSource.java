/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static io.debezium.pipeline.notification.IncrementalSnapshotNotificationService.TableScanCompletionStatus.EMPTY;
import static io.debezium.pipeline.notification.IncrementalSnapshotNotificationService.TableScanCompletionStatus.SKIPPED;
import static io.debezium.pipeline.notification.IncrementalSnapshotNotificationService.TableScanCompletionStatus.SUCCEEDED;
import static io.debezium.pipeline.notification.IncrementalSnapshotNotificationService.TableScanCompletionStatus.UNKNOWN_SCHEMA;

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
import org.bson.BsonDocument;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Projections;

import io.debezium.DebeziumException;
import io.debezium.annotation.NotThreadSafe;
import io.debezium.connector.mongodb.connection.MongoDbConnection;
import io.debezium.connector.mongodb.connection.ReplicaSet;
import io.debezium.connector.mongodb.recordemitter.MongoDbSnapshotRecordEmitter;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.signal.SignalPayload;
import io.debezium.pipeline.signal.actions.snapshotting.CloseIncrementalSnapshotWindow;
import io.debezium.pipeline.signal.actions.snapshotting.OpenIncrementalSnapshotWindow;
import io.debezium.pipeline.signal.actions.snapshotting.SnapshotConfiguration;
import io.debezium.pipeline.source.AbstractSnapshotChangeEventSource;
import io.debezium.pipeline.source.snapshot.incremental.DataCollection;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotChangeEventSource;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotContext;
import io.debezium.pipeline.source.spi.DataChangeEventListener;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.ChangeRecordEmitter;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.spi.schema.DataCollectionId;
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

    private final MongoDbConnectorConfig connectorConfig;
    private final Clock clock;
    private final MongoDbSchema collectionSchema;
    private final SnapshotProgressListener<MongoDbPartition> progressListener;
    private final DataChangeEventListener<MongoDbPartition> dataListener;
    private long totalRowsScanned = 0;
    private final ReplicaSets replicaSets;
    private final MongoDbConnection.ChangeEventSourceConnectionFactory connections;

    private MongoDbCollectionSchema currentCollection;

    protected EventDispatcher<MongoDbPartition, CollectionId> dispatcher;
    protected IncrementalSnapshotContext<CollectionId> context = null;
    protected final Map<Struct, Object[]> window = new ConcurrentHashMap<>();
    private MongoDbConnection mongo;

    private final CollectionId signallingCollectionId;

    protected final NotificationService<MongoDbPartition, ? extends OffsetContext> notificationService;

    private final ExecutorService incrementalSnapshotThreadPool;

    public MongoDbIncrementalSnapshotChangeEventSource(MongoDbConnectorConfig config,
                                                       MongoDbConnection.ChangeEventSourceConnectionFactory connections, ReplicaSets replicaSets,
                                                       EventDispatcher<MongoDbPartition, CollectionId> dispatcher,
                                                       MongoDbSchema collectionSchema,
                                                       Clock clock,
                                                       SnapshotProgressListener<MongoDbPartition> progressListener,
                                                       DataChangeEventListener<MongoDbPartition> dataChangeEventListener,
                                                       NotificationService<MongoDbPartition, ? extends OffsetContext> notificationService) {
        this.connectorConfig = config;
        this.replicaSets = replicaSets;
        this.connections = connections;
        this.dispatcher = dispatcher;
        this.collectionSchema = collectionSchema;
        this.clock = clock;
        this.progressListener = progressListener;
        this.dataListener = dataChangeEventListener;
        this.signallingCollectionId = connectorConfig.getSignalingDataCollectionId() == null ? null
                : CollectionId.parse("UNUSED", connectorConfig.getSignalingDataCollectionId());
        this.notificationService = notificationService;
        this.incrementalSnapshotThreadPool = Threads.newFixedThreadPool(MongoDbConnector.class, config.getConnectorName(),
                "incremental-snapshot-" + replicaSets.all().get(0).replicaSetName(), connectorConfig.getSnapshotMaxThreads());
    }

    @Override
    @SuppressWarnings("unchecked")
    public void closeWindow(MongoDbPartition partition, String id, OffsetContext offsetContext) throws InterruptedException {
        context = (IncrementalSnapshotContext<CollectionId>) offsetContext.getIncrementalSnapshotContext();
        if (!context.closeWindow(id)) {
            return;
        }
        sendWindowEvents(offsetContext);
        readChunk(partition, offsetContext);
    }

    @Override
    public void pauseSnapshot(MongoDbPartition partition, OffsetContext offsetContext) throws InterruptedException {
        context = (IncrementalSnapshotContext<CollectionId>) offsetContext.getIncrementalSnapshotContext();
        if (context.snapshotRunning() && !context.isSnapshotPaused()) {
            context.pauseSnapshot();
            progressListener.snapshotPaused(partition);
            notifyReplicaSets((incrementalSnapshotContext, replicaSetPartition, replicaSetOffsetContext) -> notificationService.incrementalSnapshotNotificationService()
                    .notifyPaused(incrementalSnapshotContext, replicaSetPartition, replicaSetOffsetContext), offsetContext);
        }
    }

    @Override
    public void resumeSnapshot(MongoDbPartition partition, OffsetContext offsetContext) throws InterruptedException {
        context = (IncrementalSnapshotContext<CollectionId>) offsetContext.getIncrementalSnapshotContext();
        if (context.snapshotRunning() && context.isSnapshotPaused()) {
            context.resumeSnapshot();
            progressListener.snapshotResumed(partition);
            notifyReplicaSets(
                    (incrementalSnapshotContext, replicaSetPartition, replicaSetOffsetContext) -> notificationService.incrementalSnapshotNotificationService()
                            .notifyResumed(incrementalSnapshotContext, replicaSetPartition, replicaSetOffsetContext),
                    offsetContext);
            window.clear();
            context.revertChunk();
            readChunk(partition, offsetContext);
        }
    }

    protected String getSignalCollectionName(String dataCollectionId) {
        return dataCollectionId;
    }

    protected void sendWindowEvents(OffsetContext offsetContext) throws InterruptedException {
        LOGGER.debug("Sending {} events from window buffer", window.size());
        offsetContext.incrementalSnapshotEvents();
        for (Object[] row : window.values()) {
            sendEvent(dispatcher, offsetContext, row);
        }
        offsetContext.postSnapshotCompletion();
        window.clear();
    }

    // TODO Used typed dispatcher and offset context
    protected void sendEvent(EventDispatcher<MongoDbPartition, CollectionId> dispatcher, OffsetContext offsetContext, Object[] row)
            throws InterruptedException {
        context.sendEvent(keyFromRow(row));

        MongoDbOffsetContext mongoDbOffsetContext = getMongoDbOffsetContext(offsetContext);
        ReplicaSet replicaSet = replicaSets.getSnapshotReplicaSet();
        ReplicaSetOffsetContext replicaSetOffsetContext = mongoDbOffsetContext.getReplicaSetOffsetContext(replicaSet);
        ReplicaSetPartition replicaSetPartition = mongoDbOffsetContext.getReplicaSetPartition(replicaSet);

        replicaSetOffsetContext.readEvent(context.currentDataCollectionId().getId(), clock.currentTimeAsInstant());
        dispatcher.dispatchSnapshotEvent(
                replicaSetPartition, context.currentDataCollectionId().getId(),
                getChangeRecordEmitter(
                        replicaSetPartition, replicaSetOffsetContext, row),
                dispatcher.getIncrementalSnapshotChangeEventReceiver(dataListener));
    }

    private static MongoDbOffsetContext getMongoDbOffsetContext(OffsetContext offsetContext) {
        return (MongoDbOffsetContext) offsetContext;
    }

    /**
     * Returns a {@link ChangeRecordEmitter} producing the change records for
     * the given table row.
     */
    protected ChangeRecordEmitter<MongoDbPartition> getChangeRecordEmitter(MongoDbPartition partition,
                                                                           OffsetContext offsetContext, Object[] row) {
        return new MongoDbSnapshotRecordEmitter(partition, offsetContext, clock, (BsonDocument) row[0], connectorConfig);
    }

    protected void deduplicateWindow(DataCollectionId dataCollectionId, Object key) {
        if (context.currentDataCollectionId() == null || !context.currentDataCollectionId().getId().equals(dataCollectionId)) {
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

        mongo.execute(
                "emit window open for chunk '" + context.currentChunkId() + "'",
                client -> {
                    final MongoDatabase database = client.getDatabase(collectionId.dbName());
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
        mongo.execute(
                "emit window close for chunk '" + context.currentChunkId() + "'",
                client -> {
                    final MongoDatabase database = client.getDatabase(collectionId.dbName());
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
        // Only ReplicaSet deployments are supported by incremental snapshot
        // Thus assume replicaSets.size() == 1
        mongo = connections.get(replicaSets.all().get(0), partition);

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
            readChunk(partition, offsetContext);
        }
        catch (InterruptedException e) {
            throw new DebeziumException("Reading of an initial chunk after connector restart has been interrupted");
        }
        LOGGER.info("Incremental snapshot in progress, loading of initial chunk completed");
    }

    protected void readChunk(MongoDbPartition partition, OffsetContext offsetContext) throws InterruptedException {
        if (!context.snapshotRunning()) {
            LOGGER.info("Skipping read chunk because snapshot is not running");
            postIncrementalSnapshotCompleted();
            return;
        }
        if (context.isSnapshotPaused()) {
            LOGGER.info("Incremental snapshot was paused.");
            return;
        }
        try {
            preReadChunk(context);
            context.startNewChunk();
            emitWindowOpen();
            while (context.snapshotRunning()) {
                final CollectionId currentDataCollectionId = context.currentDataCollectionId().getId();
                currentCollection = (MongoDbCollectionSchema) collectionSchema.schemaFor(currentDataCollectionId);
                if (replicaSets.all().size() > 1) {
                    LOGGER.warn("Incremental snapshotting supported only for single replica set topology, skipping collection '{}', known collections {}",
                            currentDataCollectionId);
                    notifyReplicaSets(
                            (incrementalSnapshotContext, replicaSetPartition, replicaSetOffsetContext) -> notificationService.incrementalSnapshotNotificationService()
                                    .notifyTableScanCompleted(incrementalSnapshotContext, replicaSetPartition, replicaSetOffsetContext, totalRowsScanned, SKIPPED),
                            offsetContext);
                    nextDataCollection(partition, offsetContext);
                    continue;
                }
                // TODO Collection schema is calculated dynamically, it is necessary to use a different check
                if (currentCollection == null) {
                    LOGGER.warn("Schema not found for collection '{}', known collections {}", currentDataCollectionId, collectionSchema);
                    notifyReplicaSets(
                            (incrementalSnapshotContext, replicaSetPartition, replicaSetOffsetContext) -> notificationService.incrementalSnapshotNotificationService()
                                    .notifyTableScanCompleted(incrementalSnapshotContext, replicaSetPartition, replicaSetOffsetContext, totalRowsScanned, UNKNOWN_SCHEMA),
                            offsetContext);
                    nextDataCollection(partition, offsetContext);
                    continue;
                }
                // MongoDB collection has always key so it is not necessary to check if it is available
                if (!context.maximumKey().isPresent()) {
                    context.maximumKey(readMaximumKey());
                    if (!context.maximumKey().isPresent()) {
                        LOGGER.info(
                                "No maximum key returned by the query, incremental snapshotting of collection '{}' finished as it is empty",
                                currentDataCollectionId);
                        notifyReplicaSets(
                                (incrementalSnapshotContext, replicaSetPartition, replicaSetOffsetContext) -> notificationService.incrementalSnapshotNotificationService()
                                        .notifyTableScanCompleted(incrementalSnapshotContext, replicaSetPartition, replicaSetOffsetContext, totalRowsScanned, EMPTY),
                                offsetContext);
                        nextDataCollection(partition, offsetContext);
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

                    notifyReplicaSets(
                            (incrementalSnapshotContext, replicaSetPartition, replicaSetOffsetContext) -> notificationService.incrementalSnapshotNotificationService()
                                    .notifyTableScanCompleted(incrementalSnapshotContext, replicaSetPartition, replicaSetOffsetContext, totalRowsScanned, SUCCEEDED),
                            offsetContext);

                    collectionScanCompleted(partition);
                    nextDataCollection(partition, offsetContext);
                }
                else {

                    notifyReplicaSets(
                            (incrementalSnapshotContext, replicaSetPartition, replicaSetOffsetContext) -> notificationService.incrementalSnapshotNotificationService()
                                    .notifyInProgress(incrementalSnapshotContext, replicaSetPartition, replicaSetOffsetContext),
                            offsetContext);
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

    private void nextDataCollection(MongoDbPartition partition, OffsetContext offsetContext) {
        context.nextDataCollection();
        if (!context.snapshotRunning()) {
            progressListener.snapshotCompleted(partition);
            notifyReplicaSets(
                    (incrementalSnapshotContext, replicaSetPartition, replicaSetOffsetContext) -> notificationService.incrementalSnapshotNotificationService()
                            .notifyCompleted(incrementalSnapshotContext, replicaSetPartition, replicaSetOffsetContext),
                    offsetContext);
            context.unsetCorrelationId();
        }
    }

    private Object[] readMaximumKey() throws InterruptedException {
        final CollectionId collectionId = (CollectionId) currentCollection.id();
        final AtomicReference<Object> key = new AtomicReference<>();
        mongo.execute("maximum key for '" + collectionId + "'", client -> {
            final MongoDatabase database = client.getDatabase(collectionId.dbName());
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
    public void addDataCollectionNamesToSnapshot(SignalPayload<MongoDbPartition> signalPayload,
                                                 SnapshotConfiguration snapshotConfiguration)
            throws InterruptedException {

        final MongoDbPartition partition = signalPayload.partition;
        final OffsetContext offsetContext = signalPayload.offsetContext;
        final String correlationId = signalPayload.id;

        if (!snapshotConfiguration.getAdditionalConditions().isEmpty()) {
            throw new UnsupportedOperationException("Additional condition not supported for MongoDB");
        }

        if (!Strings.isNullOrEmpty(snapshotConfiguration.getSurrogateKey())) {
            throw new UnsupportedOperationException("Surrogate key not supported for MongoDB");
        }

        context = (IncrementalSnapshotContext<CollectionId>) offsetContext.getIncrementalSnapshotContext();
        final boolean shouldReadChunk = !context.snapshotRunning();
        final String rsName = replicaSets.all().get(0).replicaSetName();
        List<String> dataCollectionIds = snapshotConfiguration.getDataCollections()
                .stream()
                .map(x -> rsName + "." + x.toString())
                .collect(Collectors.toList());

        final List<DataCollection<CollectionId>> newDataCollectionIds = context.addDataCollectionNamesToSnapshot(correlationId, dataCollectionIds, List.of(), "");

        if (shouldReadChunk) {

            progressListener.snapshotStarted(partition);

            notifyReplicaSets(
                    (incrementalSnapshotContext, replicaSetPartition, replicaSetOffsetContext) -> notificationService.incrementalSnapshotNotificationService()
                            .notifyStarted(incrementalSnapshotContext, replicaSetPartition, replicaSetOffsetContext),
                    offsetContext);

            progressListener.monitoredDataCollectionsDetermined(partition, newDataCollectionIds.stream()
                    .map(x -> x.getId()).collect(Collectors.toList()));
            readChunk(partition, offsetContext);
        }
    }

    public void notifyReplicaSets(ReplicaSetNotifier<CollectionId> notifier, OffsetContext offsetContext) {

        MongoDbOffsetContext mongoDbOffsetContext = getMongoDbOffsetContext(offsetContext);

        for (ReplicaSet rs : replicaSets.all()) {
            ReplicaSetOffsetContext replicaSetOffsetContext = mongoDbOffsetContext.getReplicaSetOffsetContext(rs);
            ReplicaSetPartition replicaSetPartition = mongoDbOffsetContext.getReplicaSetPartition(rs);
            notifier.apply(context, replicaSetPartition, replicaSetOffsetContext);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void stopSnapshot(MongoDbPartition partition, OffsetContext offsetContext, Map<String, Object> additionalData, List<String> dataCollectionPatterns) {

        context = (IncrementalSnapshotContext<CollectionId>) offsetContext.getIncrementalSnapshotContext();
        if (context.snapshotRunning()) {
            if (dataCollectionPatterns == null || dataCollectionPatterns.isEmpty()) {
                LOGGER.info("Stopping incremental snapshot.");
                try {
                    // This must be called prior to closeWindow to ensure that the correct state is set
                    // to prevent chunk rads from triggering additional open/close events.
                    context.stopSnapshot();

                    // Clear the state
                    window.clear();
                    closeWindow(partition, context.currentChunkId(), offsetContext);

                    progressListener.snapshotAborted(partition);

                    notifyReplicaSets((incrementalSnapshotContext, replicaSetPartition, replicaSetOffsetContext) -> notificationService
                            .incrementalSnapshotNotificationService().notifyAborted(incrementalSnapshotContext, replicaSetPartition, replicaSetOffsetContext),
                            offsetContext);
                }
                catch (InterruptedException e) {
                    LOGGER.warn("Failed to stop snapshot successfully.", e);
                }
            }
            else {
                LOGGER.info("Removing '{}' collections from incremental snapshot", dataCollectionPatterns);
                final String rsName = replicaSets.all().get(0).replicaSetName();
                final List<String> dataCollectionIds = dataCollectionPatterns.stream().map(x -> rsName + "." + x.toString()).collect(Collectors.toList());

                for (String dataCollectionId : dataCollectionIds) {
                    final CollectionId collectionId = CollectionId.parse(dataCollectionId);
                    if (currentCollection != null && currentCollection.id().equals(collectionId)) {
                        window.clear();
                        LOGGER.info("Removed '{}' from incremental snapshot collection list.", collectionId);

                        collectionScanCompleted(partition);
                        nextDataCollection(partition, offsetContext);
                    }
                    else {
                        if (context.removeDataCollectionFromSnapshot(dataCollectionId)) {
                            LOGGER.info("Removed '{}' from incremental snapshot collection list.", collectionId);
                        }
                        else {
                            LOGGER.warn("Could not remove '{}', collection is not part of the incremental snapshot.", collectionId);
                        }
                    }
                }

                notifyReplicaSets(
                        (incrementalSnapshotContext, replicaSetPartition, replicaSetOffsetContext) -> notificationService.incrementalSnapshotNotificationService()
                                .notifyAborted(incrementalSnapshotContext, replicaSetPartition, replicaSetOffsetContext,
                                        dataCollectionIds),
                        offsetContext);
            }
        }
        else {
            LOGGER.warn("No active incremental snapshot, stop ignored");
        }
    }

    /**
     * Dispatches the data change events for the records of a single table.
     */
    private void createDataEventsForDataCollection(MongoDbPartition partition) throws InterruptedException {
        final CollectionId collectionId = (CollectionId) currentCollection.id();
        long exportStart = clock.currentTimeInMillis();
        LOGGER.debug("Exporting data chunk from collection '{}' (total {} collections)", currentCollection.id(), context.dataCollectionsToBeSnapshottedCount());

        mongo.execute("chunk query key for '" + currentCollection.id() + "'", client -> {
            final int threads = connectorConfig.getSnapshotMaxThreads();
            final int chunkSize = connectorConfig.getIncrementalSnapshotChunkSize();
            final MongoDatabase database = client.getDatabase(collectionId.dbName());
            final MongoCollection<BsonDocument> collection = database.getCollection(collectionId.name(), BsonDocument.class);

            Document predicate = constructQueryPredicate(context.chunkEndPosititon(), context.maximumKey().get());
            LOGGER.debug("\t For collection '{}' using query: '{}', key: '{}', maximum key: '{}' to get all _id fields",
                    currentCollection.id(), predicate.toJson(), context.chunkEndPosititon(), context.maximumKey().get());

            long rows = 0;
            Object[] lastRow = null;
            Object[] firstRow = null;
            List<Future<?>> futureList = new ArrayList<>();
            Object[] lastChunkKey = context.chunkEndPosititon();

            for (BsonDocument doc : collection.find(predicate).sort(new Document(DOCUMENT_ID, 1)).projection(Projections.include(DOCUMENT_ID))
                    .limit(chunkSize * threads)) {
                rows++;
                final Object[] row = new Object[]{ doc };
                if (firstRow == null) {
                    firstRow = row;
                }
                lastRow = row;

                if (rows % chunkSize == 0) {
                    lastChunkKey = addChunkToExecutor(collection, lastRow, futureList, lastChunkKey);
                }
            }

            // in case the last iteration doesn't have enough data, do it once again for the rest of the rows
            if (rows % chunkSize != 0) {
                addChunkToExecutor(collection, lastRow, futureList, lastChunkKey);
            }

            try {
                for (Future<?> future : futureList) {
                    future.get(); // Wait for the tasks to complete
                }
            }
            catch (ExecutionException e) {
                throw new DebeziumException("Error while processing chunk", e);
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

    protected Object[] addChunkToExecutor(final MongoCollection<BsonDocument> collection, Object[] lastRow,
                                          List<Future<?>> futureList, Object[] lastChunkKey) {
        final Object[] chunkStartKey = lastChunkKey;
        final Object[] chunkEndKey = keyFromRow(lastRow);
        futureList.add(this.incrementalSnapshotThreadPool.submit(() -> {
            queryChunk(collection, chunkStartKey, chunkEndKey);
        }));
        return chunkEndKey;
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
            final Struct keyStruct = currentCollection.keyFromDocumentSnapshot(doc);
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
        var documentId = ((BsonDocument) row[0]).get(DOCUMENT_ID);

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

}
