/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.connect.errors.ConnectException;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.changestream.ChangeStreamDocument;

import io.debezium.DebeziumException;
import io.debezium.bean.spi.BeanRegistry;
import io.debezium.connector.SnapshotRecord;
import io.debezium.connector.mongodb.connection.MongoDbConnection;
import io.debezium.connector.mongodb.recordemitter.MongoDbSnapshotRecordEmitter;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.EventDispatcher.SnapshotReceiver;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.signal.actions.snapshotting.AdditionalCondition;
import io.debezium.pipeline.signal.actions.snapshotting.SnapshotConfiguration;
import io.debezium.pipeline.source.AbstractSnapshotChangeEventSource;
import io.debezium.pipeline.source.SnapshottingTask;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.ChangeRecordEmitter;
import io.debezium.pipeline.spi.SnapshotResult;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.spi.snapshot.Snapshotter;
import io.debezium.util.Clock;
import io.debezium.util.Strings;
import io.debezium.util.Threads;

/**
 * A {@link SnapshotChangeEventSource} that performs multithreaded snapshots of replica sets.
 *
 * @author Chris Cranford
 */
public class MongoDbSnapshotChangeEventSource extends AbstractSnapshotChangeEventSource<MongoDbPartition, MongoDbOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MongoDbSnapshotChangeEventSource.class);

    private final MongoDbConnectorConfig connectorConfig;
    private final MongoDbTaskContext taskContext;
    private final EventDispatcher<MongoDbPartition, CollectionId> dispatcher;
    private final Clock clock;
    private final SnapshotProgressListener<MongoDbPartition> snapshotProgressListener;
    private final ErrorHandler errorHandler;
    private final SnapshotterService snapshotterService;

    public MongoDbSnapshotChangeEventSource(MongoDbConnectorConfig connectorConfig, MongoDbTaskContext taskContext,
                                            EventDispatcher<MongoDbPartition, CollectionId> dispatcher, Clock clock,
                                            SnapshotProgressListener<MongoDbPartition> snapshotProgressListener, ErrorHandler errorHandler,
                                            NotificationService<MongoDbPartition, MongoDbOffsetContext> notificationService, SnapshotterService snapshotterService,
                                            BeanRegistry beanRegistry) {
        super(connectorConfig, snapshotProgressListener, notificationService, beanRegistry);
        this.connectorConfig = connectorConfig;
        this.taskContext = taskContext;
        this.dispatcher = dispatcher;
        this.clock = clock;
        this.snapshotProgressListener = snapshotProgressListener;
        this.errorHandler = errorHandler;
        this.snapshotterService = snapshotterService;
    }

    @Override
    protected SnapshotResult<MongoDbOffsetContext> doExecute(ChangeEventSourceContext context,
                                                             MongoDbOffsetContext prevOffsetCtx,
                                                             SnapshotContext<MongoDbPartition, MongoDbOffsetContext> snapshotContext,
                                                             SnapshottingTask snapshottingTask)
            throws Exception {
        final MongoDbSnapshotContext mongoDbSnapshotContext = (MongoDbSnapshotContext) snapshotContext;

        LOGGER.info("Snapshot step 1 - Preparing");
        if (prevOffsetCtx != null && prevOffsetCtx.isInitialSnapshotRunning()) {
            LOGGER.info("Previous snapshot was cancelled before completion; a new snapshot will be taken.");
        }

        LOGGER.info("Snapshot step 2 - Determining snapshot offsets");
        initSnapshotStartOffsets(mongoDbSnapshotContext);

        LOGGER.info("Snapshot step 3 - Snapshotting data");
        try {
            doSnapshot(context, mongoDbSnapshotContext, snapshottingTask);
        }
        catch (Throwable t) {
            LOGGER.error("Snapshot failed", t);
            throw new DebeziumException("Snapshot failed", t);
        }

        return SnapshotResult.completed(snapshotContext.offset);
    }

    @Override
    public SnapshottingTask getBlockingSnapshottingTask(MongoDbPartition partition, MongoDbOffsetContext previousOffset, SnapshotConfiguration snapshotConfiguration) {

        Map<DataCollectionId, String> filtersByTable = snapshotConfiguration.getAdditionalConditions().stream()
                .collect(Collectors.toMap(k -> CollectionId.parse(k.getDataCollection().toString()), AdditionalCondition::getFilter));

        return new SnapshottingTask(false, true, snapshotConfiguration.getDataCollections(), filtersByTable, true);
    }

    @Override
    public SnapshottingTask getSnapshottingTask(MongoDbPartition partition, MongoDbOffsetContext previousOffset) {

        final Snapshotter snapshotter = snapshotterService.getSnapshotter();
        final List<String> dataCollectionsToBeSnapshotted = connectorConfig.getDataCollectionsToBeSnapshotted();

        boolean offsetExists = previousOffset != null;
        boolean snapshotInProgress = false;

        if (offsetExists) {
            snapshotInProgress = previousOffset.isInitialSnapshotRunning();
        }

        if (offsetExists && !previousOffset.isInitialSnapshotRunning()) {
            LOGGER.info("A previous offset indicating a completed snapshot has been found.");
        }

        boolean shouldSnapshotSchema = snapshotter.shouldSnapshotSchema(offsetExists, snapshotInProgress);
        boolean shouldSnapshotData = snapshotter.shouldSnapshotData(offsetExists, snapshotInProgress);

        if (!shouldSnapshotData) {
            LOGGER.info("According to the connector configuration, no snapshot will occur.");
        }

        return new SnapshottingTask(shouldSnapshotSchema, shouldSnapshotData,
                dataCollectionsToBeSnapshotted, connectorConfig.getSnapshotFilterQueryByCollection(),
                false);
    }

    @Override
    protected SnapshotContext<MongoDbPartition, MongoDbOffsetContext> prepare(MongoDbPartition partition, boolean onDemand) {
        return new MongoDbSnapshotContext(partition);
    }

    private void doSnapshot(ChangeEventSourceContext sourceCtx, MongoDbSnapshotContext snapshotCtx, SnapshottingTask snapshottingTask)
            throws Throwable {
        try (MongoDbConnection mongo = taskContext.getConnection(dispatcher, snapshotCtx.partition)) {
            initSnapshotStartOffsets(snapshotCtx, mongo);
            SnapshotReceiver<MongoDbPartition> snapshotReceiver = dispatcher.getSnapshotChangeEventReceiver();
            snapshotCtx.offset.preSnapshotStart(snapshottingTask.isOnDemand());

            createDataEvents(sourceCtx, snapshotCtx, snapshotReceiver, mongo, snapshottingTask);

            snapshotCtx.offset.preSnapshotCompletion();
            snapshotReceiver.completeSnapshot();
            snapshotCtx.offset.postSnapshotCompletion();
        }
    }

    private void initSnapshotStartOffsets(MongoDbSnapshotContext snapshotCtx) {
        LOGGER.info("Initializing empty Offset context");
        snapshotCtx.offset = MongoDbOffsetContext.empty(connectorConfig);
    }

    private void initSnapshotStartOffsets(MongoDbSnapshotContext snapshotCtx, MongoDbConnection mongo) throws InterruptedException {
        LOGGER.info("Determine Snapshot start offset");
        mongo.execute("Setting resume token", client -> {
            ChangeStreamIterable<BsonDocument> stream = MongoUtils.openChangeStream(client, taskContext);
            try (MongoChangeStreamCursor<ChangeStreamDocument<BsonDocument>> cursor = stream.cursor()) {
                snapshotCtx.offset.initEvent(cursor);
            }
        });
        snapshotCtx.offset.initFromOpTimeIfNeeded(mongo.hello());
    }

    /**
     * Dispatches the data change events for the records of a single replica-set.
     */
    private void createDataEvents(ChangeEventSourceContext sourceContext,
                                  MongoDbSnapshotContext snapshotContext,
                                  SnapshotReceiver<MongoDbPartition> snapshotReceiver,
                                  MongoDbConnection mongo,
                                  SnapshottingTask snapshottingTask)
            throws Throwable {
        snapshotContext.lastCollection = false;
        snapshotContext.offset.startInitialSnapshot();

        LOGGER.info("Beginning snapshot at {}", snapshotContext.offset.getOffset());

        Set<Pattern> dataCollectionPattern = getDataCollectionPattern(snapshottingTask.getDataCollections());
        // mongo.collections() return a not sorted list and so not deterministic. Forcing the natural order.
        List<CollectionId> allCollections = mongo.collections().stream()
                .sorted(Comparator.comparing(CollectionId::name))
                .collect(Collectors.toList());
        final List<CollectionId> collections = determineDataCollectionsToBeSnapshotted(allCollections, dataCollectionPattern)
                .collect(Collectors.toList());
        snapshotProgressListener.monitoredDataCollectionsDetermined(snapshotContext.partition, collections);

        // Since multiple snapshot threads are to be used, create a thread pool and initiate the snapshot.
        // The current thread will wait until the snapshot threads either have completed or an error occurred.
        final int numThreads = Math.min(collections.size(), connectorConfig.getSnapshotMaxThreads());
        final Queue<CollectionId> collectionsToCopy = new ConcurrentLinkedQueue<>(collections);

        LOGGER.info("Creating snapshot worker pool with {} worker thread(s)", numThreads);
        final ExecutorService executorService = Threads.newFixedThreadPool(MongoDbConnector.class, taskContext.getServerName(), "snapshot-main",
                connectorConfig.getSnapshotMaxThreads());

        final AtomicBoolean aborted = new AtomicBoolean(false);
        final AtomicInteger threadCounter = new AtomicInteger(0);

        LOGGER.info("Preparing to use {} thread(s) to snapshot {} collection(s): {}", numThreads, collections.size(),
                Strings.join(", ", collections));

        CompletionService<Void> completionService = new ExecutorCompletionService<>(executorService);
        for (int i = 0; i < numThreads; ++i) {
            completionService
                    .submit(() -> buildCallable(sourceContext, snapshotContext, snapshotReceiver, mongo, snapshottingTask, threadCounter, aborted, collectionsToCopy));
        }

        try {
            for (int i = 0; i < numThreads; i++) {
                completionService.take().get();
            }
        }
        finally {
            executorService.shutdown();
        }

        snapshotContext.offset.stopInitialSnapshot();
    }

    private Void buildCallable(ChangeEventSourceContext sourceContext, MongoDbSnapshotContext snapshotContext, SnapshotReceiver<MongoDbPartition> snapshotReceiver,
                               MongoDbConnection mongo, SnapshottingTask snapshottingTask, AtomicInteger threadCounter, AtomicBoolean aborted,
                               Queue<CollectionId> collectionsToCopy) {

        taskContext.configureLoggingContext("snapshot" + threadCounter.incrementAndGet());
        CollectionId id = null;
        try {
            while (!aborted.get() && (id = collectionsToCopy.poll()) != null) {
                if (!sourceContext.isRunning()) {
                    throw new InterruptedException("Interrupted while snapshotting");
                }

                if (collectionsToCopy.isEmpty()) {
                    snapshotContext.lastCollection = true;
                }

                createDataEventsForCollection(
                        sourceContext,
                        snapshotContext,
                        snapshotReceiver,
                        id,
                        mongo, snapshottingTask.getFilterQueries());
            }
        }
        catch (Throwable t) {
            // Do nothing so that this thread is stopped
            LOGGER.error("Snapshot failed", t);
            aborted.set(true);
            throw new ConnectException("Snapshotting of collection " + id + " failed", t);
        }
        return null;
    }

    @Override
    protected <T extends DataCollectionId> Stream<T> determineDataCollectionsToBeSnapshotted(final Collection<T> allDataCollections,
                                                                                             Set<Pattern> snapshotAllowedDataCollections) {
        if (snapshotAllowedDataCollections.isEmpty()) {
            return allDataCollections.stream();
        }
        else {
            return allDataCollections.stream()
                    .filter(dataCollectionId -> snapshotAllowedDataCollections.stream()
                            .anyMatch(s -> s.matcher(((CollectionId) dataCollectionId).namespace()).matches()));
        }
    }

    private void createDataEventsForCollection(ChangeEventSourceContext sourceContext,
                                               MongoDbSnapshotContext snapshotContext,
                                               SnapshotReceiver<MongoDbPartition> snapshotReceiver,
                                               CollectionId collectionId, MongoDbConnection mongo,
                                               Map<DataCollectionId, String> snapshotFilterQueryForCollection)
            throws InterruptedException {
        long exportStart = clock.currentTimeInMillis();
        LOGGER.info("\t Exporting data for collection '{}'", collectionId);
        notificationService.initialSnapshotNotificationService().notifyTableInProgress(snapshotContext.partition, snapshotContext.offset, collectionId.namespace());

        mongo.execute("sync '" + collectionId + "'", client -> {
            final MongoDatabase database = client.getDatabase(collectionId.dbName());
            final MongoCollection<BsonDocument> collection = database.getCollection(collectionId.name(), BsonDocument.class);

            final int batchSize = taskContext.getConnectorConfig().getSnapshotFetchSize();

            long docs = 0;
            Optional<String> snapshotFilterForCollectionId = Optional.ofNullable(snapshotFilterQueryForCollection.get(collectionId));
            Bson filterQuery = Document.parse(snapshotFilterForCollectionId.orElse("{}"));

            try (MongoCursor<BsonDocument> cursor = collection.find(filterQuery).batchSize(batchSize).iterator()) {
                snapshotContext.lastRecordInCollection = false;
                if (cursor.hasNext()) {
                    while (cursor.hasNext()) {
                        if (!sourceContext.isRunning()) {
                            throw new InterruptedException("Interrupted while snapshotting collection " + collectionId.name());
                        }

                        BsonDocument document = cursor.next();
                        docs++;

                        snapshotContext.lastRecordInCollection = !cursor.hasNext();

                        if (snapshotContext.lastCollection && snapshotContext.lastRecordInCollection) {
                            snapshotContext.offset.markSnapshotRecord(SnapshotRecord.LAST);
                        }

                        dispatcher.dispatchSnapshotEvent(snapshotContext.partition, collectionId,
                                getChangeRecordEmitter(snapshotContext, collectionId, document),
                                snapshotReceiver);
                    }
                }
                else if (snapshotContext.lastCollection) {
                    // if the last collection does not contain any records we still need to mark the last processed event as last one
                    snapshotContext.offset.markSnapshotRecord(SnapshotRecord.LAST);
                }

                notificationService.initialSnapshotNotificationService().notifyCompletedTableSuccessfully(snapshotContext.partition, snapshotContext.offset,
                        collectionId.namespace());
                LOGGER.info("\t Finished snapshotting {} records for collection '{}'; total duration '{}'", docs, collectionId,
                        Strings.duration(clock.currentTimeInMillis() - exportStart));
                snapshotProgressListener.dataCollectionSnapshotCompleted(snapshotContext.partition, collectionId, docs);
            }
        });
    }

    private Optional<String> determineSnapshotQuery(Map<String, String> snapshotFilterQueryForCollection, CollectionId collectionId) {

        String snapshotFilterForCollectionId = snapshotFilterQueryForCollection.get(collectionId.dbName() + "." + collectionId.name());

        if (snapshotFilterForCollectionId != null) {
            return Optional.of(snapshotFilterForCollectionId);
        }

        return Optional.empty();
    }

    private ChangeRecordEmitter<MongoDbPartition> getChangeRecordEmitter(SnapshotContext<MongoDbPartition, MongoDbOffsetContext> snapshotContext,
                                                                         CollectionId collectionId, BsonDocument document) {
        snapshotContext.offset.readEvent(collectionId, getClock().currentTime());
        return new MongoDbSnapshotRecordEmitter(snapshotContext.partition, snapshotContext.offset, getClock(), document, connectorConfig);
    }

    private Clock getClock() {
        return clock;
    }

    /**
     * Mutable context that is populated in the course of snapshotting.
     */
    private static class MongoDbSnapshotContext extends SnapshotContext<MongoDbPartition, MongoDbOffsetContext> {
        public boolean lastCollection;
        public boolean lastRecordInCollection;

        MongoDbSnapshotContext(MongoDbPartition partition) {
            super(partition);
        }
    }
}
