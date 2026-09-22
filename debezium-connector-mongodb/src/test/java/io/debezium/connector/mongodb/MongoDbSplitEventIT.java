/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static io.debezium.junit.EqualityCheck.LESS_THAN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import org.apache.kafka.connect.data.Struct;
import org.awaitility.Awaitility;
import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.mongodb.MongoCommandException;
import com.mongodb.WriteConcern;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.ChangeStreamPreAndPostImagesOptions;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.client.model.changestream.FullDocumentBeforeChange;

import io.debezium.DebeziumException;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.mongodb.MongoDbConnectorConfig.CaptureMode;
import io.debezium.connector.mongodb.MongoDbConnectorConfig.FullUpdateType;
import io.debezium.connector.mongodb.MongoDbConnectorConfig.PreImageMode;
import io.debezium.connector.mongodb.connection.MongoDbConnections;
import io.debezium.data.Envelope;
import io.debezium.doc.FixFor;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.junit.SkipWhenDatabaseVersion;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.util.LoggingContext;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;

@SkipWhenDatabaseVersion(check = LESS_THAN, major = 6, minor = 0, patch = 9, reason = "Splitting Change Stream events requires MongoDB 6.0.9 or newer.")
public class MongoDbSplitEventIT extends AbstractMongoConnectorIT {

    static Stream<Arguments> captureModes() {
        return Stream.of(
                Arguments.of(CaptureMode.CHANGE_STREAMS_UPDATE_FULL, FullUpdateType.LOOKUP, 2),
                Arguments.of(CaptureMode.CHANGE_STREAMS_UPDATE_FULL, FullUpdateType.POST_IMAGE, 2),
                Arguments.of(CaptureMode.CHANGE_STREAMS_WITH_PRE_IMAGE, FullUpdateType.LOOKUP, 2),
                Arguments.of(CaptureMode.CHANGE_STREAMS_UPDATE_FULL_WITH_PRE_IMAGE, FullUpdateType.LOOKUP, 3),
                Arguments.of(CaptureMode.CHANGE_STREAMS_UPDATE_FULL_WITH_PRE_IMAGE, FullUpdateType.POST_IMAGE, 3),
                Arguments.of(CaptureMode.CHANGE_STREAMS_UPDATE_FULL, FullUpdateType.POST_IMAGE_REQUIRED, 2),
                Arguments.of(CaptureMode.CHANGE_STREAMS_UPDATE_FULL_WITH_PRE_IMAGE, FullUpdateType.POST_IMAGE_REQUIRED, 3));
    }

    @ParameterizedTest
    @MethodSource("captureModes")
    @FixFor("debezium/dbz#2619")
    void shouldValidateAndResumeAfterSplitEvent(CaptureMode captureMode, FullUpdateType fullUpdateType, int fragmentCount) throws InterruptedException {
        final var dbName = "dbit";
        final var collectionName = "splitEvents";
        final var beforePayload = "a".repeat(9 * 1024 * 1024);
        final var afterPayload = "b".repeat(9 * 1024 * 1024);
        final var before = new Document("_id", 1).append("payload", beforePayload);
        final var after = new Document("_id", 1).append("payload", afterPayload);

        config = TestHelper.getConfiguration(mongo).edit()
                .with(CommonConnectorConfig.TOPIC_PREFIX, "mongo")
                .with(MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST, dbName + "." + collectionName)
                .with(MongoDbConnectorConfig.SNAPSHOT_MODE, MongoDbConnectorConfig.SnapshotMode.NO_DATA)
                .with(MongoDbConnectorConfig.CAPTURE_MODE, captureMode)
                .with(MongoDbConnectorConfig.CAPTURE_MODE_FULL_UPDATE_TYPE, fullUpdateType)
                .with(MongoDbConnectorConfig.CAPTURE_MODE_PRE_IMAGE, preImageMode(captureMode, fullUpdateType))
                .with(MongoDbConnectorConfig.CURSOR_OVERSIZE_HANDLING_MODE, MongoDbConnectorConfig.OversizeHandlingMode.SPLIT)
                .with(MongoDbConnectorConfig.POLL_INTERVAL_MS, 10)
                .with(Heartbeat.HEARTBEAT_INTERVAL, 0)
                .build();
        context = new MongoDbTaskContext(config);

        try (var client = connect()) {
            final var database = client.getDatabase(dbName);
            database.createCollection(collectionName, new CreateCollectionOptions()
                    .changeStreamPreAndPostImagesOptions(new ChangeStreamPreAndPostImagesOptions(true)));
            final var collection = database.getCollection(collectionName);
            collection.insertOne(before);

            start(MongoDbConnector.class, config);
            waitForStreamingRunning("mongodb", "mongo");

            // Observe the actual fragment tokens independently of Debezium's merging and offset handling.
            final var stream = openSplitStream(collection, captureMode, fullUpdateType);

            final BsonDocument lastFragmentToken;
            try (var cursor = stream.cursor()) {
                collection.updateOne(new Document("_id", 1), new Document("$set", new Document("payload", afterPayload)));
                lastFragmentToken = readSplitEventResumeToken(cursor, fragmentCount);
            }

            final var records = consumeRecordsByTopic(1);
            assertThat(records.recordsForTopic("mongo." + dbName + "." + collectionName)).hasSize(1);
            final var record = records.allRecordsInOrder().get(0);
            final var value = (Struct) record.value();
            assertThat(value.getString(Envelope.FieldName.OPERATION)).isEqualTo(Envelope.Operation.UPDATE.code());
            if (captureMode.isFullUpdate()) {
                assertThat(Document.parse(value.getString("after"))).isEqualTo(after);
            }
            if (captureMode.isIncludePreImage()) {
                assertThat(Document.parse(value.getString("before"))).isEqualTo(before);
            }
            final var resumeToken = ResumeTokens.toBase64(lastFragmentToken);
            assertThat(record.sourceOffset().get(SourceInfo.RESUME_TOKEN)).isEqualTo(resumeToken);
            stopConnector();

            final var committedOffset = readLastCommittedOffset(config, record.sourcePartition());
            assertThat(committedOffset).containsEntry(SourceInfo.RESUME_TOKEN, resumeToken);

            // The server can resume from this token with the same document options.
            try (var ignored = stream.resumeAfter(lastFragmentToken).cursor()) {
                final var offset = new MongoDbOffsetContext.Loader(context.getConfig()).load(committedOffset);
                try (var connection = MongoDbConnections.create(config)) {
                    assertThat(connection.validateLogPosition(offset, context))
                            .as("Startup validation must accept the committed token of a completed split event")
                            .isTrue();
                }
            }

            start(MongoDbConnector.class, config);
            waitForStreamingRunning("mongodb", "mongo");
            final var marker = new Document("_id", 2).append("marker", "resumed");
            collection.insertOne(marker);
            final var resumedRecords = consumeRecordsByTopic(1);
            assertThat(resumedRecords.recordsForTopic("mongo." + dbName + "." + collectionName)).hasSize(1);
            final var resumedRecord = resumedRecords.allRecordsInOrder().get(0);
            final var resumedValue = (Struct) resumedRecord.value();
            assertThat(resumedValue.getString(Envelope.FieldName.OPERATION)).isEqualTo(Envelope.Operation.CREATE.code());
            assertThat(Document.parse(resumedValue.getString("after"))).isEqualTo(marker);
            assertNoRecordsToConsume();
        }
    }

    @ParameterizedTest
    @MethodSource("captureModes")
    @FixFor("debezium/dbz#2619")
    void shouldRejectUnsplitResumeTokenWhenDocumentOptionsCauseSplitting(CaptureMode captureMode, FullUpdateType fullUpdateType, int fragmentCount)
            throws InterruptedException {
        final var collectionName = "splitTokenOptions";
        config = TestHelper.getConfiguration(mongo).edit()
                .with(CommonConnectorConfig.TOPIC_PREFIX, "mongo")
                .with(MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST, "dbit." + collectionName)
                .with(MongoDbConnectorConfig.CAPTURE_MODE, captureMode)
                .with(MongoDbConnectorConfig.CAPTURE_MODE_FULL_UPDATE_TYPE, fullUpdateType)
                .with(MongoDbConnectorConfig.CAPTURE_MODE_PRE_IMAGE, preImageMode(captureMode, fullUpdateType))
                .with(MongoDbConnectorConfig.CURSOR_OVERSIZE_HANDLING_MODE, MongoDbConnectorConfig.OversizeHandlingMode.SPLIT)
                .build();
        context = new MongoDbTaskContext(config);

        try (var client = connect()) {
            final var database = client.getDatabase("dbit");
            database.createCollection(collectionName, new CreateCollectionOptions()
                    .changeStreamPreAndPostImagesOptions(new ChangeStreamPreAndPostImagesOptions(true)));
            final var collection = database.getCollection(collectionName).withWriteConcern(WriteConcern.MAJORITY);
            collection.insertOne(new Document("_id", 1).append("payload", "a".repeat(9 * 1024 * 1024)));

            // Observe the same update with identical pipelines and scopes, changing only document options.
            final var withoutDocuments = openSplitStream(collection, CaptureMode.CHANGE_STREAMS, fullUpdateType);
            final var withDocuments = openSplitStream(collection, captureMode, fullUpdateType);
            final ChangeStreamDocument<BsonDocument> unsplitEvent;
            final BsonDocument lastFragmentToken;
            try (var unsplitCursor = withoutDocuments.cursor();
                    var splitCursor = withDocuments.cursor()) {
                collection.updateOne(new Document("_id", 1),
                        new Document("$set", new Document("payload", "b".repeat(9 * 1024 * 1024))));
                unsplitEvent = readChangeStreamEvent(unsplitCursor);
                assertThat(unsplitEvent.getSplitEvent()).isNull();

                final var fragments = readSplitEvent(splitCursor, fragmentCount);
                final var firstFragment = fragments.get(0);
                assertThat(firstFragment.getClusterTime()).isEqualTo(unsplitEvent.getClusterTime());
                assertThat(firstFragment.getDocumentKey()).isEqualTo(unsplitEvent.getDocumentKey());
                assertThat(firstFragment.getOperationType()).isEqualTo(unsplitEvent.getOperationType());
                assertThat(fragments).allSatisfy(fragment -> assertThat(fragment.getResumeToken()).isNotEqualTo(unsplitEvent.getResumeToken()));
                lastFragmentToken = fragments.get(fragmentCount - 1).getResumeToken();
            }

            final var marker = new Document("_id", 2).append("marker", "resumed");
            collection.insertOne(marker);
            final var error = assertThrows(MongoCommandException.class, () -> {
                try (var resumed = withDocuments.resumeAfter(unsplitEvent.getResumeToken()).cursor()) {
                    resumed.tryNext();
                }
            });
            assertThat(error.getErrorCode()).isEqualTo(280);

            final var offset = MongoDbOffsetContext.empty(context.getConfig());
            offset.changeStreamEvent(unsplitEvent);
            final Map<String, Object> splitOffset = new HashMap<>(offset.getOffset());
            splitOffset.put(SourceInfo.RESUME_TOKEN, ResumeTokens.toBase64(lastFragmentToken));
            try (var connection = MongoDbConnections.create(config)) {
                assertThat(connection.validateLogPosition(offset, context)).isFalse();
                assertThat(connection.validateLogPosition(new MongoDbOffsetContext.Loader(context.getConfig()).load(splitOffset), context)).isTrue();
            }

            // Both tokens remain usable with the document options that produced them.
            try (var resumed = withoutDocuments.resumeAfter(unsplitEvent.getResumeToken()).cursor()) {
                assertThat(readChangeStreamEvent(resumed).getFullDocument()).isEqualTo(marker.toBsonDocument());
            }
            try (var resumed = withDocuments.resumeAfter(lastFragmentToken).cursor()) {
                assertThat(readChangeStreamEvent(resumed).getFullDocument()).isEqualTo(marker.toBsonDocument());
            }
        }
    }

    @ParameterizedTest
    @MethodSource("captureModes")
    @FixFor("debezium/dbz#2619")
    void shouldInitializeSnapshotOffsetAtEndOfSplitEvent(CaptureMode captureMode, FullUpdateType fullUpdateType, int fragmentCount)
            throws InterruptedException {
        configureSnapshot(captureMode, fullUpdateType);
        try (var client = connect()) {
            final var collection = createSnapshotCollection(client.getDatabase("dbit"));
            collection.insertOne(new Document("_id", 1).append("payload", "a".repeat(9 * 1024 * 1024)));
            try (var probe = MongoUtils.openChangeStream(client, context).batchSize(1).cursor();
                    var observer = openSplitStream(collection, captureMode, fullUpdateType).cursor()) {
                collection.updateOne(new Document("_id", 1),
                        new Document("$set", new Document("payload", "b".repeat(9 * 1024 * 1024))));
                final var marker = new Document("_id", 2).append("marker", "after-boundary");
                collection.insertOne(marker);

                final var offset = MongoDbOffsetContext.empty(context.getConfig());
                offset.initEvent(probe);
                final var fragments = readSplitEvent(observer, fragmentCount);
                assertSnapshotOffset(offset, fragments);
                // The probe must consume only the first logical event, even if the next one is available.
                assertThat(readChangeStreamEvent(probe).getFullDocument()).isEqualTo(marker.toBsonDocument());
                try (var connection = MongoDbConnections.create(config)) {
                    assertThat(connection.validateLogPosition(offset, context)).isTrue();
                }
                try (var resumed = MongoUtils.openChangeStream(client, context).resumeAfter(offset.lastResumeTokenDoc()).cursor()) {
                    assertThat(readChangeStreamEvent(resumed).getFullDocument()).isEqualTo(marker.toBsonDocument());
                }
            }
        }
    }

    @ParameterizedTest
    @MethodSource("captureModes")
    @FixFor("debezium/dbz#2619")
    void shouldResumeStreamingAfterSnapshotStartsWithSplitEvent(CaptureMode captureMode, FullUpdateType fullUpdateType, int fragmentCount)
            throws InterruptedException {
        configureSnapshot(captureMode, fullUpdateType);
        try (var client = connect()) {
            final var collection = createSnapshotCollection(client.getDatabase("dbit"));
            collection.insertOne(new Document("_id", 1).append("payload", "a".repeat(9 * 1024 * 1024)));
            final var after = new Document("_id", 1).append("payload", "b".repeat(9 * 1024 * 1024));
            final var marker = new Document("_id", 2).append("marker", "after-boundary");
            try (var observer = openSplitStream(collection, captureMode, fullUpdateType).cursor()) {
                final var updated = new CountDownLatch(1);
                final var updateStarted = new AtomicBoolean();
                final var updateFailure = new AtomicReference<Throwable>();
                final var commandLogger = (Logger) org.slf4j.LoggerFactory.getLogger("org.mongodb.driver.protocol.command");
                final var previousLevel = commandLogger.getLevel();
                // Run the write synchronously before the snapshot probe's first getMore is sent.
                // This places the update after cursor creation without timing sleeps or production test hooks.
                final var interceptor = new LogInterceptor(commandLogger.getName()) {
                    @Override
                    protected void append(ILoggingEvent event) {
                        if ("snapshot".equals(event.getMDCPropertyMap().get(LoggingContext.CONNECTOR_CONTEXT))
                                && event.getFormattedMessage().startsWith("Command \"getMore\" started")
                                && updateStarted.compareAndSet(false, true)) {
                            try {
                                collection.updateOne(new Document("_id", 1), new Document("$set", new Document("payload", after.getString("payload"))));
                                collection.insertOne(marker);
                            }
                            catch (Throwable t) {
                                updateFailure.set(t);
                            }
                            finally {
                                updated.countDown();
                            }
                        }
                    }
                };
                commandLogger.setLevel(Level.DEBUG);
                try {
                    final var engineFailure = new AtomicReference<Throwable>();
                    start(MongoDbConnector.class, config, (success, message, error) -> {
                        if (!success) {
                            engineFailure.set(new DebeziumException(message, error));
                        }
                    });
                    assertThat(updated.await(waitTimeForRecords() * 30L, TimeUnit.SECONDS)).as("Snapshot probe triggered the concurrent update").isTrue();
                    assertThat(updateFailure.get()).isNull();
                    final var fragments = readSplitEvent(observer, fragmentCount);
                    final var lastToken = ResumeTokens.toBase64(fragments.get(fragmentCount - 1).getResumeToken());

                    final var snapshotRecords = consumeRecordsByTopic(2).allRecordsInOrder();
                    final List<Document> snapshotDocuments = new ArrayList<>();
                    for (var record : snapshotRecords) {
                        final var value = (Struct) record.value();
                        assertThat(value.getString(Envelope.FieldName.OPERATION)).isEqualTo(Envelope.Operation.READ.code());
                        assertThat(record.sourceOffset().get(SourceInfo.RESUME_TOKEN)).isEqualTo(lastToken);
                        assertThat(record.sourceOffset().get(SourceInfo.TIMESTAMP)).isEqualTo(fragments.get(0).getClusterTime().getTime());
                        assertThat(record.sourceOffset().get(SourceInfo.ORDER)).isEqualTo(fragments.get(0).getClusterTime().getInc());
                        snapshotDocuments.add(Document.parse(value.getString("after")));
                    }
                    assertThat(snapshotDocuments).containsExactlyInAnyOrder(after, marker);

                    final var streamedRecord = consumeRecordsByTopic(1).allRecordsInOrder().get(0);
                    final var streamedValue = (Struct) streamedRecord.value();
                    assertThat(streamedValue.getString(Envelope.FieldName.OPERATION)).isEqualTo(Envelope.Operation.CREATE.code());
                    assertThat(Document.parse(streamedValue.getString("after"))).isEqualTo(marker);
                    assertNoRecordsToConsume();
                    assertThat(engineFailure.get()).isNull();
                }
                finally {
                    commandLogger.detachAppender(interceptor);
                    commandLogger.setLevel(previousLevel);
                    interceptor.stop();
                }
            }
        }
    }

    @Test
    @FixFor("debezium/dbz#2619")
    void shouldInitializeSnapshotOffsetWithoutEvents() {
        configureSnapshot(CaptureMode.CHANGE_STREAMS_UPDATE_FULL_WITH_PRE_IMAGE, FullUpdateType.POST_IMAGE);
        try (var client = connect()) {
            createSnapshotCollection(client.getDatabase("dbit"));
            try (var probe = MongoUtils.openChangeStream(client, context).cursor()) {
                final var offset = MongoDbOffsetContext.empty(context.getConfig());
                offset.initEvent(probe);
                assertThat(probe.getResumeToken()).isNotNull();
                assertThat(offset.lastResumeTokenDoc()).isEqualTo(probe.getResumeToken());
            }
        }
    }

    @Test
    @FixFor("debezium/dbz#2619")
    void shouldInitializeSnapshotOffsetWithUnsplitEvent() {
        configureSnapshot(CaptureMode.CHANGE_STREAMS_UPDATE_FULL_WITH_PRE_IMAGE, FullUpdateType.POST_IMAGE);
        try (var client = connect()) {
            final var collection = createSnapshotCollection(client.getDatabase("dbit"));
            try (var probe = MongoUtils.openChangeStream(client, context).cursor();
                    var observer = openSplitStream(collection, CaptureMode.CHANGE_STREAMS_UPDATE_FULL_WITH_PRE_IMAGE, FullUpdateType.POST_IMAGE).cursor()) {
                collection.insertOne(new Document("_id", 1).append("value", "small"));
                final var offset = MongoDbOffsetContext.empty(context.getConfig());
                offset.initEvent(probe);
                final var event = readChangeStreamEvent(observer);
                assertThat(event.getSplitEvent()).isNull();
                assertThat(offset.lastResumeTokenDoc()).isEqualTo(event.getResumeToken());
                assertThat(offset.lastTimestamp()).isEqualTo(event.getClusterTime());
            }
        }
    }

    @Test
    @FixFor("debezium/dbz#2619")
    void shouldNotInitializeSnapshotOffsetFromIncompleteSplitEvent() {
        configureSnapshot(CaptureMode.CHANGE_STREAMS_UPDATE_FULL_WITH_PRE_IMAGE, FullUpdateType.POST_IMAGE);
        try (var client = connect()) {
            final var collection = createSnapshotCollection(client.getDatabase("dbit"));
            collection.insertOne(new Document("_id", 1).append("payload", "a".repeat(9 * 1024 * 1024)));
            try (var probe = MongoUtils.openChangeStream(client, context).batchSize(1).cursor()) {
                collection.updateOne(new Document("_id", 1),
                        new Document("$set", new Document("payload", "b".repeat(9 * 1024 * 1024))));
                assertThat(readChangeStreamEvent(probe).getSplitEvent().getFragment()).isEqualTo(1);
                final var offset = MongoDbOffsetContext.empty(context.getConfig());
                assertThrows(DebeziumException.class, () -> offset.initEvent(probe));
                assertThat(offset.lastResumeToken()).isNull();
                assertThat(offset.sourceInfo().hasPosition()).isFalse();
            }
        }
    }

    private void configureSnapshot(CaptureMode captureMode, FullUpdateType fullUpdateType) {
        config = TestHelper.getConfiguration(mongo).edit()
                .with(CommonConnectorConfig.TOPIC_PREFIX, "mongo")
                .with(MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST, "dbit.snapshotSplitEvents")
                .with(MongoDbConnectorConfig.SNAPSHOT_MODE, MongoDbConnectorConfig.SnapshotMode.INITIAL)
                .with(MongoDbConnectorConfig.CAPTURE_MODE, captureMode)
                .with(MongoDbConnectorConfig.CAPTURE_MODE_FULL_UPDATE_TYPE, fullUpdateType)
                .with(MongoDbConnectorConfig.CAPTURE_MODE_PRE_IMAGE, preImageMode(captureMode, fullUpdateType))
                .with(MongoDbConnectorConfig.CURSOR_OVERSIZE_HANDLING_MODE, MongoDbConnectorConfig.OversizeHandlingMode.SPLIT)
                .with(MongoDbConnectorConfig.POLL_INTERVAL_MS, 10)
                .with(Heartbeat.HEARTBEAT_INTERVAL, 0)
                .build();
        context = new MongoDbTaskContext(config);
    }

    private MongoCollection<Document> createSnapshotCollection(MongoDatabase database) {
        database.createCollection("snapshotSplitEvents", new CreateCollectionOptions()
                .changeStreamPreAndPostImagesOptions(new ChangeStreamPreAndPostImagesOptions(true)));
        return database.getCollection("snapshotSplitEvents").withWriteConcern(WriteConcern.MAJORITY);
    }

    private void assertSnapshotOffset(MongoDbOffsetContext offset, List<ChangeStreamDocument<BsonDocument>> fragments) {
        assertThat(offset.lastResumeTokenDoc()).isEqualTo(fragments.get(fragments.size() - 1).getResumeToken());
        assertThat(offset.lastTimestamp()).isEqualTo(fragments.get(0).getClusterTime());
        assertThat(offset.sourceInfo().collectionId()).isEqualTo(new CollectionId("dbit", "snapshotSplitEvents"));
    }

    private ChangeStreamIterable<BsonDocument> openSplitStream(MongoCollection<Document> collection, CaptureMode captureMode, FullUpdateType fullUpdateType) {
        final var stream = collection.watch(List.of(new Document("$changeStreamSplitLargeEvent", new Document())), BsonDocument.class)
                .maxAwaitTime(1, TimeUnit.SECONDS);
        if (captureMode.isFullUpdate()) {
            stream.fullDocument(fullUpdateType == FullUpdateType.POST_IMAGE_REQUIRED ? FullDocument.REQUIRED
                    : fullUpdateType.isPostImage() ? FullDocument.WHEN_AVAILABLE : FullDocument.UPDATE_LOOKUP);
        }
        if (captureMode.isIncludePreImage()) {
            stream.fullDocumentBeforeChange(preImageMode(captureMode, fullUpdateType) == PreImageMode.REQUIRED
                    ? FullDocumentBeforeChange.REQUIRED
                    : FullDocumentBeforeChange.WHEN_AVAILABLE);
        }
        return stream;
    }

    private static PreImageMode preImageMode(CaptureMode captureMode, FullUpdateType fullUpdateType) {
        return captureMode.isIncludePreImage() && fullUpdateType == FullUpdateType.POST_IMAGE_REQUIRED ? PreImageMode.REQUIRED : PreImageMode.WHEN_AVAILABLE;
    }

    private ChangeStreamDocument<BsonDocument> readChangeStreamEvent(MongoChangeStreamCursor<ChangeStreamDocument<BsonDocument>> cursor) {
        final var event = new AtomicReference<ChangeStreamDocument<BsonDocument>>();
        Awaitility.await("Receiving a change stream event")
                .atMost(waitTimeForRecords() * 30L, TimeUnit.SECONDS)
                .until(() -> {
                    event.set(cursor.tryNext());
                    return event.get() != null;
                });
        return event.get();
    }

    private BsonDocument readSplitEventResumeToken(MongoChangeStreamCursor<ChangeStreamDocument<BsonDocument>> cursor, int fragmentCount) {
        return readSplitEvent(cursor, fragmentCount).get(fragmentCount - 1).getResumeToken();
    }

    private List<ChangeStreamDocument<BsonDocument>> readSplitEvent(MongoChangeStreamCursor<ChangeStreamDocument<BsonDocument>> cursor, int fragmentCount) {
        final List<ChangeStreamDocument<BsonDocument>> fragments = new ArrayList<>();
        Awaitility.await("Receiving all " + fragmentCount + " change stream fragments")
                .atMost(waitTimeForRecords() * 30L, TimeUnit.SECONDS)
                .until(() -> {
                    final var event = cursor.tryNext();
                    if (event == null) {
                        return false;
                    }
                    assertThat(event.getSplitEvent()).isNotNull();
                    assertThat(event.getSplitEvent().getFragment()).isEqualTo(fragments.size() + 1);
                    assertThat(event.getSplitEvent().getOf()).isEqualTo(fragmentCount);
                    assertThat(event.getResumeToken()).isNotNull();
                    fragments.add(event);
                    return fragments.size() == fragmentCount;
                });
        return fragments;
    }
}
