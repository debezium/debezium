/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static io.debezium.junit.EqualityCheck.LESS_THAN;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;
import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoException;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.ChangeStreamPreAndPostImagesOptions;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.data.Envelope;
import io.debezium.junit.SkipWhenDatabaseVersion;

@SkipWhenDatabaseVersion(check = LESS_THAN, major = 6, reason = "Required pre- and post-images require MongoDB 6.0 or newer.")
public class MongoDbRequiredImagesIT extends AbstractMongoConnectorIT {

    private static final String DATABASE = "requiredImages";
    private static final String COLLECTION = "documents";
    private static final String TOPIC = "mongo." + DATABASE + "." + COLLECTION;

    @ParameterizedTest
    @CsvSource({
            "deployment, required, post_image, required, whenAvailable",
            "database, required, post_image, required, whenAvailable",
            "collection, required, post_image, required, whenAvailable",
            "deployment, when_available, post_image_required, whenAvailable, required",
            "database, when_available, post_image_required, whenAvailable, required",
            "collection, when_available, post_image_required, whenAvailable, required",
            "collection, required, lookup, required, updateLookup",
            "collection, required, post_image_required, required, required"
    })
    void shouldSendImagePoliciesToMongoDb(String scope, String preImage, String fullUpdate, String expectedPreImage, String expectedFullDocument) {
        config = imageConfiguration(preImage, fullUpdate).edit()
                .with(MongoDbConnectorConfig.CAPTURE_SCOPE, scope)
                .with(MongoDbConnectorConfig.CAPTURE_TARGET, scope.equals("collection") ? DATABASE + "." + COLLECTION : DATABASE)
                .build();
        final var options = new AtomicReference<BsonDocument>();
        final var settings = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(mongo.getConnectionString()))
                .addCommandListener(new CommandListener() {
                    @Override
                    public void commandStarted(CommandStartedEvent event) {
                        if (event.getCommandName().equals("aggregate")) {
                            final var firstStage = event.getCommand().getArray("pipeline").get(0).asDocument();
                            if (firstStage.containsKey("$changeStream")) {
                                options.set(firstStage.getDocument("$changeStream"));
                            }
                        }
                    }
                }).build();
        try (var client = MongoClients.create(settings)) {
            createCollection(client.getDatabase(DATABASE), true);
            try (var ignored = MongoUtils.openChangeStream(client, new MongoDbTaskContext(config)).cursor()) {
                assertThat(options.get()).isNotNull();
                assertThat(options.get().getString("fullDocumentBeforeChange").getValue()).isEqualTo(expectedPreImage);
                assertThat(options.get().getString("fullDocument").getValue()).isEqualTo(expectedFullDocument);
            }
        }
    }

    @ParameterizedTest
    @CsvSource({
            "required, post_image",
            "when_available, post_image_required",
            "required, post_image_required"
    })
    void shouldConsumeSnapshotAndAllOperations(String preImage, String fullUpdate) throws Exception {
        config = imageConfiguration(preImage, fullUpdate);
        try (var client = connect()) {
            final var database = client.getDatabase(DATABASE);
            createCollection(database, true);
            final var collection = database.getCollection(COLLECTION).withWriteConcern(WriteConcern.MAJORITY);
            collection.insertOne(document(1, 0));
            start(MongoDbConnector.class, config);
            assertImage(consumeRecordsByTopic(1).allRecordsInOrder().get(0), Envelope.Operation.READ, null, document(1, 0));
            waitForStreamingRunning("mongodb", "mongo");

            collection.insertOne(document(2, 0));
            collection.updateOne(new Document("_id", 1), new Document("$set", new Document("value", 1)));
            collection.replaceOne(new Document("_id", 1), document(1, 2));
            collection.deleteOne(new Document("_id", 1));
            final var records = consumeRecordsByTopic(5).recordsForTopic(TOPIC);
            assertThat(records).hasSize(5);
            assertImage(records.get(0), Envelope.Operation.CREATE, null, document(2, 0));
            assertImage(records.get(1), Envelope.Operation.UPDATE, document(1, 0), document(1, 1));
            assertImage(records.get(2), Envelope.Operation.UPDATE, document(1, 1), document(1, 2));
            assertImage(records.get(3), Envelope.Operation.DELETE, document(1, 2), null);
            assertThat(records.get(4).value()).isNull();
        }
    }

    @Test
    void shouldReturnEachRequiredPostImageWhenResuming() throws Exception {
        config = imageConfiguration("required", "post_image_required");
        try (var client = connect()) {
            final var database = client.getDatabase(DATABASE);
            createCollection(database, true);
            final var collection = database.getCollection(COLLECTION).withWriteConcern(WriteConcern.MAJORITY);
            start(MongoDbConnector.class, config);
            waitForStreamingRunning("mongodb", "mongo");
            collection.insertOne(document(1, 0));
            consumeRecordsByTopic(1);
            stopConnector();

            collection.updateOne(new Document("_id", 1), new Document("$set", new Document("value", 1)));
            collection.updateOne(new Document("_id", 1), new Document("$set", new Document("value", 2)));
            start(MongoDbConnector.class, config);
            final var records = consumeRecordsByTopic(2).recordsForTopic(TOPIC);
            assertImage(records.get(0), Envelope.Operation.UPDATE, document(1, 0), document(1, 1));
            assertImage(records.get(1), Envelope.Operation.UPDATE, document(1, 1), document(1, 2));
        }
    }

    @ParameterizedTest
    @CsvSource({
            "required, post_image, false",
            "when_available, post_image_required, false",
            "required, post_image_required, false",
            "required, post_image, true",
            "when_available, post_image_required, true",
            "required, post_image_required, true"
    })
    void shouldFailOnMissingImageWhileStreaming(String preImage, String fullUpdate, boolean disableAtRuntime) throws Exception {
        config = imageConfiguration(preImage, fullUpdate);
        try (var client = connect()) {
            final var database = client.getDatabase(DATABASE);
            createCollection(database, disableAtRuntime);
            final var collection = database.getCollection(COLLECTION).withWriteConcern(WriteConcern.MAJORITY);
            final var failure = startExpectingFailure();
            waitForStreamingRunning("mongodb", "mongo");
            // An INSERT is valid even when required images are not enabled. Consuming it also establishes the cursor.
            collection.insertOne(document(1, 0));
            final var insert = consumeRecordsByTopic(1).allRecordsInOrder().get(0);
            if (disableAtRuntime) {
                setImagesEnabled(database, false);
            }
            collection.updateOne(new Document("_id", 1), new Document("$set", new Document("value", 1)));

            assertMissingImageFailure(failure.get(30, TimeUnit.SECONDS));
            stopConnector();
            assertNoRecordsToConsume();
            assertThat(readLastCommittedOffset(config, insert.sourcePartition()))
                    .containsEntry(SourceInfo.RESUME_TOKEN, insert.sourceOffset().get(SourceInfo.RESUME_TOKEN));
        }
    }

    @ParameterizedTest
    @CsvSource({
            "required, post_image",
            "when_available, post_image_required",
            "required, post_image_required"
    })
    void shouldFailOnHistoricalMissingImageEvenAfterReenablingPapi(String preImage, String fullUpdate) throws Exception {
        config = imageConfiguration(preImage, fullUpdate).edit()
                .with(MongoDbConnectorConfig.SNAPSHOT_MODE, "when_needed")
                .build();
        try (var client = connect()) {
            final var database = client.getDatabase(DATABASE);
            createCollection(database, true);
            final var collection = database.getCollection(COLLECTION).withWriteConcern(WriteConcern.MAJORITY);
            start(MongoDbConnector.class, config);
            waitForStreamingRunning("mongodb", "mongo");
            collection.insertOne(document(1, 0));
            final var insert = consumeRecordsByTopic(1).allRecordsInOrder().get(0);
            stopConnector();
            final var offset = readLastCommittedOffset(config, insert.sourcePartition());

            setImagesEnabled(database, false);
            collection.updateOne(new Document("_id", 1), new Document("$set", new Document("value", 1)));
            setImagesEnabled(database, true);
            final var failure = startExpectingFailure();
            assertMissingImageFailure(failure.get(30, TimeUnit.SECONDS));
            stopConnector();
            assertNoRecordsToConsume();
            assertThat(readLastCommittedOffset(config, insert.sourcePartition())).isEqualTo(offset);
        }
    }

    @Test
    void shouldContinueWithoutImagesWhenAvailable() throws Exception {
        config = imageConfiguration("when_available", "post_image");
        try (var client = connect()) {
            final var database = client.getDatabase(DATABASE);
            createCollection(database, false);
            final var collection = database.getCollection(COLLECTION).withWriteConcern(WriteConcern.MAJORITY);
            start(MongoDbConnector.class, config);
            waitForStreamingRunning("mongodb", "mongo");
            collection.insertOne(document(1, 0));
            consumeRecordsByTopic(1);
            collection.updateOne(new Document("_id", 1), new Document("$set", new Document("value", 1)));
            assertImage(consumeRecordsByTopic(1).allRecordsInOrder().get(0), Envelope.Operation.UPDATE, null, null);
        }
    }

    @ParameterizedTest
    @CsvSource({ "required, post_image", "when_available, post_image_required" })
    void shouldFailWhenRecordedImagesExpire(String preImage, String fullUpdate) throws Exception {
        config = imageConfiguration(preImage, fullUpdate).edit()
                .with(MongoDbConnectorConfig.SNAPSHOT_MODE, "when_needed")
                .build();
        try (var client = connect()) {
            final var database = client.getDatabase(DATABASE);
            final var admin = client.getDatabase("admin");
            final var previousOptions = admin.runCommand(new Document("getClusterParameter", "changeStreamOptions"))
                    .getList("clusterParameters", Document.class).get(0).get("preAndPostImages", Document.class);
            try {
                createCollection(database, true);
                final var collection = database.getCollection(COLLECTION).withWriteConcern(WriteConcern.MAJORITY);
                final var uuid = database.listCollections().filter(new Document("name", COLLECTION)).first().get("info", Document.class).get("uuid");
                final var images = client.getDatabase("config").getCollection("system.preimages");
                final var imageFilter = new Document("_id.nsUUID", uuid);

                start(MongoDbConnector.class, config);
                waitForStreamingRunning("mongodb", "mongo");
                // A full truncation marker lets MongoDB 8.0 remove these images without waiting for more changes.
                collection.insertOne(document(1, 0).append("payload", "a".repeat(2 * 1024 * 1024)));
                final var insert = consumeRecordsByTopic(1).allRecordsInOrder().get(0);
                stopConnector();
                final var offset = readLastCommittedOffset(config, insert.sourcePartition());
                collection.updateOne(new Document("_id", 1), new Document("$set", new Document("value", 1)));
                assertThat(images.countDocuments(imageFilter)).isPositive();
                admin.runCommand(new Document("setClusterParameter", new Document("changeStreamOptions",
                        new Document("preAndPostImages", new Document("expireAfterSeconds", 1)))));
                Awaitility.await("Recorded change stream images expire")
                        .atMost(60, TimeUnit.SECONDS)
                        .until(() -> images.countDocuments(imageFilter) == 0);

                final var failure = startExpectingFailure();
                assertMissingImageFailure(failure.get(30, TimeUnit.SECONDS));
                stopConnector();
                assertNoRecordsToConsume();
                assertThat(readLastCommittedOffset(config, insert.sourcePartition())).isEqualTo(offset);
            }
            finally {
                admin.runCommand(new Document("setClusterParameter", new Document("changeStreamOptions",
                        new Document("preAndPostImages", previousOptions))));
            }
        }
    }

    private Configuration imageConfiguration(String preImage, String fullUpdate) {
        return TestHelper.getConfiguration(mongo).edit()
                .with(CommonConnectorConfig.TOPIC_PREFIX, "mongo")
                .with(MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST, DATABASE + "." + COLLECTION)
                .with(MongoDbConnectorConfig.CAPTURE_MODE, "change_streams_update_full_with_pre_image")
                .with("capture.mode.pre.image", preImage)
                .with("capture.mode.full.update.type", fullUpdate)
                .with(MongoDbConnectorConfig.POLL_INTERVAL_MS, 10)
                .build();
    }

    private CompletableFuture<Throwable> startExpectingFailure() {
        final var failure = new CompletableFuture<Throwable>();
        start(MongoDbConnector.class, config, (success, message, error) -> {
            if (!success) {
                failure.complete(error);
            }
        });
        return failure;
    }

    private static void assertMissingImageFailure(Throwable failure) {
        assertThat(failure).isNotNull();
        Throwable cause = failure;
        while (cause != null) {
            assertThat(cause).isNotInstanceOf(RetriableException.class);
            if (cause instanceof MongoException mongoException && mongoException.getCode() == 47) {
                assertThat(mongoException.getMessage()).contains("image");
                return;
            }
            cause = cause.getCause();
        }
        throw new AssertionError("Expected MongoDB's missing image error", failure);
    }

    private static void createCollection(MongoDatabase database, boolean imagesEnabled) {
        database.createCollection(COLLECTION, new CreateCollectionOptions()
                .changeStreamPreAndPostImagesOptions(new ChangeStreamPreAndPostImagesOptions(imagesEnabled)));
    }

    private static void setImagesEnabled(MongoDatabase database, boolean enabled) {
        database.runCommand(new Document("collMod", COLLECTION)
                .append("changeStreamPreAndPostImages", new Document("enabled", enabled)));
    }

    private static Document document(int id, int value) {
        return new Document("_id", id).append("value", value).append("unchanged", "payload");
    }

    private static void assertImage(SourceRecord record, Envelope.Operation operation, Document before, Document after) {
        final var value = (Struct) record.value();
        assertThat(value.getString(Envelope.FieldName.OPERATION)).isEqualTo(operation.code());
        assertThat(parseImage(value.getString("before"))).isEqualTo(before);
        assertThat(parseImage(value.getString("after"))).isEqualTo(after);
    }

    private static Document parseImage(String image) {
        return image == null ? null : Document.parse(image);
    }
}
