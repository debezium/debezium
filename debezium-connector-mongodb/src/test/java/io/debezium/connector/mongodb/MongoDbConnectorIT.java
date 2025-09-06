/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static io.debezium.connector.mongodb.JsonSerialization.COMPACT_JSON_SETTINGS;
import static io.debezium.junit.EqualityCheck.GREATER_THAN_OR_EQUAL;
import static io.debezium.junit.EqualityCheck.LESS_THAN;
import static java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME;
import static java.time.temporal.ChronoUnit.MILLIS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.junit.Test;

import com.mongodb.DBRef;
import com.mongodb.client.ClientSession;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.ChangeStreamPreAndPostImagesOptions;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.InsertOneOptions;
import com.mongodb.client.model.UpdateOneModel;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.mongodb.MongoDbConnectorConfig.FiltersMatchMode;
import io.debezium.connector.mongodb.MongoDbConnectorConfig.FullUpdateType;
import io.debezium.connector.mongodb.events.BufferingChangeStreamCursor;
import io.debezium.converters.CloudEventsConverterTest;
import io.debezium.data.Envelope;
import io.debezium.data.Envelope.Operation;
import io.debezium.data.VerifyRecord;
import io.debezium.doc.FixFor;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.junit.SkipWhenDatabaseVersion;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.schema.DatabaseSchema;
import io.debezium.util.Collect;
import io.debezium.util.IoUtil;
import io.debezium.util.Testing;

/**
 * @author Randall Hauch
 *
 */
public class MongoDbConnectorIT extends AbstractMongoConnectorIT {

    /**
     * Verifies that the connector doesn't run with an invalid configuration. This does not actually connect to the Mongo server.
     */
    @Test
    public void shouldNotStartWithInvalidConfiguration() {
        config = Configuration.create()
                .with(MongoDbConnectorConfig.SSL_ENABLED, "true")
                .build();

        // we expect the engine will log at least one error, so preface it ...
        logger.info("Attempting to start the connector with an INVALID configuration, so MULTIPLE error messages & one exceptions will appear in the log");
        start(MongoDbConnector.class, config, (success, msg, error) -> {
            assertThat(success).isFalse();
            assertThat(error).isNotNull();
        });
        assertConnectorNotRunning();
    }

    @Test
    public void shouldFailToValidateInvalidConfiguration() {
        Configuration config = Configuration.create().build();
        MongoDbConnector connector = new MongoDbConnector();
        Config result = connector.validate(config.asMap());

        assertConfigurationErrors(result, MongoDbConnectorConfig.CONNECTION_STRING, 1);
        assertConfigurationErrors(result, CommonConnectorConfig.TOPIC_PREFIX, 1);
        assertNoConfigurationErrors(result, MongoDbConnectorConfig.USER);
        assertNoConfigurationErrors(result, MongoDbConnectorConfig.PASSWORD);
        assertNoConfigurationErrors(result, MongoDbConnectorConfig.DATABASE_INCLUDE_LIST);
        assertNoConfigurationErrors(result, MongoDbConnectorConfig.DATABASE_EXCLUDE_LIST);
        assertNoConfigurationErrors(result, MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST);
        assertNoConfigurationErrors(result, MongoDbConnectorConfig.COLLECTION_EXCLUDE_LIST);
        assertNoConfigurationErrors(result, MongoDbConnectorConfig.SNAPSHOT_MAX_THREADS);
        assertNoConfigurationErrors(result, MongoDbConnectorConfig.MAX_QUEUE_SIZE);
        assertNoConfigurationErrors(result, MongoDbConnectorConfig.MAX_BATCH_SIZE);
        assertNoConfigurationErrors(result, MongoDbConnectorConfig.POLL_INTERVAL_MS);
        assertNoConfigurationErrors(result, MongoDbConnectorConfig.SSL_ENABLED);
        assertNoConfigurationErrors(result, MongoDbConnectorConfig.SSL_ALLOW_INVALID_HOSTNAMES);
        assertNoConfigurationErrors(result, CommonConnectorConfig.TOMBSTONES_ON_DELETE);
    }

    @Test
    public void shouldFailToValidateWithReplicaSetModeParams() {
        config = TestHelper.getConfiguration(mongo)
                .edit()
                .with(MongoDbConnector.DEPRECATED_SHARD_CS_PARAMS_FILED, "readPreference=primary")
                .with(MongoDbConnector.DEPRECATED_CONNECTION_MODE_FILED, "replica_set")
                .build();
        MongoDbConnector connector = new MongoDbConnector();
        Config result = connector.validate(config.asMap());

        assertConfigurationErrors(result, MongoDbConnectorConfig.CONNECTION_STRING, 2);
    }

    @Test
    public void shouldThrowExceptionWhenFieldExcludeListDatabasePartIsOnlyProvided() {
        shouldValidateFilterFieldConfiguration(MongoDbConnectorConfig.FIELD_EXCLUDE_LIST, "inventory", 1);
    }

    @Test
    public void shouldThrowExceptionWhenFieldExcludeListDatabaseAndCollectionPartIsOnlyProvided() {
        shouldValidateFilterFieldConfiguration(MongoDbConnectorConfig.FIELD_EXCLUDE_LIST, "inventory.collectionA", 1);
    }

    @Test
    public void shouldThrowExceptionWhenFieldExcludeListDatabaseAndCollectionPartsAreMissing() {
        shouldValidateFilterFieldConfiguration(MongoDbConnectorConfig.FIELD_EXCLUDE_LIST, ".name", 1);
    }

    @Test
    public void shouldThrowExceptionWhenFieldExcludeListFieldPartIsMissing() {
        shouldValidateFilterFieldConfiguration(MongoDbConnectorConfig.FIELD_EXCLUDE_LIST, "db1.collectionA.", 1);
    }

    @Test
    public void shouldNotThrowExceptionWhenFieldExcludeListHasLeadingWhiteSpaces() {
        shouldValidateFilterFieldConfiguration(MongoDbConnectorConfig.FIELD_EXCLUDE_LIST, " *.collectionA.name", 0);
    }

    @Test
    public void shouldNotThrowExceptionWhenFieldExcludeListHasWhiteSpaces() {
        shouldValidateFilterFieldConfiguration(MongoDbConnectorConfig.FIELD_EXCLUDE_LIST, "db1.collectionA.name ,db2.collectionB.house ", 0);
    }

    @Test
    public void shouldNotThrowExceptionWhenFieldExcludeListIsValid() {
        shouldValidateFilterFieldConfiguration(MongoDbConnectorConfig.FIELD_EXCLUDE_LIST, "db1.collectionA.name1", 0);
    }

    @Test
    public void shouldThrowExceptionWhenFieldRenamesDatabaseAndCollectionPartsAreMissing() {
        shouldValidateFilterFieldConfiguration(MongoDbConnectorConfig.FIELD_RENAMES, ".name=new_name", 1);
    }

    @Test
    public void shouldThrowExceptionWhenFieldRenamesReplacementPartIsMissing() {
        shouldValidateFilterFieldConfiguration(MongoDbConnectorConfig.FIELD_RENAMES, "db1.collectionA.", 1);
    }

    @Test
    public void shouldThrowExceptionWhenFieldRenamesReplacementPartSeparatorIsMissing() {
        shouldValidateFilterFieldConfiguration(MongoDbConnectorConfig.FIELD_RENAMES, "db1.collectionA.namenew_name", 1);
    }

    @Test
    public void shouldThrowExceptionWhenFieldRenamesRenameMappingKeyIsMissing() {
        shouldValidateFilterFieldConfiguration(MongoDbConnectorConfig.FIELD_RENAMES, "db1.collectionA.=new_name", 1);
    }

    @Test
    public void shouldThrowExceptionWhenFieldRenamesRenameMappingValueIsMissing() {
        shouldValidateFilterFieldConfiguration(MongoDbConnectorConfig.FIELD_RENAMES, "db1.collectionA.name=", 1);
    }

    @Test
    public void shouldNotThrowExceptionWhenFieldRenamesHasLeadingWhiteSpaces() {
        shouldValidateFilterFieldConfiguration(MongoDbConnectorConfig.FIELD_RENAMES, " db1.collectionA.name:newname", 0);
    }

    @Test
    public void shouldNotThrowExceptionWhenFieldRenamesHasWhiteSpaces() {
        shouldValidateFilterFieldConfiguration(MongoDbConnectorConfig.FIELD_RENAMES, "*.collectionA.name:new_name, db2.collectionB.house:new_house ", 0);
    }

    @Test
    public void shouldNotThrowExceptionWhenFieldRenamesIsValid() {
        shouldValidateFilterFieldConfiguration(MongoDbConnectorConfig.FIELD_RENAMES, "db1.collectionA.name1:new_name1", 0);
    }

    public void shouldValidateFilterFieldConfiguration(Field field, String value, int errorCount) {
        config = TestHelper.getConfiguration(mongo).edit()
                .with(field, value)
                .build();
        MongoDbConnector connector = new MongoDbConnector();
        Config result = connector.validate(config.asMap());

        if (errorCount == 0) {
            assertNoConfigurationErrors(result, field);
        }
        else {
            assertConfigurationErrors(result, field, errorCount);
        }
    }

    @Test
    public void shouldValidateAcceptableConfiguration() {
        config = TestHelper.getConfiguration(mongo);

        // Add data to the databases so that the databases will be listed ...
        context = new MongoDbTaskContext(config);
        storeDocuments("dbval", "validationColl1", "simple_objects.json");
        storeDocuments("dbval2", "validationColl2", "restaurants1.json");

        MongoDbConnector connector = new MongoDbConnector();
        Config result = connector.validate(config.asMap());

        assertNoConfigurationErrors(result, MongoDbConnectorConfig.CONNECTION_STRING);
        assertNoConfigurationErrors(result, CommonConnectorConfig.TOPIC_PREFIX);
        assertNoConfigurationErrors(result, MongoDbConnectorConfig.USER);
        assertNoConfigurationErrors(result, MongoDbConnectorConfig.PASSWORD);
        assertNoConfigurationErrors(result, MongoDbConnectorConfig.DATABASE_INCLUDE_LIST);
        assertNoConfigurationErrors(result, MongoDbConnectorConfig.DATABASE_EXCLUDE_LIST);
        assertNoConfigurationErrors(result, MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST);
        assertNoConfigurationErrors(result, MongoDbConnectorConfig.COLLECTION_EXCLUDE_LIST);
        assertNoConfigurationErrors(result, MongoDbConnectorConfig.SNAPSHOT_MAX_THREADS);
        assertNoConfigurationErrors(result, MongoDbConnectorConfig.MAX_QUEUE_SIZE);
        assertNoConfigurationErrors(result, MongoDbConnectorConfig.MAX_BATCH_SIZE);
        assertNoConfigurationErrors(result, MongoDbConnectorConfig.POLL_INTERVAL_MS);
        assertNoConfigurationErrors(result, MongoDbConnectorConfig.SSL_ENABLED);
        assertNoConfigurationErrors(result, MongoDbConnectorConfig.SSL_ALLOW_INVALID_HOSTNAMES);
        assertNoConfigurationErrors(result, CommonConnectorConfig.TOMBSTONES_ON_DELETE);
        assertNoConfigurationErrors(result, MongoDbConnectorConfig.CAPTURE_MODE);
    }

    @Test
    @SkipWhenDatabaseVersion(check = LESS_THAN, major = 6, reason = "wallTime support in Change Stream is officially released in Mongo 6.0.")
    public void shouldProvideWallTime() throws InterruptedException {
        config = TestHelper.getConfiguration(mongo).edit()
                .with(MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST, "dbit.*")
                .with(CommonConnectorConfig.TOPIC_PREFIX, "mongo")
                .build();

        context = new MongoDbTaskContext(config);
        TestHelper.cleanDatabase(mongo, "dbit");
        start(MongoDbConnector.class, config);
        waitForStreamingRunning("mongodb", "mongo");

        // Insert record
        final Instant timestamp = Instant.now();
        ObjectId objId = new ObjectId();
        Document obj = new Document("_id", objId);
        insertDocuments("dbit", "c1", obj);

        final SourceRecords records = consumeRecordsByTopic(1);
        assertThat(records.allRecordsInOrder().size()).isEqualTo(1);
        assertNoRecordsToConsume();
        final SourceRecord record = records.allRecordsInOrder().get(0);
        final Struct value = (Struct) record.value();

        final long wallTime = value.getStruct(Envelope.FieldName.SOURCE).getInt64(SourceInfo.WALL_TIME);
        Instant instant = Instant.ofEpochMilli(wallTime);
        assertThat(instant.truncatedTo(MILLIS).getNano()).isNotZero();
        assertThat(wallTime).isGreaterThanOrEqualTo(timestamp.toEpochMilli());
    }

    @Test
    @SkipWhenDatabaseVersion(check = GREATER_THAN_OR_EQUAL, major = 6, reason = "wallTime support in Change Stream is officially released in Mongo 6.0.")
    public void shouldNotProvideWallTimeForOlderVersions() throws InterruptedException {
        config = TestHelper.getConfiguration(mongo).edit()
                .with(MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST, "dbit.*")
                .with(CommonConnectorConfig.TOPIC_PREFIX, "mongo")
                .build();

        context = new MongoDbTaskContext(config);
        TestHelper.cleanDatabase(mongo, "dbit");
        start(MongoDbConnector.class, config);
        waitForStreamingRunning("mongodb", "mongo");

        // Insert record
        ObjectId objId = new ObjectId();
        Document obj = new Document("_id", objId);
        insertDocuments("dbit", "c1", obj);

        final SourceRecords records = consumeRecordsByTopic(1);
        assertThat(records.allRecordsInOrder().size()).isEqualTo(1);
        assertNoRecordsToConsume();
        final SourceRecord record = records.allRecordsInOrder().get(0);
        final Struct value = (Struct) record.value();
        // For pre-6.0 version, wallTime should not be presented
        assertThat(value.getStruct(Envelope.FieldName.SOURCE).getInt64(SourceInfo.WALL_TIME)).isNull();
    }

    @Test
    @SkipWhenDatabaseVersion(check = LESS_THAN, major = 6, reason = "Pre-image support in Change Stream is officially released in Mongo 6.0.")
    public void shouldConsumePreImage() throws InterruptedException {
        config = TestHelper.getConfiguration(mongo).edit()
                .with(MongoDbConnectorConfig.CAPTURE_MODE, MongoDbConnectorConfig.CaptureMode.CHANGE_STREAMS_UPDATE_FULL_WITH_PRE_IMAGE)
                .with(MongoDbConnectorConfig.POLL_INTERVAL_MS, 10)
                .with(MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST, "dbit.*")
                .with(CommonConnectorConfig.TOPIC_PREFIX, "mongo")
                .build();

        // Set up the replication context for connections ...
        context = new MongoDbTaskContext(config);

        // Cleanup database
        TestHelper.cleanDatabase(mongo, "dbit");

        // Before starting the connector, add data to the databases ...
        storeDocuments("dbit", "simpletons", "simple_objects.json");
        storeDocuments("dbit", "restaurants", "restaurants1.json");

        // Start the connector ...
        start(MongoDbConnector.class, config);

        // ---------------------------------------------------------------------------------------------------------------
        // Consume all of the events due to startup and initialization of the database
        // ---------------------------------------------------------------------------------------------------------------
        SourceRecords records = consumeRecordsByTopic(12);
        records.topics().forEach(System.out::println);
        assertThat(records.recordsForTopic("mongo.dbit.simpletons").size()).isEqualTo(6);
        assertThat(records.recordsForTopic("mongo.dbit.restaurants").size()).isEqualTo(6);
        assertThat(records.topics().size()).isEqualTo(2);
        AtomicBoolean foundLast = new AtomicBoolean(false);
        records.forEach(record -> {
            // Check that all records are valid, and can be serialized and deserialized ...
            validate(record);
            verifyFromInitialSnapshot(record, foundLast);
            verifyReadOperation(record);
        });
        assertThat(foundLast.get()).isTrue();

        AtomicReference<String> id = new AtomicReference<>();
        String collName = "preimage";
        try (var client = connect()) {
            MongoDatabase db1 = client.getDatabase("dbit");
            CreateCollectionOptions options = new CreateCollectionOptions();
            options.changeStreamPreAndPostImagesOptions(new ChangeStreamPreAndPostImagesOptions(true));
            db1.createCollection(collName, options);
        }

        Testing.Debug.enable();

        try (var client = connect()) {
            MongoDatabase db1 = client.getDatabase("dbit");
            MongoCollection<Document> coll = db1.getCollection(collName);

            // Insert the document with a generated ID ...
            Document doc = Document.parse("{\"a\": 1, \"b\": 2}");
            InsertOneOptions insertOptions = new InsertOneOptions().bypassDocumentValidation(true);
            coll.insertOne(doc, insertOptions);

            // Find the document to get the generated ID ...
            doc = coll.find().first();
            Testing.debug("Document: " + doc);
            id.set(doc.getObjectId("_id").toString());
            Testing.debug("Document ID: " + id.get());
        }

        try (var client = connect()) {
            MongoDatabase db1 = client.getDatabase("dbit");
            MongoCollection<Document> coll = db1.getCollection(collName);

            // Find the document ...
            Document doc = coll.find().first();
            Testing.debug("Document: " + doc);
            Document filter = Document.parse("{\"a\": 1}");
            Document operation = Document.parse("{ \"$set\": { \"b\": 10 } }");
            coll.updateOne(filter, operation);

            doc = coll.find().first();
            Testing.debug("Document: " + doc);
        }

        try (var client = connect()) {
            MongoDatabase db1 = client.getDatabase("dbit");
            MongoCollection<Document> coll = db1.getCollection(collName);

            // Find the document ...
            Document doc = coll.find().first();
            Testing.debug("Document: " + doc);
            Document filter = Document.parse("{\"a\": 1}");
            Document operation = Document.parse("{\"a\": 1, \"b\": 50}");
            coll.replaceOne(filter, operation);

            doc = coll.find().first();
            Testing.debug("Document: " + doc);
        }

        // Wait until we can consume the 1 insert and 1 update and 1 replace...
        SourceRecords insertAndUpdateAndReplace = consumeRecordsByTopic(3);
        assertThat(insertAndUpdateAndReplace.recordsForTopic("mongo.dbit." + collName).size()).isEqualTo(3);
        assertThat(insertAndUpdateAndReplace.topics().size()).isEqualTo(1);
        insertAndUpdateAndReplace.forEach(record -> {
            // Check that all records are valid, and can be serialized and deserialized ...
            validate(record);
            verifyNotFromInitialSnapshot(record);
        });
        SourceRecord insertRecord = insertAndUpdateAndReplace.allRecordsInOrder().get(0);
        verifyCreateOperation(insertRecord);

        SourceRecord updateRecord = insertAndUpdateAndReplace.allRecordsInOrder().get(1);
        verifyUpdateOperation(updateRecord);

        SourceRecord replaceRecord = insertAndUpdateAndReplace.allRecordsInOrder().get(2);
        verifyUpdateOperation(replaceRecord);

        Testing.debug("Insert event: " + insertRecord);
        Testing.debug("Update event: " + updateRecord);
        Testing.debug("Replace event: " + replaceRecord);

        Struct insertValue = (Struct) insertRecord.value();
        Struct updateValue = (Struct) updateRecord.value();
        Struct replaceValue = (Struct) replaceRecord.value();

        assertThat(insertValue.getString("before")).isNull();
        assertThat(insertValue.getString("after")).isEqualTo(updateValue.getString("before"));
        assertThat(updateValue.getString("after")).isEqualTo(replaceValue.getString("before"));

        // ---------------------------------------------------------------------------------------------------------------
        // Delete a document
        // ---------------------------------------------------------------------------------------------------------------
        try (var client = connect()) {
            MongoDatabase db1 = client.getDatabase("dbit");
            MongoCollection<Document> coll = db1.getCollection(collName);
            Document filter = Document.parse("{\"a\": 1}");
            coll.deleteOne(filter);
        }

        // Wait until we can consume the 1 delete ...
        SourceRecords delete = consumeRecordsByTopic(2);
        assertThat(delete.recordsForTopic("mongo.dbit." + collName).size()).isEqualTo(2);
        assertThat(delete.topics().size()).isEqualTo(1);

        SourceRecord deleteRecord = delete.allRecordsInOrder().get(0);
        validate(deleteRecord);
        verifyNotFromInitialSnapshot(deleteRecord);
        verifyDeleteOperation(deleteRecord);

        Struct deleteValue = (Struct) deleteRecord.value();
        assertThat(replaceValue.getString("after")).isEqualTo(deleteValue.getString("before"));

        SourceRecord tombStoneRecord = delete.allRecordsInOrder().get(1);
        validate(tombStoneRecord);

        Testing.debug("Delete event: " + deleteRecord);
        Testing.debug("Tombstone event: " + tombStoneRecord);
        Struct deleteKey = (Struct) deleteRecord.key();
        String deleteId = toObjectId(deleteKey.getString("id")).toString();
        assertThat(deleteId).isEqualTo(id.get());
    }

    @Test
    @SkipWhenDatabaseVersion(check = LESS_THAN, major = 6, reason = "Pre-image support in Change Stream is officially released in Mongo 6.0.")
    public void shouldConsumeFullUpdateWithPostImage() throws InterruptedException {
        shouldConsumeFullUpdate(FullUpdateType.POST_IMAGE, "updated", "final");
    }

    @Test
    public void shouldConsumeFullUpdateWithLookup() throws InterruptedException {
        shouldConsumeFullUpdate(FullUpdateType.LOOKUP, "updated", "final");
    }

    public void shouldConsumeFullUpdate(FullUpdateType fullUpdateType, String firstUpdate, String secondUpdate) throws InterruptedException {
        config = TestHelper.getConfiguration(mongo).edit()
                .with(MongoDbConnectorConfig.CAPTURE_MODE, MongoDbConnectorConfig.CaptureMode.CHANGE_STREAMS_UPDATE_FULL)
                .with(MongoDbConnectorConfig.CAPTURE_MODE_FULL_UPDATE_TYPE, fullUpdateType)
                .with(MongoDbConnectorConfig.POLL_INTERVAL_MS, 10)
                .with(MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST, "dbit.*")
                .with(CommonConnectorConfig.TOPIC_PREFIX, "mongo")
                .build();

        // Set up the replication context for connections ...
        context = new MongoDbTaskContext(config);

        // Cleanup database
        TestHelper.cleanDatabase(mongo, "dbit");

        // Create database
        String collName = "images";
        try (var client = connect()) {
            MongoDatabase db1 = client.getDatabase("dbit");
            CreateCollectionOptions options = new CreateCollectionOptions();
            if (fullUpdateType.isPostImage()) {
                options.changeStreamPreAndPostImagesOptions(new ChangeStreamPreAndPostImagesOptions(true));
            }
            db1.createCollection(collName, options);
        }

        var doc = new Document("_id", "0").append("val", "init");
        insertDocuments("dbit", collName, doc);

        // Start the connector ...
        start(MongoDbConnector.class, config);

        // Consume initial event
        SourceRecords records = consumeRecordsByTopic(1);
        records.topics().forEach(System.out::println);
        assertThat(records.recordsForTopic("mongo.dbit.images").size()).isEqualTo(1);
        assertThat(records.topics().size()).isEqualTo(1);
        AtomicBoolean foundLast = new AtomicBoolean(false);
        records.forEach(record -> {
            // Check that all records are valid, and can be serialized and deserialized ...
            validate(record);
            verifyFromInitialSnapshot(record, foundLast);
            verifyReadOperation(record);
        });
        assertThat(foundLast.get()).isTrue();

        // stop the connector
        stopConnector();
        Testing.Debug.enable();

        // update the document twice
        var selector = new Document("_id", "0");
        var update1 = new Document("$set", new Document("val", firstUpdate));
        var update2 = new Document("$set", new Document("val", secondUpdate));
        updateDocument("dbit", collName, selector, update1);
        updateDocument("dbit", collName, selector, update2);

        // start the connector again
        start(MongoDbConnector.class, config);

        // Wait until we can consume the 2 updates
        SourceRecords updateRecords = consumeRecordsByTopic(2);
        assertThat(updateRecords.recordsForTopic("mongo.dbit.images").size()).isEqualTo(2);
        assertThat(updateRecords.topics().size()).isEqualTo(1);
        updateRecords.forEach(record -> {
            // Check that all records are valid, and can be serialized and deserialized ...
            validate(record);
            verifyNotFromInitialSnapshot(record);
        });

        // Verify after values
        SourceRecord updateRecord1 = updateRecords.allRecordsInOrder().get(0);
        verifyUpdateOperation(updateRecord1);
        SourceRecord updateRecord2 = updateRecords.allRecordsInOrder().get(1);
        verifyUpdateOperation(updateRecord2);

        Testing.debug("Update event 1: " + updateRecord1);
        Testing.debug("Update event 2: " + updateRecord2);

        Struct updateValue1 = (Struct) updateRecord1.value();
        Struct updateValue2 = (Struct) updateRecord2.value();

        if (fullUpdateType.isPostImage()) {
            assertThat(updateValue1.getString("after")).contains(firstUpdate);
            assertThat(updateValue2.getString("after")).contains(secondUpdate);
        }
        else {
            assertThat(updateValue1.getString("after")).contains(secondUpdate);
            assertThat(updateValue2.getString("after")).contains(secondUpdate);
        }
    }

    @Test
    @SkipWhenDatabaseVersion(check = LESS_THAN, major = 6, reason = "Pre-image support in Change Stream is officially released in Mongo 6.0.")
    public void shouldConsumeLargeEvents() throws InterruptedException {
        final var collName = "large";
        final var dbName = "dbit";

        config = TestHelper.getConfiguration(mongo).edit()
                .with(MongoDbConnectorConfig.SNAPSHOT_MODE, MongoDbConnectorConfig.SnapshotMode.NO_DATA)
                .with(MongoDbConnectorConfig.CAPTURE_MODE, MongoDbConnectorConfig.CaptureMode.CHANGE_STREAMS_UPDATE_FULL_WITH_PRE_IMAGE)
                .with(MongoDbConnectorConfig.CURSOR_OVERSIZE_HANDLING_MODE, MongoDbConnectorConfig.OversizeHandlingMode.SPLIT)
                .with(MongoDbConnectorConfig.POLL_INTERVAL_MS, 10)
                .with(MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST, dbName + "." + collName)
                .with(CommonConnectorConfig.TOPIC_PREFIX, "mongo")
                .build();

        // Set up the replication context for connections ...
        context = new MongoDbTaskContext(config);

        // Cleanup database
        TestHelper.cleanDatabase(mongo, "dbit");

        // Enable pre images
        try (var client = connect()) {
            MongoDatabase db1 = client.getDatabase(dbName);
            CreateCollectionOptions options = new CreateCollectionOptions();
            options.changeStreamPreAndPostImagesOptions(new ChangeStreamPreAndPostImagesOptions(true));
            db1.createCollection(collName, options);
        }

        // Before starting the connector, add data to the databases ...
        var docId = 0;
        var beforeValue = String.join("", Collections.nCopies(16 * 1024 * 1024 - 1024, "a"));
        var expectedBeforeDocument = new Document("_id", 0).append("value", beforeValue);
        insertDocuments("dbit", collName, expectedBeforeDocument);

        // Start the connector ...
        start(MongoDbConnector.class, config);
        waitForStreamingRunning("mongodb", "mongo");

        // Update document
        var afterValue = String.join("", Collections.nCopies(16 * 1024 * 1024 - 1024, "b"));
        var expectedAfterDocument = new Document("_id", 0).append("value", afterValue);
        try (var client = connect()) {
            MongoDatabase db1 = client.getDatabase(dbName);
            MongoCollection<Document> coll = db1.getCollection(collName);

            // Insert the document with a generated ID ...
            var updateDocument = new Document("$set", new Document("value", afterValue));
            updateDocument(dbName, collName, new Document("_id", docId), updateDocument);
        }

        // Wait until we can consume the 1 insert and 1 update and 1 replace...
        SourceRecords records = consumeRecordsByTopic(1);
        assertThat(records.allRecordsInOrder().size()).isEqualTo(1);
        SourceRecord updateRecord = records.allRecordsInOrder().get(0);

        validate(updateRecord);
        verifyUpdateOperation(updateRecord);
        verifyNotFromInitialSnapshot(updateRecord);

        Struct updateValue = (Struct) updateRecord.value();
        var actualBeforeDocument = Document.parse(updateValue.getString("before"));
        var actualAfterDocument = Document.parse(updateValue.getString("after"));

        assertThat(actualBeforeDocument).isEqualTo(expectedBeforeDocument);
        assertThat(actualAfterDocument).isEqualTo(expectedAfterDocument);

    }

    @Test
    public void shouldLogInvalidOffsetWithSnapshotModeInitial() throws InterruptedException {
        final LogInterceptor logInterceptor = new LogInterceptor(MongoDbConnectorTask.class);

        // Configuration to capture changes from both databases
        config = TestHelper.getConfiguration(mongo).edit()
                .with(MongoDbConnectorConfig.POLL_INTERVAL_MS, 10)
                .with(MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST, "dbit.*")
                .with(CommonConnectorConfig.TOPIC_PREFIX, "mongo")
                .build();

        // Create invalid offset captured from some previous run
        // > unless time travel is involved it should be effectively globally unique
        Map<Map<String, ?>, Map<String, ?>> offset = Map.of(
                Collect.hashMapOf("server_id", "mongo"),
                Collect.hashMapOf(
                        SourceInfo.TIMESTAMP, 0,
                        SourceInfo.ORDER, -1,
                        SourceInfo.RESUME_TOKEN, "826617AF13000000012B022C0100296E5A1004DF2AABE77A714014A9E60EFF70AFF1DC46645F696400646617AF13E337477353A2CE830004"));
        storeOffsets(config, offset);

        // Set up the replication context for connections ...
        context = new MongoDbTaskContext(config);

        // Cleanup databases
        TestHelper.cleanDatabase(mongo, "dbit");

        // Before starting the connector, add data to the databases ...
        insertDocuments("dbit", "colA", new Document("value", "foo"));
        insertDocuments("dbit", "colB", new Document("value", "bar"));

        // ---------------------------------------------------------------------------------------------------------------
        // Start the connector
        // ---------------------------------------------------------------------------------------------------------------
        start(MongoDbConnector.class, config);

        // Snapshot not performed due to SnapshotMode.INITIAL, warning in the logs
        logInterceptor.containsMessage("Last recorded offset is no longer available on the server");
    }

    @Test
    public void shouldConsumeAllEventsFromDatabase() throws InterruptedException, IOException {
        // Use the DB configuration to define the connector's configuration ...
        config = TestHelper.getConfiguration(mongo).edit()
                .with(MongoDbConnectorConfig.POLL_INTERVAL_MS, 10)
                .with(MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST, "dbit.*")
                .with(CommonConnectorConfig.TOPIC_PREFIX, "mongo")
                // .with(MongoDbConnectorConfig.CAPTURE_MODE, CaptureMode.OPLOG)
                .build();

        // Set up the replication context for connections ...
        context = new MongoDbTaskContext(config);

        // Cleanup database
        TestHelper.cleanDatabase(mongo, "dbit");

        // Before starting the connector, add data to the databases ...
        storeDocuments("dbit", "simpletons", "simple_objects.json");
        storeDocuments("dbit", "restaurants", "restaurants1.json");

        // Start the connector ...
        start(MongoDbConnector.class, config);

        // ---------------------------------------------------------------------------------------------------------------
        // Consume all of the events due to startup and initialization of the database
        // ---------------------------------------------------------------------------------------------------------------
        SourceRecords records = consumeRecordsByTopic(12);
        records.topics().forEach(System.out::println);
        assertThat(records.recordsForTopic("mongo.dbit.simpletons").size()).isEqualTo(6);
        assertThat(records.recordsForTopic("mongo.dbit.restaurants").size()).isEqualTo(6);
        assertThat(records.topics().size()).isEqualTo(2);
        AtomicBoolean foundLast = new AtomicBoolean(false);
        records.forEach(record -> {
            // Check that all records are valid, and can be serialized and deserialized ...
            validate(record);
            verifyFromInitialSnapshot(record, foundLast);
            verifyReadOperation(record);
        });
        assertThat(foundLast.get()).isTrue();

        // At this point, the connector has performed the initial sync and awaits changes ...

        // ---------------------------------------------------------------------------------------------------------------
        // Store more documents while the connector is still running
        // ---------------------------------------------------------------------------------------------------------------
        storeDocuments("dbit", "restaurants", "restaurants2.json");

        // Wait until we can consume the 4 documents we just added ...
        SourceRecords records2 = consumeRecordsByTopic(4);
        assertThat(records2.recordsForTopic("mongo.dbit.restaurants").size()).isEqualTo(4);
        assertThat(records2.topics().size()).isEqualTo(1);
        records2.forEach(record -> {
            // Check that all records are valid, and can be serialized and deserialized ...
            validate(record);
            verifyNotFromInitialSnapshot(record);
            verifyCreateOperation(record);
        });

        // ---------------------------------------------------------------------------------------------------------------
        // Stop the connector
        // ---------------------------------------------------------------------------------------------------------------
        stopConnector();

        // ---------------------------------------------------------------------------------------------------------------
        // Store more documents while the connector is NOT running
        // ---------------------------------------------------------------------------------------------------------------
        storeDocuments("dbit", "restaurants", "restaurants3.json");

        // ---------------------------------------------------------------------------------------------------------------
        // Start the connector and we should only see the documents added since it was stopped
        // ---------------------------------------------------------------------------------------------------------------
        start(MongoDbConnector.class, config);

        // Wait until we can consume the 4 documents we just added ...
        SourceRecords records3 = consumeRecordsByTopic(5);
        assertThat(records3.recordsForTopic("mongo.dbit.restaurants").size()).isEqualTo(5);
        assertThat(records3.topics().size()).isEqualTo(1);
        records3.forEach(record -> {
            // Check that all records are valid, and can be serialized and deserialized ...
            validate(record);
            verifyNotFromInitialSnapshot(record);
            verifyCreateOperation(record);
        });

        // ---------------------------------------------------------------------------------------------------------------
        // Store more documents while the connector is still running
        // ---------------------------------------------------------------------------------------------------------------
        storeDocuments("dbit", "restaurants", "restaurants4.json");

        // Wait until we can consume the 4 documents we just added ...
        SourceRecords records4 = consumeRecordsByTopic(8);
        assertThat(records4.recordsForTopic("mongo.dbit.restaurants").size()).isEqualTo(8);
        assertThat(records4.topics().size()).isEqualTo(1);
        records4.forEach(record -> {
            // Check that all records are valid, and can be serialized and deserialized ...
            validate(record);
            verifyNotFromInitialSnapshot(record);
            verifyCreateOperation(record);
        });

        // ---------------------------------------------------------------------------------------------------------------
        // Create and then update a document
        // ---------------------------------------------------------------------------------------------------------------
        // Testing.Debug.enable();
        AtomicReference<String> id = new AtomicReference<>();
        try (var client = connect()) {
            MongoDatabase db1 = client.getDatabase("dbit");
            MongoCollection<Document> coll = db1.getCollection("arbitrary");
            coll.drop();

            // Insert the document with a generated ID ...
            Document doc = Document.parse("{\"a\": 1, \"b\": 2}");
            InsertOneOptions insertOptions = new InsertOneOptions().bypassDocumentValidation(true);
            coll.insertOne(doc, insertOptions);

            // Find the document to get the generated ID ...
            doc = coll.find().first();
            Testing.debug("Document: " + doc);
            id.set(doc.getObjectId("_id").toString());
            Testing.debug("Document ID: " + id.get());
        }

        try (var client = connect()) {
            MongoDatabase db1 = client.getDatabase("dbit");
            MongoCollection<Document> coll = db1.getCollection("arbitrary");

            // Find the document ...
            Document doc = coll.find().first();
            Testing.debug("Document: " + doc);
            Document filter = Document.parse("{\"a\": 1}");
            Document operation = Document.parse("{ \"$set\": { \"b\": 10 } }");
            coll.updateOne(filter, operation);

            doc = coll.find().first();
            Testing.debug("Document: " + doc);
        }

        // Wait until we can consume the 1 insert and 1 update ...
        SourceRecords insertAndUpdate = consumeRecordsByTopic(2);
        assertThat(insertAndUpdate.recordsForTopic("mongo.dbit.arbitrary").size()).isEqualTo(2);
        assertThat(insertAndUpdate.topics().size()).isEqualTo(1);
        records4.forEach(record -> {
            // Check that all records are valid, and can be serialized and deserialized ...
            validate(record);
            verifyNotFromInitialSnapshot(record);
            verifyCreateOperation(record);
        });
        SourceRecord insertRecord = insertAndUpdate.allRecordsInOrder().get(0);
        SourceRecord updateRecord = insertAndUpdate.allRecordsInOrder().get(1);
        Testing.debug("Insert event: " + insertRecord);
        Testing.debug("Update event: " + updateRecord);
        Struct insertKey = (Struct) insertRecord.key();
        Struct updateKey = (Struct) updateRecord.key();
        String insertId = toObjectId(insertKey.getString("id")).toString();
        String updateId = toObjectId(updateKey.getString("id")).toString();
        assertThat(insertId).isEqualTo(id.get());
        assertThat(updateId).isEqualTo(id.get());

        // ---------------------------------------------------------------------------------------------------------------
        // Delete a document
        // -------------------------------------------------------------------------------------------------------------
        try (var client = connect()) {
            MongoDatabase db1 = client.getDatabase("dbit");
            MongoCollection<Document> coll = db1.getCollection("arbitrary");
            Document filter = Document.parse("{\"a\": 1}");
            coll.deleteOne(filter);
        }

        // Wait until we can consume the 1 delete ...
        SourceRecords delete = consumeRecordsByTopic(2);
        assertThat(delete.recordsForTopic("mongo.dbit.arbitrary").size()).isEqualTo(2);
        assertThat(delete.topics().size()).isEqualTo(1);

        SourceRecord deleteRecord = delete.allRecordsInOrder().get(0);
        validate(deleteRecord);
        verifyNotFromInitialSnapshot(deleteRecord);
        verifyDeleteOperation(deleteRecord);

        SourceRecord tombStoneRecord = delete.allRecordsInOrder().get(1);
        validate(tombStoneRecord);

        Testing.debug("Delete event: " + deleteRecord);
        Testing.debug("Tombstone event: " + tombStoneRecord);
        Struct deleteKey = (Struct) deleteRecord.key();
        String deleteId = toObjectId(deleteKey.getString("id")).toString();
        assertThat(deleteId).isEqualTo(id.get());
    }

    @Test
    /*
     * Verifies that streaming starts from the specified timestamp.
     *
     * The following procedure is used:
     * 1) Two documents are inserted,
     * 2) Capture timestamp is retrieved (insert of the second doc)
     * 3) Insert additional document
     *
     * Connector should capture only second and third insert
     */
    public void shouldConsumeEventsFromTimestamp() throws InterruptedException, IOException {
        // Cleanup database
        TestHelper.cleanDatabase(mongo, "dbit");

        // insert some data
        long startOpTime = -1;
        var expectedDocs = List.of(
                Document.parse("{\"_id\": 0}"),
                Document.parse("{\"_id\": 1}"),
                Document.parse("{\"_id\": 2}"));
        try (var client = connect()) {
            var db = client.getDatabase("dbit");
            // insert two documents
            db.getCollection("test").insertOne(expectedDocs.get(0));
            db.getCollection("test").insertOne(expectedDocs.get(1));

            // get capture start timestamps
            var serverStatus = db.runCommand(new Document("serverStatus", 1), BsonDocument.class);
            startOpTime = serverStatus.getTimestamp("operationTime").getValue();

            // insert additional document
            db.getCollection("test").insertOne(expectedDocs.get(2));
        }

        // Use the DB configuration to define the connector's configuration ...
        config = TestHelper.getConfiguration(mongo).edit()
                .with(MongoDbConnectorConfig.POLL_INTERVAL_MS, 10)
                .with(MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST, "dbit.*")
                .with(CommonConnectorConfig.TOPIC_PREFIX, "mongo")
                .with(MongoDbConnectorConfig.SNAPSHOT_MODE, MongoDbConnectorConfig.SnapshotMode.NO_DATA)
                .with(MongoDbConnectorConfig.CAPTURE_START_OP_TIME, startOpTime)
                .build();

        // Set up the replication context for connections ...
        context = new MongoDbTaskContext(config);

        // Start the connector ...
        start(MongoDbConnector.class, config);
        waitForStreamingRunning("mongodb", "mongo");

        // Check consumed records
        final SourceRecords records = consumeAvailableRecordsByTopic();
        assertThat(records.allRecordsInOrder().size()).isEqualTo(2);
        assertNoRecordsToConsume();

        var actualDocs = records.allRecordsInOrder()
                .stream()
                .map(r -> ((Struct) r.value()).getString("after"))
                .map(Document::parse)
                .toList();
        assertThat(actualDocs.get(0)).isEqualTo(expectedDocs.get(1));
        assertThat(actualDocs.get(1)).isEqualTo(expectedDocs.get(2));
    }

    @Test
    @SkipWhenDatabaseVersion(check = LESS_THAN, major = 6, reason = "Pre-image support in Change Stream is officially released in Mongo 6.0.")
    public void shouldThrowErrorForOversizedEventsWithoutOversizeHandlingMode() throws InterruptedException {
        final LogInterceptor logInterceptor = new LogInterceptor(BufferingChangeStreamCursor.class);
        final LogInterceptor logInterceptor2 = new LogInterceptor(MongoDbStreamingChangeEventSource.class);

        final var collName = "large";
        final var dbName = "dbit";

        config = TestHelper.getConfiguration(mongo).edit()
                .with(MongoDbConnectorConfig.SNAPSHOT_MODE, MongoDbConnectorConfig.SnapshotMode.NO_DATA)
                .with(MongoDbConnectorConfig.CAPTURE_MODE, MongoDbConnectorConfig.CaptureMode.CHANGE_STREAMS_UPDATE_FULL_WITH_PRE_IMAGE)
                .with(MongoDbConnectorConfig.POLL_INTERVAL_MS, 10)
                .with(MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST, dbName + "." + collName)
                .with(CommonConnectorConfig.TOPIC_PREFIX, "mongo")
                .build();

        // Set up the replication context for connections ...
        context = new MongoDbTaskContext(config);

        // Cleanup database
        TestHelper.cleanDatabase(mongo, dbName);

        // Enable pre images
        try (var client = connect()) {
            MongoDatabase db1 = client.getDatabase(dbName);
            CreateCollectionOptions options = new CreateCollectionOptions();
            options.changeStreamPreAndPostImagesOptions(new ChangeStreamPreAndPostImagesOptions(true));
            db1.createCollection(collName, options);
        }

        // Start the connector ...
        start(MongoDbConnector.class, config);
        waitForStreamingRunning("mongodb", "mongo");

        // Add some data to the databases ...
        Document doc1 = new Document("_id", 0).append("value", "a");
        Document doc2 = new Document("_id", 1).append("value", "b");
        insertDocuments(dbName, collName, doc1, doc2);

        // Add an oversized document
        Document doc3 = new Document("_id", 2).append("value", String.join("", Collections.nCopies(16 * 1024 * 1024 - 1024, "c")));
        insertDocuments(dbName, collName, doc3);

        // Bulk update documents
        try (var client = connect()) {
            MongoDatabase db1 = client.getDatabase(dbName);
            MongoCollection<Document> coll = db1.getCollection(collName);

            // Update the documents
            coll.bulkWrite(Arrays.asList(
                    new UpdateOneModel<>(new Document("_id", 0), new Document("$set", new Document("value", "aa"))),
                    new UpdateOneModel<>(new Document("_id", 1), new Document("$set", new Document("value", "bb"))),
                    new UpdateOneModel<>(new Document("_id", 2),
                            new Document("$set", new Document("value", String.join("", Collections.nCopies(16 * 1024 * 1024 - 1024, "d")))))));
        }

        // Consume records
        SourceRecords records = consumeRecordsByTopic(3);
        Testing.print(records.recordsForTopic("mongo.dbit.large"));
        assertThat(records.recordsForTopic("mongo.dbit.large").size()).isEqualTo(3);

        // Stop the connector
        stopConnector(value -> {
            logInterceptor.containsErrorMessage("Fetcher thread has failed");
            logInterceptor2.containsErrorMessage("Error while reading change stream");
        });
    }

    @Test
    @SkipWhenDatabaseVersion(check = LESS_THAN, major = 4, minor = 4, reason = "$bsonSize aggregation operator support is officially released in Mongo 4.4.")
    public void shouldSkipOversizedEvents() throws InterruptedException, IOException {
        // Use the DB configuration to define the connector's configuration ...
        config = TestHelper.getConfiguration(mongo).edit()
                .with(MongoDbConnectorConfig.POLL_INTERVAL_MS, 10)
                .with(MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST, "dbit.*")
                .with(CommonConnectorConfig.TOPIC_PREFIX, "mongo")
                .with(MongoDbConnectorConfig.CAPTURE_MODE, MongoDbConnectorConfig.CaptureMode.CHANGE_STREAMS_UPDATE_FULL_WITH_PRE_IMAGE)
                .with(MongoDbConnectorConfig.CURSOR_OVERSIZE_HANDLING_MODE, MongoDbConnectorConfig.OversizeHandlingMode.SKIP)
                .with(MongoDbConnectorConfig.CURSOR_OVERSIZE_SKIP_THRESHOLD, 40) // maximum 25 bytes
                .build();

        // Set up the replication context for connections ...
        context = new MongoDbTaskContext(config);

        // Cleanup database
        TestHelper.cleanDatabase(mongo, "dbit");

        // Start the connector ...
        start(MongoDbConnector.class, config);
        waitForStreamingRunning("mongodb", "mongo");

        // start first document (total size is 32 bytes)
        var doc = new Document(Map.of("_id", 1, "val", new byte[8]));
        insertDocuments("dbit", "wrong", doc);

        SourceRecords records = consumeRecordsByTopic(1);
        assertThat(records.topics().size()).isEqualTo(1);
        assertThat(records.recordsForTopic("mongo.dbit.wrong")).hasSize(1);

        // this update is OK (total size is 40 bytes)
        var updateDoc = new Document("$set", new Document("val", new byte[16]));
        updateDocument("dbit", "wrong", new Document("_id", 1), updateDoc);

        records = consumeRecordsByTopic(1);
        assertThat(records.topics().size()).isEqualTo(1);
        assertThat(records.recordsForTopic("mongo.dbit.wrong")).hasSize(1);

        // this update is not ok (total size is 56 bytes)
        updateDoc = new Document("$set", new Document("val", new byte[32]));
        updateDocument("dbit", "wrong", new Document("_id", 1), updateDoc);
        var record = consumeRecord();
        assertThat(record).isNull();

        // store another document
        doc = new Document(Map.of("_id", 1, "var", new byte[8]));
        insertDocuments("dbit", "right", doc);

        records = consumeRecordsByTopic(1);
        assertThat(records.topics().size()).isEqualTo(1);
        assertThat(records.recordsForTopic("mongo.dbit.right")).hasSize(1);

    }

    @Test
    @FixFor("DBZ-1831")
    public void shouldConsumeAllEventsFromDatabaseWithSkippedOperations() throws InterruptedException, IOException {
        // Use the DB configuration to define the connector's configuration ...
        config = TestHelper.getConfiguration(mongo).edit()
                .with(MongoDbConnectorConfig.POLL_INTERVAL_MS, 10)
                .with(MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST, "dbit.*")
                .with(CommonConnectorConfig.TOPIC_PREFIX, "mongo")
                .with(MongoDbConnectorConfig.SKIPPED_OPERATIONS, "u")
                .build();

        // Set up the replication context for connections ...
        context = new MongoDbTaskContext(config);

        // Cleanup database
        TestHelper.cleanDatabase(mongo, "dbit");

        // Start the connector ...
        start(MongoDbConnector.class, config);
        waitForStreamingRunning("mongodb", "mongo");

        // ---------------------------------------------------------------------------------------------------------------
        // Create and then update a document
        // ---------------------------------------------------------------------------------------------------------------
        // Testing.Debug.enable();
        AtomicReference<String> id = new AtomicReference<>();
        try (var client = connect()) {
            MongoDatabase db1 = client.getDatabase("dbit");
            MongoCollection<Document> coll = db1.getCollection("arbitrary");
            coll.drop();

            // Insert the document with a generated ID ...
            Document doc = Document.parse("{\"a\": 1, \"b\": 2}");
            InsertOneOptions insertOptions = new InsertOneOptions().bypassDocumentValidation(true);
            coll.insertOne(doc, insertOptions);

            // Find the document to get the generated ID ...
            doc = coll.find().first();
            Testing.debug("Document: " + doc);
            id.set(doc.getObjectId("_id").toString());
            Testing.debug("Document ID: " + id.get());
        }

        SourceRecords insert = consumeRecordsByTopic(1);
        assertThat(insert.recordsForTopic("mongo.dbit.arbitrary")).hasSize(1);

        try (var client = connect()) {
            MongoDatabase db1 = client.getDatabase("dbit");
            MongoCollection<Document> coll = db1.getCollection("arbitrary");

            // Find the document ...
            Document doc = coll.find().first();
            Testing.debug("Document: " + doc);
            Document filter = Document.parse("{\"a\": 1}");
            Document operation = Document.parse("{ \"$set\": { \"b\": 10 } }");
            coll.updateOne(filter, operation);

            doc = coll.find().first();
            Testing.debug("Document: " + doc);
        }

        try (var client = connect()) {
            MongoDatabase db1 = client.getDatabase("dbit");
            MongoCollection<Document> coll = db1.getCollection("arbitrary");

            // Find the document ...
            Document doc = coll.find().first();
            Testing.debug("Document: " + doc);
            Document filter = Document.parse("{\"a\": 1}");

            // delete
            coll.deleteOne(filter);

            doc = coll.find().first();
            Testing.debug("Document: " + doc);
        }

        // Next should be the delete but not the skipped update
        SourceRecords delete = consumeRecordsByTopic(1);
        assertThat(delete.recordsForTopic("mongo.dbit.arbitrary")).hasSize(1);
        SourceRecord deleteRecord = delete.allRecordsInOrder().get(0);
        validate(deleteRecord);
        verifyDeleteOperation(deleteRecord);
    }

    @Test
    @FixFor("DBZ-1168")
    public void shouldConsumeAllEventsFromDatabaseWithCustomAuthSource() throws InterruptedException, IOException {

        final String authDbName = "authdb";

        // Use the DB configuration to define the connector's configuration ...
        config = TestHelper.getConfiguration(mongo).edit()
                .with(MongoDbConnectorConfig.POLL_INTERVAL_MS, 10)
                .with(MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST, "dbit.*")
                .with(CommonConnectorConfig.TOPIC_PREFIX, "mongo")
                .build();

        // Set up the replication context for connections ...
        context = new MongoDbTaskContext(config);

        // Cleanup database
        TestHelper.cleanDatabase(mongo, "dbit");

        try (var client = connect()) {
            final MongoDatabase db = client.getDatabase(authDbName);
            try {
                db.runCommand(BsonDocument.parse("{dropUser: \"dbz\"}"));
            }
            catch (Exception e) {
                logger.info("Expected error while dropping user", e);
            }
            db.runCommand(BsonDocument.parse(
                    "{createUser: \"dbz\", pwd: \"pass\", roles: [{role: \"readAnyDatabase\", db: \"admin\"}]}"));
        }

        // Before starting the connector, add data to the databases ...
        storeDocuments("dbit", "simpletons", "simple_objects.json");
        storeDocuments("dbit", "restaurants", "restaurants1.json");

        // Use the DB configuration to define the connector's configuration ...
        config = TestHelper.getConfiguration(mongo).edit()
                .with(MongoDbConnectorConfig.USER, "dbz")
                .with(MongoDbConnectorConfig.PASSWORD, "pass")
                .with(MongoDbConnectorConfig.AUTH_SOURCE, authDbName)
                .with(MongoDbConnectorConfig.POLL_INTERVAL_MS, 10)
                .with(MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST, "dbit.*")
                .with(CommonConnectorConfig.TOPIC_PREFIX, "mongo")
                .build();

        // Set up the replication context for connections ...
        context = new MongoDbTaskContext(config);

        // Start the connector ...
        start(MongoDbConnector.class, config);

        // ---------------------------------------------------------------------------------------------------------------
        // Consume all of the events due to startup and initialization of the database
        // ---------------------------------------------------------------------------------------------------------------
        SourceRecords records = consumeRecordsByTopic(12);
        records.topics().forEach(System.out::println);
        assertThat(records.recordsForTopic("mongo.dbit.simpletons").size()).isEqualTo(6);
        assertThat(records.recordsForTopic("mongo.dbit.restaurants").size()).isEqualTo(6);
        assertThat(records.topics().size()).isEqualTo(2);
        AtomicBoolean foundLast = new AtomicBoolean(false);
        records.forEach(record -> {
            // Check that all records are valid, and can be serialized and deserialized ...
            validate(record);
            verifyFromInitialSnapshot(record, foundLast);
            verifyReadOperation(record);
        });
        assertThat(foundLast.get()).isTrue();

        // At this point, the connector has performed the initial sync and awaits changes ...

        // ---------------------------------------------------------------------------------------------------------------
        // Store more documents while the connector is still running
        // ---------------------------------------------------------------------------------------------------------------
        storeDocuments("dbit", "restaurants", "restaurants2.json");

        // Wait until we can consume the 4 documents we just added ...
        SourceRecords records2 = consumeRecordsByTopic(4);
        assertThat(records2.recordsForTopic("mongo.dbit.restaurants").size()).isEqualTo(4);
        assertThat(records2.topics().size()).isEqualTo(1);
        records2.forEach(record -> {
            // Check that all records are valid, and can be serialized and deserialized ...
            validate(record);
            verifyNotFromInitialSnapshot(record);
            verifyCreateOperation(record);
        });

        // ---------------------------------------------------------------------------------------------------------------
        // Stop the connector
        // ---------------------------------------------------------------------------------------------------------------
        stopConnector();
    }

    @Test
    @FixFor("DBZ-6434")
    public void testMissingAuthenticationCredentials() {
        final String authDbName = "authdb";

        // Use the DB configuration to define the connector's configuration ...
        config = TestHelper.getConfiguration(mongo).edit()
                .with(MongoDbConnectorConfig.USER, "dbz")
                .with(MongoDbConnectorConfig.AUTH_SOURCE, authDbName)
                .with(MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST, "dbit.*")
                .with(CommonConnectorConfig.TOPIC_PREFIX, "mongo")
                .build();

        // Set up the replication context for connections ...
        context = new MongoDbTaskContext(config);

        // Start the connector ...
        start(MongoDbConnector.class, config);

        assertConnectorNotRunning();
        stopConnector();
    }

    @Test
    @FixFor("DBZ-4575")
    public void shouldConsumeEventsOnlyFromIncludedDatabasesWithRegexFilter() throws IOException, InterruptedException {
        shouldConsumeEventsOnlyFromIncludedDatabases(FiltersMatchMode.REGEX);
    }

    @Test
    @FixFor("DBZ-4575")
    public void shouldConsumeEventsOnlyFromIncludedDatabasesWithLiteralFilter() throws IOException, InterruptedException {
        shouldConsumeEventsOnlyFromIncludedDatabases(FiltersMatchMode.LITERAL);
    }

    public void shouldConsumeEventsOnlyFromIncludedDatabases(FiltersMatchMode filtersMatchMode) throws InterruptedException, IOException {

        // Use the DB configuration to define the connector's configuration ...
        config = TestHelper.getConfiguration(mongo).edit()
                .with(MongoDbConnectorConfig.POLL_INTERVAL_MS, 10)
                .with(MongoDbConnectorConfig.DATABASE_INCLUDE_LIST, "inc")
                .with(MongoDbConnectorConfig.FILTERS_MATCH_MODE, filtersMatchMode)
                .with(CommonConnectorConfig.TOPIC_PREFIX, "mongo")
                .build();

        // Set up the replication context for connections ...
        context = new MongoDbTaskContext(config);

        // Cleanup database
        TestHelper.cleanDatabase(mongo, "inc");
        TestHelper.cleanDatabase(mongo, "exc");

        // Before starting the connector, add data to the databases ...
        storeDocuments("inc", "simpletons", "simple_objects.json");
        storeDocuments("exc", "restaurants", "restaurants1.json");

        // Start the connector ...
        start(MongoDbConnector.class, config);

        // ---------------------------------------------------------------------------------------------------------------
        // Consume all of the events due to startup and initialization of the database
        // ---------------------------------------------------------------------------------------------------------------
        SourceRecords records = consumeRecordsByTopic(6);
        records.topics().forEach(System.out::println);
        assertThat(records.recordsForTopic("mongo.inc.simpletons").size()).isEqualTo(6);
        assertThat(records.recordsForTopic("mongo.exc.restaurants")).isNull();
        assertThat(records.topics().size()).isEqualTo(1);
        AtomicBoolean foundLast = new AtomicBoolean(false);
        records.forEach(record -> {
            // Check that all records are valid, and can be serialized and deserialized ...
            validate(record);
            verifyFromInitialSnapshot(record, foundLast);
            verifyReadOperation(record);
        });
        assertThat(foundLast.get()).isTrue();

        // At this point, the connector has performed the initial sync and awaits changes ...

        // ---------------------------------------------------------------------------------------------------------------
        // Store more documents while the connector is still running
        // ---------------------------------------------------------------------------------------------------------------
        storeDocuments("exc", "restaurants", "restaurants2.json");
        storeDocuments("inc", "restaurants", "restaurants2.json");

        // Wait until we can consume the 4 documents we just added ...
        SourceRecords records2 = consumeRecordsByTopic(4);
        assertThat(records2.recordsForTopic("mongo.exc.restaurants")).isNull();
        assertThat(records2.recordsForTopic("mongo.inc.restaurants").size()).isEqualTo(4);
        assertThat(records2.topics().size()).isEqualTo(1);
        records2.forEach(record -> {
            // Check that all records are valid, and can be serialized and deserialized ...
            validate(record);
            verifyNotFromInitialSnapshot(record);
            verifyCreateOperation(record);
        });
        // ---------------------------------------------------------------------------------------------------------------
        // Stop the connector
        // ---------------------------------------------------------------------------------------------------------------
        stopConnector();
    }

    @Test
    public void shouldConsumeEventsOnlyFromNonExcludedCollectionsWithRegexFilter() throws IOException, InterruptedException {
        shouldConsumeEventsOnlyFromNonExcludedCollections(FiltersMatchMode.REGEX);
    }

    @Test
    public void shouldConsumeEventsOnlyFromNonExcludedCollectionsWithLiteralFilter() throws IOException, InterruptedException {
        shouldConsumeEventsOnlyFromNonExcludedCollections(FiltersMatchMode.LITERAL);
    }

    public void shouldConsumeEventsOnlyFromNonExcludedCollections(FiltersMatchMode matchMode) throws InterruptedException, IOException {

        var dbIncludeList = (matchMode == FiltersMatchMode.REGEX) ? "db.*" : "dbA,dbB";
        var collExcludeList = (matchMode == FiltersMatchMode.REGEX) ? ".*simpletons" : "dbA.simpletons,dbB.simpletons";

        // Use the DB configuration to define the connector's configuration ...
        config = TestHelper.getConfiguration(mongo).edit()
                .with(MongoDbConnectorConfig.POLL_INTERVAL_MS, 10)
                .with(MongoDbConnectorConfig.DATABASE_INCLUDE_LIST, dbIncludeList)
                .with(MongoDbConnectorConfig.COLLECTION_EXCLUDE_LIST, collExcludeList)
                .with(MongoDbConnectorConfig.FILTERS_MATCH_MODE, matchMode)
                .with(CommonConnectorConfig.TOPIC_PREFIX, "mongo")
                .build();

        // Set up the replication context for connections ...
        context = new MongoDbTaskContext(config);

        // Cleanup database
        TestHelper.cleanDatabase(mongo, "dbA");
        TestHelper.cleanDatabase(mongo, "dbB");
        TestHelper.cleanDatabase(mongo, "cDB");

        // Before starting the connector, add data to the databases ...
        storeDocuments("dbA", "restaurants", "restaurants1.json");
        storeDocuments("dbB", "restaurants", "restaurants1.json");
        storeDocuments("cDB", "restaurants", "restaurants1.json");
        storeDocuments("dbA", "simpletons", "simple_objects.json");
        storeDocuments("dbB", "simpletons", "simple_objects.json");

        // Start the connector ...
        start(MongoDbConnector.class, config);

        // ---------------------------------------------------------------------------------------------------------------
        // Consume all of the events due to startup and initialization of the database
        // ---------------------------------------------------------------------------------------------------------------
        SourceRecords records = consumeRecordsByTopic(5 * 6);
        records.topics().forEach(System.out::println);
        assertThat(records.topics().size()).isEqualTo(2);
        assertThat(records.recordsForTopic("mongo.dbA.restaurants").size()).isEqualTo(6);
        assertThat(records.recordsForTopic("mongo.dbB.restaurants").size()).isEqualTo(6);
        assertThat(records.recordsForTopic("mongo.dbA.simpletons")).isNull();
        assertThat(records.recordsForTopic("mongo.dbB.simpletons")).isNull();
        assertThat(records.recordsForTopic("mongo.cDB.restaurants")).isNull();

        AtomicBoolean foundLast = new AtomicBoolean(false);
        records.forEach(record -> {
            // Check that all records are valid, and can be serialized and deserialized ...
            validate(record);
            verifyFromInitialSnapshot(record, foundLast);
            verifyReadOperation(record);
        });
        assertThat(foundLast.get()).isTrue();

        // At this point, the connector has performed the initial sync and awaits changes ...

        // ---------------------------------------------------------------------------------------------------------------
        // Store more documents while the connector is still running
        // ---------------------------------------------------------------------------------------------------------------
        storeDocuments("dbA", "restaurants", "restaurants2.json");
        storeDocuments("cDB", "restaurants", "restaurants2.json");

        // Wait until we can consume the 4 documents we just added ...
        SourceRecords records2 = consumeRecordsByTopic(4);
        assertThat(records2.recordsForTopic("mongo.dbA.restaurants").size()).isEqualTo(4);
        assertThat(records2.recordsForTopic("mongo.cDB.restaurants")).isNull();
        assertThat(records2.topics().size()).isEqualTo(1);
        records2.forEach(record -> {
            // Check that all records are valid, and can be serialized and deserialized ...
            validate(record);
            verifyNotFromInitialSnapshot(record);
            verifyCreateOperation(record);
        });
        // ---------------------------------------------------------------------------------------------------------------
        // Stop the connector
        // ---------------------------------------------------------------------------------------------------------------
        stopConnector();
    }

    @Test
    @FixFor("DBZ-1767")
    public void shouldSupportDbRef() throws InterruptedException, IOException {

        // Use the DB configuration to define the connector's configuration ...
        config = TestHelper.getConfiguration(mongo).edit()
                .with(MongoDbConnectorConfig.POLL_INTERVAL_MS, 10)
                .with(MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST, "dbit.*")
                .with(CommonConnectorConfig.TOPIC_PREFIX, "mongo")
                .build();

        // Set up the replication context for connections ...
        context = new MongoDbTaskContext(config);

        // Cleanup database
        TestHelper.cleanDatabase(mongo, "dbit");

        // Before starting the connector, add data to the databases ...
        storeDocuments("dbit", "spec", "spec_objects.json");

        // Set up the replication context for connections ...
        context = new MongoDbTaskContext(config);

        // Start the connector ...
        start(MongoDbConnector.class, config);

        // ---------------------------------------------------------------------------------------------------------------
        // Consume all of the events due to startup and initialization of the database
        // ---------------------------------------------------------------------------------------------------------------
        SourceRecords records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic("mongo.dbit.spec").size()).isEqualTo(1);
        assertThat(records.topics().size()).isEqualTo(1);
        AtomicBoolean foundLast = new AtomicBoolean(false);
        records.forEach(record -> {
            // Check that all records are valid, and can be serialized and deserialized ...
            validate(record);
            verifyFromInitialSnapshot(record, foundLast);
            verifyReadOperation(record);
        });
        assertThat(foundLast.get()).isTrue();

        // At this point, the connector has performed the initial sync and awaits changes ...

        // ---------------------------------------------------------------------------------------------------------------
        // Store another document while the connector is still running
        // ---------------------------------------------------------------------------------------------------------------
        try (var client = connect()) {
            client.getDatabase("dbit").getCollection("spec")
                    .insertOne(Document.parse("{ '_id' : 2, 'data' : { '$ref' : 'a2', '$id' : 4, '$db' : 'b2' } }"));
        }

        SourceRecords records2 = consumeRecordsByTopic(1);
        assertThat(records2.recordsForTopic("mongo.dbit.spec").size()).isEqualTo(1);
        assertThat(records2.topics().size()).isEqualTo(1);
        records2.forEach(record -> {
            // Check that all records are valid, and can be serialized and deserialized ...
            validate(record);
            verifyNotFromInitialSnapshot(record);
            verifyCreateOperation(record);
        });

        // ---------------------------------------------------------------------------------------------------------------
        // Stop the connector
        // ---------------------------------------------------------------------------------------------------------------
        stopConnector();
    }

    @Test
    @FixFor("DBZ-865 and DBZ-1242")
    public void shouldConsumeEventsFromCollectionWithReplacedTopicName() throws InterruptedException, IOException {
        // This captures all logged messages, allowing us to verify log message was written.
        final LogInterceptor logInterceptor = new LogInterceptor(MongoDbSchema.class);

        // Use the DB configuration to define the connector's configuration ...
        config = TestHelper.getConfiguration(mongo).edit()
                .with(MongoDbConnectorConfig.POLL_INTERVAL_MS, 10)
                .with(MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST, "dbit.dbz865.*")
                .with(CommonConnectorConfig.TOPIC_PREFIX, "mongo")
                .build();

        // Set up the replication context for connections ...
        context = new MongoDbTaskContext(config);

        // Cleanup database
        TestHelper.cleanDatabase(mongo, "dbit");

        try (var client = connect()) {
            MongoDatabase db1 = client.getDatabase("dbit");
            MongoCollection<Document> coll = db1.getCollection("dbz865_my@collection");
            coll.drop();

            Document doc = Document.parse("{\"a\": 1, \"b\": 2}");
            InsertOneOptions insertOptions = new InsertOneOptions().bypassDocumentValidation(true);
            coll.insertOne(doc, insertOptions);
        }

        // Start the connector ...
        start(MongoDbConnector.class, config);

        // ---------------------------------------------------------------------------------------------------------------
        // Consume all of the events due to startup and initialization of the database
        // ---------------------------------------------------------------------------------------------------------------
        SourceRecords records = consumeRecordsByTopic(12);
        records.topics().forEach(System.out::println);
        assertThat(records.recordsForTopic("mongo.dbit.dbz865_my_collection")).hasSize(1);
        assertThat(records.topics().size()).isEqualTo(1);
        AtomicBoolean foundLast = new AtomicBoolean(false);
        records.forEach(record -> {
            // Check that all records are valid, and can be serialized and deserialized ...
            validate(record);
            verifyFromInitialSnapshot(record, foundLast);
            verifyReadOperation(record);
        });
        assertThat(foundLast.get()).isTrue();

        // ---------------------------------------------------------------------------------------------------------------
        // Stop the connector
        // ---------------------------------------------------------------------------------------------------------------
        stopConnector(value -> assertThat(logInterceptor.containsWarnMessage(DatabaseSchema.NO_CAPTURED_DATA_COLLECTIONS_WARNING)).isFalse());
    }

    @Test
    @FixFor("DBZ-1242")
    public void testEmptySchemaWarningAfterApplyingCollectionFilters() throws Exception {
        // This captures all logged messages, allowing us to verify log message was written.
        final LogInterceptor logInterceptor = new LogInterceptor(MongoDbSchema.class);

        // Use the DB configuration to define the connector's configuration...
        config = TestHelper.getConfiguration(mongo).edit()
                .with(MongoDbConnectorConfig.POLL_INTERVAL_MS, 10)
                .with(MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST, "dbit.dbz865.my_products")
                .with(CommonConnectorConfig.TOPIC_PREFIX, "mongo")
                .build();

        // Set up the replication context for connections...
        context = new MongoDbTaskContext(config);

        // Cleanup database
        TestHelper.cleanDatabase(mongo, "dbit");

        try (var client = connect()) {
            MongoDatabase db1 = client.getDatabase("dbit");
            MongoCollection<Document> coll = db1.getCollection("dbz865_my@collection");
            coll.drop();

            Document doc = Document.parse("{\"a\": 1, \"b\": 2}");
            InsertOneOptions insertOptions = new InsertOneOptions().bypassDocumentValidation(true);
            coll.insertOne(doc, insertOptions);
        }

        // Start the connector...
        start(MongoDbConnector.class, config);

        // Consume all records
        consumeRecordsByTopic(12);

        stopConnector(value -> assertThat(logInterceptor.containsWarnMessage(DatabaseSchema.NO_CAPTURED_DATA_COLLECTIONS_WARNING)).isTrue());
    }

    protected void verifyFromInitialSnapshot(SourceRecord record, AtomicBoolean foundLast) {
        if (record.sourceOffset().containsKey(SourceInfo.INITIAL_SYNC)) {
            assertThat(record.sourceOffset().containsKey(SourceInfo.INITIAL_SYNC)).isTrue();
            Struct value = (Struct) record.value();
            assertThat(value.getStruct(Envelope.FieldName.SOURCE).getString(SourceInfo.SNAPSHOT_KEY)).isEqualTo("true");
        }
        else {
            // Only the last record in the initial sync should be marked as not being part of the initial sync ...
            assertThat(foundLast.getAndSet(true)).isFalse();
            Struct value = (Struct) record.value();
            assertThat(value.getStruct(Envelope.FieldName.SOURCE).getString(SourceInfo.SNAPSHOT_KEY)).isEqualTo("last");
        }
    }

    @Test
    @FixFor("DBZ-1215")
    public void shouldConsumeTransaction() throws InterruptedException, IOException {
        // Use the DB configuration to define the connector's configuration ...
        config = TestHelper.getConfiguration(mongo).edit()
                .with(MongoDbConnectorConfig.POLL_INTERVAL_MS, 10)
                .with(MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST, "dbit.*")
                .with(CommonConnectorConfig.TOPIC_PREFIX, "mongo")
                .build();

        // Set up the replication context for connections ...
        context = new MongoDbTaskContext(config);

        if (!TestHelper.transactionsSupported()) {
            logger.info("Test not executed, transactions not supported in the server");
            return;
        }

        // Cleanup database
        TestHelper.cleanDatabase(mongo, "dbit");

        // Before starting the connector, add data to the databases ...
        storeDocuments("dbit", "simpletons", "simple_objects.json");
        storeDocuments("dbit", "restaurants", "restaurants1.json");

        // Start the connector ...
        start(MongoDbConnector.class, config);

        // ---------------------------------------------------------------------------------------------------------------
        // Consume all of the events due to startup and initialization of the database
        // ---------------------------------------------------------------------------------------------------------------
        SourceRecords records = consumeRecordsByTopic(12);
        records.topics().forEach(System.out::println);
        assertThat(records.recordsForTopic("mongo.dbit.simpletons").size()).isEqualTo(6);
        assertThat(records.recordsForTopic("mongo.dbit.restaurants").size()).isEqualTo(6);
        assertThat(records.topics().size()).isEqualTo(2);
        AtomicBoolean foundLast = new AtomicBoolean(false);
        records.forEach(record -> {
            // Check that all records are valid, and can be serialized and deserialized ...
            validate(record);
            verifyFromInitialSnapshot(record, foundLast);
            verifyReadOperation(record);
        });
        assertThat(foundLast.get()).isTrue();

        // At this point, the connector has performed the initial sync and awaits changes ...

        // ---------------------------------------------------------------------------------------------------------------
        // Store more documents while the connector is still running
        // ---------------------------------------------------------------------------------------------------------------
        storeDocumentsInTx("dbit", "restaurants", "restaurants2.json");

        // Wait until we can consume the 4 documents we just added ...
        SourceRecords records2 = consumeRecordsByTopic(4);
        assertThat(records2.recordsForTopic("mongo.dbit.restaurants").size()).isEqualTo(4);
        assertThat(records2.topics().size()).isEqualTo(1);
        final AtomicLong txOrder = new AtomicLong(0);
        records2.forEach(record -> {
            // Check that all records are valid, and can be serialized and deserialized ...
            validate(record);
            verifyNotFromInitialSnapshot(record);
            verifyCreateOperation(record);
        });

        // ---------------------------------------------------------------------------------------------------------------
        // Stop the connector
        // ---------------------------------------------------------------------------------------------------------------
        stopConnector();

        // ---------------------------------------------------------------------------------------------------------------
        // Store more documents while the connector is NOT running
        // ---------------------------------------------------------------------------------------------------------------
        storeDocumentsInTx("dbit", "restaurants", "restaurants3.json");

        // ---------------------------------------------------------------------------------------------------------------
        // Start the connector and we should only see the documents added since it was stopped
        // ---------------------------------------------------------------------------------------------------------------
        start(MongoDbConnector.class, config);

        // Wait until we can consume the 4 documents we just added ...
        SourceRecords records3 = consumeRecordsByTopic(5);
        assertThat(records3.recordsForTopic("mongo.dbit.restaurants").size()).isEqualTo(5);
        assertThat(records3.topics().size()).isEqualTo(1);
        txOrder.set(0);
        records3.forEach(record -> {
            // Check that all records are valid, and can be serialized and deserialized ...
            validate(record);
            verifyNotFromInitialSnapshot(record);
            verifyCreateOperation(record);
        });
    }

    @Test
    @FixFor("DBZ-1215")
    public void shouldResumeTransactionInMiddle() throws InterruptedException, IOException {
        // Use the DB configuration to define the connector's configuration ...
        config = TestHelper.getConfiguration(mongo).edit()
                .with(MongoDbConnectorConfig.POLL_INTERVAL_MS, 10)
                .with(MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST, "dbit.*")
                .with(CommonConnectorConfig.TOPIC_PREFIX, "mongo")
                .build();

        // Set up the replication context for connections ...
        context = new MongoDbTaskContext(config);

        if (!TestHelper.transactionsSupported()) {
            logger.info("Test not executed, transactions not supported in the server");
            return;
        }

        // Cleanup database
        TestHelper.cleanDatabase(mongo, "dbit");

        // Before starting the connector, add data to the databases ...
        storeDocuments("dbit", "simpletons", "simple_objects.json");
        storeDocuments("dbit", "restaurants", "restaurants1.json");

        // Start the connector and terminate it when third event from transaction arrives
        startAndConsumeTillEnd(MongoDbConnector.class, config, record -> {
            final Struct struct = (Struct) record.value();
            final String name = struct.getString("after");
            return "Taste The Tropics Ice Cream".contains(name);
        });

        // ---------------------------------------------------------------------------------------------------------------
        // Consume all of the events due to startup and initialization of the database
        // ---------------------------------------------------------------------------------------------------------------
        SourceRecords records = consumeRecordsByTopic(12);
        records.topics().forEach(System.out::println);
        assertThat(records.recordsForTopic("mongo.dbit.simpletons").size()).isEqualTo(6);
        assertThat(records.recordsForTopic("mongo.dbit.restaurants").size()).isEqualTo(6);
        assertThat(records.topics().size()).isEqualTo(2);
        AtomicBoolean foundLast = new AtomicBoolean(false);
        records.forEach(record -> {
            // Check that all records are valid, and can be serialized and deserialized ...
            validate(record);
            verifyFromInitialSnapshot(record, foundLast);
            verifyReadOperation(record);
        });
        assertThat(foundLast.get()).isTrue();

        // At this point, the connector has performed the initial sync and awaits changes ...

        // ---------------------------------------------------------------------------------------------------------------
        // Store more documents while the connector is still running, connector should be stopped
        // after second record
        // ---------------------------------------------------------------------------------------------------------------
        storeDocumentsInTx("dbit", "restaurants", "restaurants2.json");

        // Wait until we can consume the two records of those documents we just added ...
        SourceRecords records2 = consumeRecordsByTopic(2);
        assertThat(records2.recordsForTopic("mongo.dbit.restaurants").size()).isEqualTo(2);
        assertThat(records2.topics().size()).isEqualTo(1);
        final AtomicLong txOrder = new AtomicLong(0);
        records2.forEach(record -> {
            // Check that all records are valid, and can be serialized and deserialized ...
            validate(record);
            verifyNotFromInitialSnapshot(record);
            verifyCreateOperation(record);
        });

        // ---------------------------------------------------------------------------------------------------------------
        // Stop the connector
        // ---------------------------------------------------------------------------------------------------------------
        stopConnector();

        // ---------------------------------------------------------------------------------------------------------------
        // Store more documents while the connector is NOT running
        // ---------------------------------------------------------------------------------------------------------------
        storeDocumentsInTx("dbit", "restaurants", "restaurants3.json");

        // ---------------------------------------------------------------------------------------------------------------
        // Start the connector and we should only see the rest of transaction
        // and the documents added since it was stopped
        // ---------------------------------------------------------------------------------------------------------------
        start(MongoDbConnector.class, config);

        // Wait until we can consume 2 (for incomplete transaction) + 5 (new documents added)
        final int recCount = 7;
        SourceRecords records3 = consumeRecordsByTopic(recCount);
        assertThat(records3.recordsForTopic("mongo.dbit.restaurants").size()).isEqualTo(recCount);
        assertThat(records3.topics().size()).isEqualTo(1);
        final List<Long> expectedTxOrd = Collect.arrayListOf(3L, 4L, 1L, 2L, 3L, 4L, 5L);
        records3.forEach(record -> {
            // Check that all records are valid, and can be serialized and deserialized ...
            validate(record);
            verifyNotFromInitialSnapshot(record);
            verifyCreateOperation(record);
        });
    }

    @Test
    @FixFor("DBZ-2116")
    public void shouldSnapshotDocumentContainingFieldNamedOp() throws Exception {
        // Use the DB configuration to define the connector's configuration ...
        config = TestHelper.getConfiguration(mongo).edit()
                .with(MongoDbConnectorConfig.POLL_INTERVAL_MS, 10)
                .with(MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST, "dbit.*")
                .with(CommonConnectorConfig.TOPIC_PREFIX, "mongo")
                .build();

        // Set up the replication context for connections ...
        context = new MongoDbTaskContext(config);

        // Cleanup database
        TestHelper.cleanDatabase(mongo, "dbit");

        // Before starting the connector, add data to the databases ...
        storeDocuments("dbit", "fieldnamedop", "fieldnamedop.json");

        // Start the connector ...
        start(MongoDbConnector.class, config);

        // ---------------------------------------------------------------------------------------------------------------
        // Consume all of the events due to startup and initialization of the database
        // ---------------------------------------------------------------------------------------------------------------
        SourceRecords records = consumeRecordsByTopic(2);
        assertThat(records.recordsForTopic("mongo.dbit.fieldnamedop").size()).isEqualTo(2);
        assertThat(records.topics().size()).isEqualTo(1);
        AtomicBoolean foundLast = new AtomicBoolean(false);
        records.forEach(record -> {
            // Check that all records are valid, and can be serialized and deserialized ...
            validate(record);
            verifyFromInitialSnapshot(record, foundLast);
            verifyReadOperation(record);
        });
        assertThat(foundLast.get()).isTrue();

        SourceRecord record = records.recordsForTopic("mongo.dbit.fieldnamedop").get(0);
        assertThat(((Struct) record.value()).get("op")).isEqualTo("r");

        Document after = Document.parse((String) ((Struct) record.value()).get("after"));
        assertThat(after.get("op")).isEqualTo("foo");

        record = records.recordsForTopic("mongo.dbit.fieldnamedop").get(1);
        assertThat(((Struct) record.value()).get("op")).isEqualTo("r");

        after = Document.parse((String) ((Struct) record.value()).get("after"));
        assertThat(after.get("op")).isEqualTo("bar");
    }

    @Test
    @FixFor("DBZ-2496")
    public void shouldFilterItemsInCollectionWhileTakingSnapshot() throws Exception {
        // Use the DB configuration to define the connector's configuration ...
        config = TestHelper.getConfiguration(mongo).edit()
                .with(MongoDbConnectorConfig.POLL_INTERVAL_MS, 10)
                .with(MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST, "dbit.*")
                .with(CommonConnectorConfig.TOPIC_PREFIX, "mongo")
                .with(MongoDbConnectorConfig.SNAPSHOT_FILTER_QUERY_BY_COLLECTION, "dbit.simpletons,dbit.restaurants1,dbit.restaurants4")
                .with(MongoDbConnectorConfig.SNAPSHOT_FILTER_QUERY_BY_COLLECTION + "." + "dbit.simpletons", "{ \"_id\": { \"$gt\": 4 } }")
                .with(MongoDbConnectorConfig.SNAPSHOT_FILTER_QUERY_BY_COLLECTION + "." + "dbit.restaurants1",
                        "{ $or: [ { cuisine: \"American \"}, { \"grades.grade\": \"Z\" } ] }")
                .with(MongoDbConnectorConfig.SNAPSHOT_FILTER_QUERY_BY_COLLECTION + "." + "dbit.restaurants4", "{ cuisine: \"American \" , borough: \"Manhattan\"  }")
                .build();

        // Set up the replication context for connections ...
        context = new MongoDbTaskContext(config);

        // Cleanup database
        TestHelper.cleanDatabase(mongo, "dbit");

        // Before starting the connector, add data to the databases ...

        // Before starting the connector, add data to the databases ...
        storeDocuments("dbit", "simpletons", "simple_objects.json");
        storeDocuments("dbit", "restaurants1", "restaurants1.json");
        storeDocuments("dbit", "restaurants2", "restaurants2.json");
        storeDocuments("dbit", "restaurants4", "restaurants4.json");

        // Start the connector ...
        start(MongoDbConnector.class, config);

        // ---------------------------------------------------------------------------------------------------------------
        // Consume all of the events due to startup and initialization of the database
        // ---------------------------------------------------------------------------------------------------------------
        SourceRecords records = consumeRecordsByTopic(15);
        assertThat(records.topics().size()).isEqualTo(4);
        assertThat(records.recordsForTopic("mongo.dbit.simpletons").size()).isEqualTo(4);
        assertThat(records.recordsForTopic("mongo.dbit.restaurants1").size()).isEqualTo(3);
        assertThat(records.recordsForTopic("mongo.dbit.restaurants2").size()).isEqualTo(4);
        assertThat(records.recordsForTopic("mongo.dbit.restaurants4").size()).isEqualTo(4);
        assertNoRecordsToConsume();

        stopConnector();
    }

    @Test
    @FixFor("DBZ-2456")
    public void shouldSelectivelySnapshot() throws InterruptedException {
        config = TestHelper.getConfiguration(mongo).edit()
                .with(MongoDbConnectorConfig.POLL_INTERVAL_MS, 10)
                .with(MongoDbConnectorConfig.SNAPSHOT_MODE, MongoDbConnectorConfig.SnapshotMode.INITIAL)
                .with(MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST, "dbit.*")
                .with(CommonConnectorConfig.SNAPSHOT_MODE_TABLES, "dbit.restaurants1")
                .with(CommonConnectorConfig.TOPIC_PREFIX, "mongo")
                .build();

        // Set up the replication context for connections ...
        context = new MongoDbTaskContext(config);

        // Cleanup database
        TestHelper.cleanDatabase(mongo, "dbit");

        // Before starting the connector, add data to the databases ...
        storeDocuments("dbit", "restaurants1", "restaurants1.json");
        storeDocuments("dbit", "restaurants2", "restaurants2.json");

        // Start the connector ...
        start(MongoDbConnector.class, config);
        waitForStreamingRunning("mongodb", "mongo");

        SourceRecords records = consumeRecordsByTopic(6);

        List<SourceRecord> restaurant1 = records.recordsForTopic("mongo.dbit.restaurants1");
        List<SourceRecord> restaurant2 = records.recordsForTopic("mongo.dbit.restaurants2");

        assertThat(restaurant1.size()).isEqualTo(6);
        assertThat(restaurant2).isNull();

        // Insert record
        final Instant timestamp = Instant.now();
        ObjectId objId = new ObjectId();
        Document obj = Document.parse("{\"name\": \"Brunos On The Boulevard\", \"restaurant_id\": \"40356151\"}");
        insertDocuments("dbit", "restaurants2", obj);

        // Consume records, should be 1, the insert
        records = consumeRecordsByTopic(1);
        assertThat(records.allRecordsInOrder().size()).isEqualTo(1);
        assertNoRecordsToConsume();

        stopConnector();
    }

    protected void verifyNotFromInitialSnapshot(SourceRecord record) {
        assertThat(record.sourceOffset().containsKey(SourceInfo.INITIAL_SYNC)).isFalse();
        Struct value = (Struct) record.value();
        assertThat(value.getStruct(Envelope.FieldName.SOURCE).getString(SourceInfo.SNAPSHOT_KEY)).isNull();
    }

    protected void verifyCreateOperation(SourceRecord record) {
        verifyOperation(record, Operation.CREATE);
    }

    protected void verifyReadOperation(SourceRecord record) {
        verifyOperation(record, Operation.READ);
    }

    protected void verifyUpdateOperation(SourceRecord record) {
        verifyOperation(record, Operation.UPDATE);
    }

    protected void verifyDeleteOperation(SourceRecord record) {
        verifyOperation(record, Operation.DELETE);
    }

    protected void verifyOperation(SourceRecord record, Operation expected) {
        Struct value = (Struct) record.value();
        assertThat(value.getString(Envelope.FieldName.OPERATION)).isEqualTo(expected.code());
    }

    protected void storeDocuments(String dbName, String collectionName, String pathOnClasspath) {
        try (var client = connect()) {
            Testing.debug("Storing in '" + dbName + "." + collectionName + "' documents loaded from from '" + pathOnClasspath + "'");
            MongoDatabase db1 = client.getDatabase(dbName);
            MongoCollection<Document> coll = db1.getCollection(collectionName);
            coll.drop();
            storeDocuments(coll, pathOnClasspath);
        }
    }

    protected void storeDocuments(MongoCollection<Document> collection, String pathOnClasspath) {
        InsertOneOptions insertOptions = new InsertOneOptions().bypassDocumentValidation(true);
        loadTestDocuments(pathOnClasspath).forEach(doc -> {
            assertThat(doc).isNotNull();
            assertThat(doc.size()).isGreaterThan(0);
            collection.insertOne(doc, insertOptions);
        });
    }

    protected void storeDocumentsInTx(String dbName, String collectionName, String pathOnClasspath) {
        try (var client = connect()) {
            Testing.debug("Storing in '" + dbName + "." + collectionName + "' documents loaded from from '" + pathOnClasspath + "'");
            MongoDatabase db1 = client.getDatabase(dbName);
            MongoCollection<Document> coll = db1.getCollection(collectionName);
            coll.drop();
            db1.createCollection(collectionName);
            final ClientSession session = client.startSession();

            MongoDatabase admin = client.getDatabase("admin");
            if (admin != null) {
                int timeout = Integer.parseInt(System.getProperty("mongo.transaction.lock.request.timeout.ms", "1000"));
                Testing.debug("Setting MongoDB transaction lock request timeout as '" + timeout + "ms'");
                admin.runCommand(session, new Document().append("setParameter", 1).append("maxTransactionLockRequestTimeoutMillis", timeout));
            }

            session.startTransaction();
            storeDocuments(session, coll, pathOnClasspath);
            session.commitTransaction();
        }
    }

    protected void storeDocuments(ClientSession session, MongoCollection<Document> collection, String pathOnClasspath) {
        InsertOneOptions insertOptions = new InsertOneOptions().bypassDocumentValidation(true);
        loadTestDocuments(pathOnClasspath).forEach(doc -> {
            assertThat(doc).isNotNull();
            assertThat(doc.size()).isGreaterThan(0);
            if (session == null) {
                collection.insertOne(doc, insertOptions);
            }
            else {
                collection.insertOne(session, doc, insertOptions);
            }
        });
    }

    protected List<Document> loadTestDocuments(String pathOnClasspath) {
        List<Document> results = new ArrayList<>();
        try (InputStream stream = Testing.Files.readResourceAsStream(pathOnClasspath)) {
            assertThat(stream).isNotNull();
            IoUtil.readLines(stream, line -> {
                Document doc = Document.parse(line);
                assertThat(doc.size()).isGreaterThan(0);
                results.add(doc);
            });
        }
        catch (IOException e) {
            fail("Unable to find or read file '" + pathOnClasspath + "': " + e.getMessage());
        }
        return results;
    }

    @Test
    @FixFor("DBZ-1198")
    public void shouldEmitHeartbeatMessages() throws InterruptedException, IOException {
        // Use the DB configuration to define the connector's configuration ...
        config = TestHelper.getConfiguration(mongo).edit()
                .with(MongoDbConnectorConfig.POLL_INTERVAL_MS, 10)
                .with(MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST, "dbit.mhb")
                .with(CommonConnectorConfig.TOPIC_PREFIX, "mongo")
                .with(Heartbeat.HEARTBEAT_INTERVAL, "1")
                .build();

        // Set up the replication context for connections ...
        context = new MongoDbTaskContext(config);

        // Cleanup database
        TestHelper.cleanDatabase(mongo, "dbit");

        try (var client = connect()) {
            MongoDatabase db1 = client.getDatabase("dbit");
            MongoCollection<Document> coll1 = db1.getCollection("mhb");
            coll1.drop();
            Document doc = Document.parse("{\"a\": 1, \"b\": 2}");
            InsertOneOptions insertOptions = new InsertOneOptions().bypassDocumentValidation(true);
            coll1.insertOne(doc, insertOptions);

            MongoCollection<Document> coll2 = db1.getCollection("nmhb");
            coll2.drop();
        }

        // Start the connector ...
        start(MongoDbConnector.class, config);
        SourceRecords records = consumeRecordsByTopic(1);
        assertThat(records.allRecordsInOrder()).hasSize(1);
        assertThat(records.recordsForTopic("mongo.dbit.mhb")).hasSize(1);
        Thread.sleep(1000);
        try (var client = connect()) {
            MongoDatabase db1 = client.getDatabase("dbit");
            MongoCollection<Document> coll = db1.getCollection("mhb");

            Document doc = Document.parse("{\"a\": 2, \"b\": 2}");
            InsertOneOptions insertOptions = new InsertOneOptions().bypassDocumentValidation(true);
            coll.insertOne(doc, insertOptions);
        }

        // Monitored collection event followed by heartbeat
        records = consumeRecordsByTopic(2);
        assertThat(records.recordsForTopic("mongo.dbit.mhb")).hasSize(1);
        final Map<String, ?> monitoredOffset = records.recordsForTopic("mongo.dbit.mhb").get(0).sourceOffset();
        final Integer monitoredTs = (Integer) monitoredOffset.get(SourceInfo.TIMESTAMP);
        final Integer monitoredOrd = (Integer) monitoredOffset.get(SourceInfo.ORDER);
        assertThat(records.recordsForTopic("__debezium-heartbeat.mongo")).hasSize(1);
        final Map<String, ?> hbAfterMonitoredOffset = records.recordsForTopic("__debezium-heartbeat.mongo").get(0).sourceOffset();

        // Change events are sent on empty cursor `getMore` batches. The first empty batch happens prior to the first monitored event
        assertThat(monitoredTs).isGreaterThanOrEqualTo((Integer) hbAfterMonitoredOffset.get(SourceInfo.TIMESTAMP));

        try (var client = connect()) {
            MongoDatabase db1 = client.getDatabase("dbit");
            MongoCollection<Document> coll = db1.getCollection("nmhb");

            Document doc = Document.parse("{\"a\": 3, \"b\": 2}");
            InsertOneOptions insertOptions = new InsertOneOptions().bypassDocumentValidation(true);
            coll.insertOne(doc, insertOptions);
        }

        // Heartbeat created by non-monitored collection event
        final int heartbeatRecordCount = 1;
        records = consumeRecordsByTopic(heartbeatRecordCount);
        final List<SourceRecord> heartbeatRecords = records.recordsForTopic("__debezium-heartbeat.mongo");
        assertThat(heartbeatRecords.size()).isGreaterThanOrEqualTo(1);
        heartbeatRecords.forEach(record -> {
            // Offset of the heartbeats should be greater than of the last monitored event
            final Map<String, ?> offset = record.sourceOffset();
            final Integer ts = (Integer) offset.get(SourceInfo.TIMESTAMP);
            final Integer ord = (Integer) offset.get(SourceInfo.ORDER);
            assertThat(ts > monitoredTs || (ts == monitoredTs && ord > monitoredOrd));
        });
        stopConnector();
    }

    @Test
    @FixFor("DBZ-1292")
    public void shouldOutputRecordsInCloudEventsFormat() throws Exception {
        config = TestHelper.getConfiguration(mongo).edit()
                .with(MongoDbConnectorConfig.POLL_INTERVAL_MS, 10)
                .with(MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST, "dbit.*")
                .with(CommonConnectorConfig.TOPIC_PREFIX, "mongo")
                .build();

        context = new MongoDbTaskContext(config);

        TestHelper.cleanDatabase(mongo, "dbit");
        storeDocuments("dbit", "restaurants", "restaurants1.json");
        start(MongoDbConnector.class, config);

        SourceRecords records = consumeRecordsByTopic(12);
        List<SourceRecord> topicRecords = records.recordsForTopic("mongo.dbit.restaurants");
        for (SourceRecord record : topicRecords) {
            CloudEventsConverterTest.shouldConvertToCloudEventsInJson(record, false);
            CloudEventsConverterTest.shouldConvertToCloudEventsInJsonWithDataAsAvro(record, false);
            CloudEventsConverterTest.shouldConvertToCloudEventsInAvro(record, "mongodb", "mongo", false);
        }

        storeDocuments("dbit", "restaurants", "restaurants2.json");

        // Wait until we can consume the 4 documents we just added ...
        SourceRecords records2 = consumeRecordsByTopic(4);
        List<SourceRecord> topicRecords2 = records2.recordsForTopic("mongo.dbit.restaurants");
        for (SourceRecord record : topicRecords2) {
            CloudEventsConverterTest.shouldConvertToCloudEventsInJson(record, false);
            CloudEventsConverterTest.shouldConvertToCloudEventsInJsonWithDataAsAvro(record, false);
            CloudEventsConverterTest.shouldConvertToCloudEventsInAvro(record, "mongodb", "mongo", false);
        }

        stopConnector();
    }

    @Test
    public void shouldGenerateRecordForInsertEvent() throws Exception {
        config = TestHelper.getConfiguration(mongo).edit()
                .with(MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST, "dbit.*")
                .with(CommonConnectorConfig.TOPIC_PREFIX, "mongo")
                .build();

        context = new MongoDbTaskContext(config);

        TestHelper.cleanDatabase(mongo, "dbit");

        start(MongoDbConnector.class, config);
        waitForStreamingRunning("mongodb", "mongo");

        // Insert record
        final Instant timestamp = Instant.now();
        ObjectId objId = new ObjectId();
        Document obj = new Document("_id", objId);
        insertDocuments("dbit", "c1", obj);

        // Consume records, should be 1, the insert
        final SourceRecords records = consumeRecordsByTopic(1);
        assertThat(records.allRecordsInOrder().size()).isEqualTo(1);
        assertNoRecordsToConsume();

        final SourceRecord deleteRecord = records.allRecordsInOrder().get(0);
        final Struct key = (Struct) deleteRecord.key();
        final Struct value = (Struct) deleteRecord.value();

        assertThat(key.schema()).isSameAs(deleteRecord.keySchema());
        assertThat(key.get("id")).isEqualTo(formatObjectId(objId));

        assertThat(value.schema()).isSameAs(deleteRecord.valueSchema());
        // assertThat(value.getString(Envelope.FieldName.BEFORE)).isNull();
        assertThat(value.getString(Envelope.FieldName.AFTER)).isEqualTo(obj.toJson(COMPACT_JSON_SETTINGS));
        assertThat(value.getString(Envelope.FieldName.OPERATION)).isEqualTo(Operation.CREATE.code());
        assertThat(value.getInt64(Envelope.FieldName.TIMESTAMP)).isGreaterThanOrEqualTo(timestamp.toEpochMilli());

        // final Struct actualSource = value.getStruct(Envelope.FieldName.SOURCE);
        // context.source().collectionEvent("rs0", new CollectionId("rs0", "dbit", "c1"));
        // assertThat(actualSource).isEqualTo(context.source().struct());
    }

    @Test
    public void shouldGenerateRecordForUpdateEvent() throws Exception {
        config = TestHelper.getConfiguration(mongo).edit()
                .with(MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST, "dbit.*")
                .with(CommonConnectorConfig.TOPIC_PREFIX, "mongo")
                .build();

        context = new MongoDbTaskContext(config);

        TestHelper.cleanDatabase(mongo, "dbit");

        start(MongoDbConnector.class, config);
        waitForStreamingRunning("mongodb", "mongo");

        // Insert record
        ObjectId objId = new ObjectId();
        Document obj = new Document("_id", objId);
        insertDocuments("dbit", "c1", obj);

        // Consume the insert
        consumeRecordsByTopic(1);
        assertNoRecordsToConsume();

        Document updateObj = new Document()
                .append("$set", new Document()
                        .append("name", "Sally"));

        final Instant timestamp = Instant.now();
        final Document filter = Document.parse("{\"_id\": {\"$oid\": \"" + objId + "\"}}");
        updateDocument("dbit", "c1", filter, updateObj);

        // Consume records, should be 1, the update
        final SourceRecords records = consumeRecordsByTopic(1);
        assertThat(records.allRecordsInOrder().size()).isEqualTo(1);
        assertNoRecordsToConsume();

        final SourceRecord deleteRecord = records.allRecordsInOrder().get(0);
        final Struct key = (Struct) deleteRecord.key();
        final Struct value = (Struct) deleteRecord.value();

        assertThat(key.schema()).isSameAs(deleteRecord.keySchema());
        assertThat(key.get("id")).isEqualTo(formatObjectId(objId));

        TestHelper.assertChangeStreamUpdate(
                objId,
                value,
                "{\"_id\": {\"$oid\": \"<OID>\"},\"name\": \"Sally\"}",
                null,
                "{\"name\": \"Sally\"}");

        assertThat(value.schema()).isSameAs(deleteRecord.valueSchema());
        assertThat(value.getString(Envelope.FieldName.OPERATION)).isEqualTo(Operation.UPDATE.code());
        assertThat(value.getInt64(Envelope.FieldName.TIMESTAMP)).isGreaterThanOrEqualTo(timestamp.toEpochMilli());

        // final Struct actualSource = value.getStruct(Envelope.FieldName.SOURCE);
        // context.source().collectionEvent("rs0", new CollectionId("rs0", "dbit", "c1"));
        // assertThat(actualSource).isEqualTo(context.source().struct());
    }

    @Test
    public void shouldGeneratorRecordForDeleteEvent() throws Exception {
        config = TestHelper.getConfiguration(mongo).edit()
                .with(MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST, "dbit.*")
                .with(CommonConnectorConfig.TOPIC_PREFIX, "mongo")
                .build();

        context = new MongoDbTaskContext(config);

        TestHelper.cleanDatabase(mongo, "dbit");

        start(MongoDbConnector.class, config);
        waitForStreamingRunning("mongodb", "mongo");

        // Insert record
        ObjectId objId = new ObjectId();
        Document obj = new Document("_id", objId);
        insertDocuments("dbit", "c1", obj);

        // Consume the insert
        consumeRecordsByTopic(1);
        assertNoRecordsToConsume();

        // Delete record from datbase
        final Instant timestamp = Instant.now();
        deleteDocument("dbit", "c1", objId);

        // Consume records, should be 2, delete and tombstone
        final SourceRecords records = consumeRecordsByTopic(2);
        assertThat(records.allRecordsInOrder().size()).isEqualTo(2);
        assertNoRecordsToConsume();

        final SourceRecord deleteRecord = records.allRecordsInOrder().get(0);
        final Struct key = (Struct) deleteRecord.key();
        final Struct value = (Struct) deleteRecord.value();

        assertThat(key.schema()).isSameAs(deleteRecord.keySchema());
        assertThat(key.get("id")).isEqualTo(formatObjectId(objId));

        assertThat(value.schema()).isSameAs(deleteRecord.valueSchema());
        assertThat(value.getString(Envelope.FieldName.AFTER)).isNull();
        assertThat(value.getString(Envelope.FieldName.OPERATION)).isEqualTo(Operation.DELETE.code());
        assertThat(value.getInt64(Envelope.FieldName.TIMESTAMP)).isGreaterThanOrEqualTo(timestamp.toEpochMilli());

        // final Struct actualSource = value.getStruct(Envelope.FieldName.SOURCE);
        // context.source().collectionEvent("rs0", new CollectionId("rs0", "dbit", "c1"));
        // assertThat(actualSource).isEqualTo(context.source().struct());

        final SourceRecord tombstoneRecord = records.allRecordsInOrder().get(1);
        final Struct tombstoneKey = (Struct) tombstoneRecord.key();
        assertThat(tombstoneKey.schema()).isSameAs(tombstoneRecord.keySchema());
        assertThat(tombstoneKey.get("id")).isEqualTo(formatObjectId(objId));
        assertThat(tombstoneRecord.value()).isNull();
        assertThat(tombstoneRecord.valueSchema()).isNull();
    }

    @Test
    @FixFor("DBZ-582")
    public void shouldGenerateRecordForDeleteEventWithoutTombstone() throws Exception {
        config = TestHelper.getConfiguration(mongo).edit()
                .with(MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST, "dbit.*")
                .with(CommonConnectorConfig.TOPIC_PREFIX, "mongo")
                .with(MongoDbConnectorConfig.TOMBSTONES_ON_DELETE, false)
                .build();

        context = new MongoDbTaskContext(config);

        TestHelper.cleanDatabase(mongo, "dbit");

        start(MongoDbConnector.class, config);
        waitForStreamingRunning("mongodb", "mongo");

        // Insert record
        ObjectId objId = new ObjectId();
        Document obj = new Document("_id", objId);
        insertDocuments("dbit", "c1", obj);

        // Consume the insert
        consumeRecordsByTopic(1);
        assertNoRecordsToConsume();

        // Delete record from datbase
        final Instant timestamp = Instant.now();
        deleteDocument("dbit", "c1", objId);

        // Consume records, should only ever 1
        final SourceRecords records = consumeRecordsByTopic(1);
        assertThat(records.allRecordsInOrder().size()).isEqualTo(1);
        assertNoRecordsToConsume();

        final SourceRecord record = records.allRecordsInOrder().get(0);
        final Struct key = (Struct) record.key();
        final Struct value = (Struct) record.value();

        assertThat(key.schema()).isSameAs(record.keySchema());
        assertThat(key.get("id")).isEqualTo(formatObjectId(objId));

        assertThat(value.schema()).isSameAs(record.valueSchema());
        assertThat(value.getString(Envelope.FieldName.AFTER)).isNull();
        assertThat(value.getString(Envelope.FieldName.OPERATION)).isEqualTo(Operation.DELETE.code());
        assertThat(value.getInt64(Envelope.FieldName.TIMESTAMP)).isGreaterThanOrEqualTo(timestamp.toEpochMilli());

        // final Struct actualSource = value.getStruct(Envelope.FieldName.SOURCE);
        // context.source().collectionEvent("rs0", new CollectionId("rs0", "dbit", "c1"));
        // assertThat(actualSource).isEqualTo(context.source().struct());
    }

    @Test
    public void shouldGenerateRecordsWithCorrectlySerializedId() throws Exception {
        config = TestHelper.getConfiguration(mongo).edit()
                .with(MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST, "dbit.*")
                .with(CommonConnectorConfig.TOPIC_PREFIX, "mongo")
                .build();

        context = new MongoDbTaskContext(config);

        TestHelper.cleanDatabase(mongo, "dbit");

        start(MongoDbConnector.class, config);
        waitForStreamingRunning("mongodb", "mongo");

        // long
        Document obj0 = new Document()
                .append("_id", Long.valueOf(Integer.MAX_VALUE) + 10)
                .append("name", "Sally");
        insertDocuments("dbit", "c1", obj0);

        // String
        Document obj1 = new Document()
                .append("_id", "123")
                .append("name", "Sally");
        insertDocuments("dbit", "c1", obj1);

        // Complex key type
        Document obj2 = new Document()
                .append("_id", new Document().append("company", 32).append("dept", "home improvement"))
                .append("name", "Sally");
        insertDocuments("dbit", "c1", obj2);

        // Date
        Calendar cal = Calendar.getInstance();
        cal.set(2017, 9, 19);
        Document obj3 = new Document()
                .append("_id", cal.getTime())
                .append("name", "Sally");
        insertDocuments("dbit", "c1", obj3);

        final boolean decimal128Supported = TestHelper.decimal128Supported();
        if (decimal128Supported) {
            // Decimal128
            Document obj4 = new Document()
                    .append("_id", new Decimal128(new BigDecimal("123.45678")))
                    .append("name", "Sally");
            insertDocuments("dbit", "c1", obj4);
        }

        final SourceRecords records = consumeRecordsByTopic(decimal128Supported ? 5 : 4);
        final List<SourceRecord> sourceRecords = records.allRecordsInOrder();

        assertSourceRecordKeyFieldIsEqualTo(sourceRecords.get(0), "id", "2147483657");
        assertSourceRecordKeyFieldIsEqualTo(sourceRecords.get(1), "id", "\"123\"");
        assertSourceRecordKeyFieldIsEqualTo(sourceRecords.get(2), "id", "{\"company\": 32,\"dept\": \"home improvement\"}");
        // that's actually not what https://docs.mongodb.com/manual/reference/mongodb-extended-json/#date suggests;
        // seems JsonSerializers is not fully compliant with that description
        assertSourceRecordKeyFieldIsEqualTo(sourceRecords.get(3), "id",
                "{\"$date\": \"" + ZonedDateTime.ofInstant(Instant.ofEpochMilli(cal.getTimeInMillis()), ZoneId.of("Z")).format(ISO_OFFSET_DATE_TIME) + "\"}");

        if (decimal128Supported) {
            assertSourceRecordKeyFieldIsEqualTo(sourceRecords.get(4), "id", "{\"$numberDecimal\": \"123.45678\"}");
        }
    }

    @Test
    public void shouldSkipNonPipelineRecords() throws Exception {
        config = TestHelper.getConfiguration(mongo).edit()
                .with(MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST, "dbit.*")
                .with(MongoDbConnectorConfig.CURSOR_PIPELINE, "[{$match:{'fullDocument.name':'Dennis'}}]")
                .with(CommonConnectorConfig.TOPIC_PREFIX, "mongo")
                .build();

        context = new MongoDbTaskContext(config);

        TestHelper.cleanDatabase(mongo, "dbit");

        start(MongoDbConnector.class, config);
        waitForStreamingRunning("mongodb", "mongo");

        var coll = "c1";
        insertDocuments("dbit", coll,
                new Document().append("_id", 1).append("name", "Albert"),
                new Document().append("_id", 2).append("name", "Bobby"),
                new Document().append("_id", 3).append("name", "Clyde"),
                new Document().append("_id", 4).append("name", "Dennis"));

        var records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic("mongo.dbit" + "." + coll))
                .hasSize(1)
                .element(0)
                .satisfies(record -> assertThat(Document.parse(((Struct) record.value()).getString(Envelope.FieldName.AFTER)))
                        .isEqualTo(Document.parse("{_id:4,name:'Dennis'}")));
    }

    private static void assertSourceRecordKeyFieldIsEqualTo(SourceRecord record, String fieldName, String expected) {
        final Struct struct = (Struct) record.key();
        assertThat(struct.get(fieldName)).isEqualTo(expected);
    }

    @Test
    public void shouldSupportDbRef2() throws Exception {
        config = TestHelper.getConfiguration(mongo).edit()
                .with(MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST, "dbit.*")
                .with(CommonConnectorConfig.TOPIC_PREFIX, "mongo")
                .build();

        context = new MongoDbTaskContext(config);

        TestHelper.cleanDatabase(mongo, "dbit");

        start(MongoDbConnector.class, config);
        waitForStreamingRunning("mongodb", "mongo");

        ObjectId objId = new ObjectId();
        Document obj = new Document()
                .append("_id", objId)
                .append("name", "Sally")
                .append("ref", new DBRef("othercollection", 15));

        final Instant timestamp = Instant.now();
        insertDocuments("dbit", "c1", obj);

        final SourceRecords records = consumeRecordsByTopic(1);
        assertThat(records.topics().size()).isEqualTo(1);
        assertThat(records.allRecordsInOrder().size()).isEqualTo(1);

        final SourceRecord record = records.allRecordsInOrder().get(0);
        final Struct key = (Struct) record.key();
        final Struct value = (Struct) record.value();
        assertThat(key.schema()).isSameAs(record.keySchema());
        assertThat(key.get("id")).isEqualTo(formatObjectId(objId));

        assertThat(value.schema()).isSameAs(record.valueSchema());

        // @formatter:off
        String expected = "{"
                +     "\"_id\": {\"$oid\": \"" + objId + "\"},"
                +     "\"name\": \"Sally\","
                +     "\"ref\": {\"$ref\": \"othercollection\",\"$id\": 15}"
                + "}";
        // @formatter:on

        assertThat(value.getString(Envelope.FieldName.AFTER)).isEqualTo(expected);
        assertThat(value.getString(Envelope.FieldName.OPERATION)).isEqualTo(Operation.CREATE.code());
        assertThat(value.getInt64(Envelope.FieldName.TIMESTAMP)).isGreaterThanOrEqualTo(timestamp.toEpochMilli());

        // Struct actualSource = value.getStruct(Envelope.FieldName.SOURCE);
        // context.source().collectionEvent("rs0", new CollectionId("rs0", "dbit", "c1"));
        // assertThat(actualSource).isEqualTo(context.source().struct());
    }

    @Test
    public void shouldReplicateContent() throws Exception {
        config = TestHelper.getConfiguration(mongo).edit()
                .with(MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST, "dbA.contacts")
                .with(CommonConnectorConfig.TOPIC_PREFIX, "mongo")
                .with(MongoDbConnectorConfig.SNAPSHOT_MODE, MongoDbConnectorConfig.SnapshotMode.INITIAL)
                .build();

        context = new MongoDbTaskContext(config);

        TestHelper.cleanDatabase(mongo, "dbA");

        try (var client = connect()) {
            // Create database and collection
            MongoDatabase db = client.getDatabase("dbA");
            MongoCollection<Document> contacts = db.getCollection("contacts");
            InsertOneOptions options = new InsertOneOptions().bypassDocumentValidation(true);
            contacts.insertOne(new Document().append("name", "Jon Snow"), options);
            assertThat(db.getCollection("contacts").countDocuments()).isEqualTo(1);

            // Read collection and find document
            Bson filter = com.mongodb.client.model.Filters.eq("name", "Jon Snow");
            FindIterable<Document> results = db.getCollection("contacts").find(filter);
            try (MongoCursor<Document> cursor = results.iterator()) {
                assertThat(cursor.tryNext().getString("name")).isEqualTo("Jon Snow");
                assertThat(cursor.tryNext()).isNull();
            }
        }

        // Start the connector
        start(MongoDbConnector.class, config);
        waitForStreamingRunning("mongodb", "mongo");

        final List<String> expectedNames = List.of("Jon Snow", "Sally Hamm");
        try (var client = connect()) {
            MongoDatabase db = client.getDatabase("dbA");
            MongoCollection<Document> contacts = db.getCollection("contacts");
            InsertOneOptions options = new InsertOneOptions().bypassDocumentValidation(true);
            contacts.insertOne(new Document().append("name", "Sally Hamm"), options);
            assertThat(db.getCollection("contacts").countDocuments()).isEqualTo(2);

            // Read collection results
            FindIterable<Document> results = db.getCollection("contacts").find();

            Set<String> foundNames = new HashSet<>();
            try (MongoCursor<Document> cursor = results.iterator()) {
                while (cursor.hasNext()) {
                    final String name = cursor.next().getString("name");
                    foundNames.add(name);
                }
            }

            assertThat(foundNames).containsOnlyElementsOf(expectedNames);
        }

        // Consume records
        List<SourceRecord> records = consumeRecordsByTopic(2).allRecordsInOrder();
        final Set<String> foundNames = new HashSet<>();
        records.forEach(record -> {
            VerifyRecord.isValid(record);

            final Struct value = (Struct) record.value();
            final String after = value.getString(Envelope.FieldName.AFTER);

            final Document document = Document.parse(after);
            foundNames.add(document.getString("name"));

            final Operation operation = Operation.forCode(value.getString(Envelope.FieldName.OPERATION));
            assertThat(operation == Operation.READ || operation == Operation.CREATE).isTrue();
        });
        assertNoRecordsToConsume();
        assertThat(foundNames).containsOnlyElementsOf(expectedNames);

        // Stop connector
        stopConnector();

        // Restart connector
        start(MongoDbConnector.class, config);

        // Verify there are no records to be consumed
        waitForStreamingRunning("mongodb", "mongo");
        assertNoRecordsToConsume();

        // Remove Jon Snow
        AtomicReference<ObjectId> jonSnowId = new AtomicReference<>();
        try (var client = connect()) {
            MongoDatabase db = client.getDatabase("dbA");
            MongoCollection<Document> contacts = db.getCollection("contacts");

            Bson filter = com.mongodb.client.model.Filters.eq("name", "Jon Snow");
            FindIterable<Document> results = db.getCollection("contacts").find(filter);
            try (MongoCursor<Document> cursor = results.iterator()) {
                final Document document = cursor.tryNext();
                assertThat(document.getString("name")).isEqualTo("Jon Snow");
                assertThat(cursor.tryNext()).isNull();

                jonSnowId.set(document.getObjectId("_id"));
                assertThat(jonSnowId.get()).isNotNull();
            }

            contacts.deleteOne(filter);
        }

        // Consume records, delete and tombstone
        records = consumeRecordsByTopic(2).allRecordsInOrder();
        final Set<ObjectId> foundIds = new HashSet<>();
        records.forEach(record -> {
            VerifyRecord.isValid(record);

            final Struct key = (Struct) record.key();
            final ObjectId id = toObjectId(key.getString("id"));
            foundIds.add(id);
            if (record.value() != null) {
                final Struct value = (Struct) record.value();
                final Operation operation = Operation.forCode(value.getString(Envelope.FieldName.OPERATION));
                assertThat(operation).isEqualTo(Operation.DELETE);
            }
        });

        // Stop connector
        stopConnector();

        // Restart connector and clear offsets
        initializeConnectorTestFramework();
        start(MongoDbConnector.class, config);

        waitForSnapshotToBeCompleted("mongodb", "mongo");

        // Consume records, one record in snapshot
        records = consumeRecordsByTopic(1).allRecordsInOrder();
        foundNames.clear();

        records.forEach(record -> {
            VerifyRecord.isValid(record);

            final Struct value = (Struct) record.value();
            final String after = value.getString(Envelope.FieldName.AFTER);

            final Document document = Document.parse(after);
            foundNames.add(document.getString("name"));

            final Operation operation = Operation.forCode(value.getString(Envelope.FieldName.OPERATION));
            assertThat(operation).isEqualTo(Operation.READ);
        });

        final List<String> allExpectedNames = List.of("Sally Hamm");
        assertThat(foundNames).containsOnlyElementsOf(allExpectedNames);

        waitForStreamingRunning("mongodb", "mongo");
        assertNoRecordsToConsume();
    }

    @Test
    public void shouldNotReplicateSnapshot() throws Exception {
        // todo: this configuration causes NPE at MongoDbStreamingChangeEventSource.java:143
        config = TestHelper.getConfiguration(mongo).edit()
                .with(MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST, "dbA.contacts")
                .with(CommonConnectorConfig.TOPIC_PREFIX, "mongo")
                .with(MongoDbConnectorConfig.SNAPSHOT_MODE, MongoDbConnectorConfig.SnapshotMode.NO_DATA)
                .build();

        context = new MongoDbTaskContext(config);

        TestHelper.cleanDatabase(mongo, "dbA");

        try (var client = connect()) {
            // Create database and collection
            MongoDatabase db = client.getDatabase("dbA");
            MongoCollection<Document> contacts = db.getCollection("contacts");
            InsertOneOptions options = new InsertOneOptions().bypassDocumentValidation(true);
            contacts.insertOne(new Document().append("name", "Jon Snow"), options);
            assertThat(db.getCollection("contacts").countDocuments()).isEqualTo(1);
        }

        // Start the connector
        start(MongoDbConnector.class, config);
        waitForStreamingRunning("mongodb", "mongo");

        try (var client = connect()) {
            MongoDatabase db = client.getDatabase("dbA");
            MongoCollection<Document> contacts = db.getCollection("contacts");
            InsertOneOptions options = new InsertOneOptions().bypassDocumentValidation(true);
            contacts.insertOne(new Document().append("name", "Ygritte"), options);
            assertThat(db.getCollection("contacts").countDocuments()).isEqualTo(2);
        }

        // Consume records
        List<SourceRecord> records = consumeRecordsByTopic(1).allRecordsInOrder();
        final Set<String> foundNames = new HashSet<>();
        records.forEach(record -> {
            VerifyRecord.isValid(record);

            final Struct value = (Struct) record.value();
            final String after = value.getString(Envelope.FieldName.AFTER);

            final Document document = Document.parse(after);
            foundNames.add(document.getString("name"));

            final Operation operation = Operation.forCode(value.getString(Envelope.FieldName.OPERATION));
            assertThat(operation).isEqualTo(Operation.CREATE);
        });
        assertNoRecordsToConsume();
        assertThat(foundNames).containsOnly("Ygritte");
    }

    @Test
    @FixFor("DBZ-1880")
    public void shouldGenerateRecordForUpdateEventUsingLegacyV1SourceInfo() throws Exception {
        config = TestHelper.getConfiguration(mongo).edit()
                .with(MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST, "dbit.*")
                .with(CommonConnectorConfig.TOPIC_PREFIX, "mongo")
                .build();

        context = new MongoDbTaskContext(config);

        TestHelper.cleanDatabase(mongo, "dbit");

        start(MongoDbConnector.class, config);
        waitForStreamingRunning("mongodb", "mongo");

        // Insert record
        ObjectId objId = new ObjectId();
        Document obj = new Document("_id", objId).append("name", "John");
        insertDocuments("dbit", "c1", obj);

        // Consume the insert
        consumeRecordsByTopic(1);
        assertNoRecordsToConsume();

        Document updateObj = new Document()
                .append("$set", new Document()
                        .append("name", "Sally"));

        final Instant timestamp = Instant.now();
        final Document filter = Document.parse("{\"_id\": {\"$oid\": \"" + objId + "\"}}");
        updateDocument("dbit", "c1", filter, updateObj);

        // Consume records, should be 1, the update
        final SourceRecords records = consumeRecordsByTopic(1);
        assertThat(records.allRecordsInOrder().size()).isEqualTo(1);
        assertNoRecordsToConsume();

        final SourceRecord deleteRecord = records.allRecordsInOrder().get(0);
        final Struct key = (Struct) deleteRecord.key();
        final Struct value = (Struct) deleteRecord.value();

        assertThat(key.schema()).isSameAs(deleteRecord.keySchema());
        assertThat(key.get("id")).isEqualTo(formatObjectId(objId));

        TestHelper.assertChangeStreamUpdate(
                objId,
                value,
                "{\"_id\": {\"$oid\": \"<OID>\"},\"name\": \"Sally\"}",
                null,
                "{\"name\": \"Sally\"}");

        assertThat(value.schema()).isSameAs(deleteRecord.valueSchema());
        assertThat(value.getString(Envelope.FieldName.OPERATION)).isEqualTo(Operation.UPDATE.code());
        assertThat(value.getInt64(Envelope.FieldName.TIMESTAMP)).isGreaterThanOrEqualTo(timestamp.toEpochMilli());
    }

    @FixFor("DBZ-6522")
    @Test
    public void shouldConsumeDocumentsWithComplexIds() throws Exception {
        config = TestHelper.getConfiguration(mongo).edit()
                .with(MongoDbConnectorConfig.POLL_INTERVAL_MS, 10)
                .with(CommonConnectorConfig.TOPIC_PREFIX, "mongo")
                .with(MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST, "dbit.*")
                .build();

        context = new MongoDbTaskContext(config);
        TestHelper.cleanDatabase(mongo, "dbit");

        start(MongoDbConnector.class, config);
        waitForStreamingRunning("mongodb", "mongo");

        Document doc = new Document("_id", 4367438483L).append("name", "John Doe").append("age", 25);
        insertDocuments("dbit", "colA", doc);

        SourceRecords records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic("mongo.dbit.colA")).hasSize(1);

        stopConnector();

        start(MongoDbConnector.class, config);
        waitForStreamingRunning("mongodb", "mongo");

        Document doc1 = new Document("_id", 1).append("name", "Jane Doe").append("age", 22);
        insertDocuments("dbit", "colA", doc1);

        records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic("mongo.dbit.colA")).hasSize(1);

        stopConnector();
    }

    @FixFor("DBZ-6522")
    @Test
    public void shouldConsumeEventsFromOffsetWithDataResumeToken() throws InterruptedException {
        LogInterceptor logInterceptor = new LogInterceptor(MongoDbOffsetContext.class);

        config = TestHelper.getConfiguration(mongo).edit()
                .with(MongoDbConnectorConfig.POLL_INTERVAL_MS, 10)
                .with(MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST, "dbit.*")
                .with(CommonConnectorConfig.TOPIC_PREFIX, "mongo")
                .build();

        MongoDbConnectorConfig connectorConfig = new MongoDbConnectorConfig(config);

        // Create a change stream pipeline
        var pipelineFactory = new ChangeStreamPipelineFactory(connectorConfig, new Filters.FilterConfig(config));
        ChangeStreamPipeline pipeline = pipelineFactory.create();

        var stages = pipeline.getStages();
        String resumeToken;

        // Insert a document using router and obtain resume token
        try (var router = connect()) {
            var routerStream = router.watch(stages, BsonDocument.class);
            try (var rc = routerStream.cursor()) {
                insertDocuments("dbit", "colA", new Document("_id", 1).append("name", "John"));
                rc.next();
                resumeToken = Objects.requireNonNull(rc.getResumeToken()).get("_data").asString().getValue();
            }
        }

        Map<Map<String, ?>, Map<String, ?>> offset = Map.of(
                Collect.hashMapOf("server_id", "mongo"),
                Collect.hashMapOf(
                        SourceInfo.TIMESTAMP, 0,
                        SourceInfo.ORDER, -1,
                        SourceInfo.RESUME_TOKEN,
                        resumeToken));
        storeOffsets(config, offset);

        // Set up the replication context for connections ...
        context = new MongoDbTaskContext(config);

        // Cleanup database
        TestHelper.cleanDatabase(mongo, "dbit");

        // Before starting the connector, add data to the databases ...
        insertDocuments("dbit", "colA", new Document("_id", 24734982398L).append("name", "Jane"));

        // Start the connector ...
        start(MongoDbConnector.class, config);
        waitForStreamingRunning("mongodb", "mongo");

        // Consume the records ...
        SourceRecords records = consumeRecordsByTopic(1);
        assertThat(logInterceptor.containsMessage("Old resume token format detected, attempting to parse as string " + resumeToken)).isTrue();
        assertThat(records.recordsForTopic("mongo.dbit.colA").size()).isEqualTo(1);

        assertNoRecordsToConsume();
    }

    @Test
    public void shouldAlwaysSnapshot() throws Exception {

        config = TestHelper.getConfiguration(mongo).edit()
                .with(MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST, "dbA.contacts")
                .with(CommonConnectorConfig.TOPIC_PREFIX, "mongo")
                .with(MongoDbConnectorConfig.SNAPSHOT_MODE, MongoDbConnectorConfig.SnapshotMode.ALWAYS)
                .build();

        context = new MongoDbTaskContext(config);

        TestHelper.cleanDatabase(mongo, "dbA");

        try (var client = connect()) {
            // Create database and collection
            MongoDatabase db = client.getDatabase("dbA");
            MongoCollection<Document> contacts = db.getCollection("contacts");
            InsertOneOptions options = new InsertOneOptions().bypassDocumentValidation(true);
            contacts.insertOne(new Document().append("name", "Jon Snow"), options);
            contacts.insertOne(new Document().append("name", "Ygritte"), options);
            assertThat(db.getCollection("contacts").countDocuments()).isEqualTo(2);
        }

        // Start the connector
        start(MongoDbConnector.class, config);
        waitForSnapshotToBeCompleted("mongodb", "mongo");

        // Consume records
        List<SourceRecord> records = consumeRecordsByTopic(2).allRecordsInOrder();
        final Set<String> foundNames = new HashSet<>();
        records.forEach(record -> {
            VerifyRecord.isValid(record);

            final Struct value = (Struct) record.value();
            final String after = value.getString(Envelope.FieldName.AFTER);

            final Document document = Document.parse(after);
            foundNames.add(document.getString("name"));

            final Operation operation = Operation.forCode(value.getString(Envelope.FieldName.OPERATION));
            assertThat(operation).isEqualTo(Operation.READ);
        });
        assertNoRecordsToConsume();
        assertThat(foundNames).containsOnly("Jon Snow", "Ygritte");

        stopConnector();

        try (var client = connect()) {
            // Create database and collection
            MongoDatabase db = client.getDatabase("dbA");
            MongoCollection<Document> contacts = db.getCollection("contacts");
            contacts.deleteOne(new Document().append("name", "Jon Snow"));
            contacts.insertOne(new Document().append("name", "Arya Stark"));
            assertThat(db.getCollection("contacts").countDocuments()).isEqualTo(2);
        }

        // Start the connector
        start(MongoDbConnector.class, config);
        waitForSnapshotToBeCompleted("mongodb", "mongo");

        // Consume records
        records = consumeRecordsByTopic(2).allRecordsInOrder();
        final Set<String> founds = new HashSet<>();
        records.forEach(record -> {
            VerifyRecord.isValid(record);

            final Struct value = (Struct) record.value();
            final String after = value.getString(Envelope.FieldName.AFTER);

            final Document document = Document.parse(after);
            founds.add(document.getString("name"));

            final Operation operation = Operation.forCode(value.getString(Envelope.FieldName.OPERATION));
            assertThat(operation).isEqualTo(Operation.READ);
        });
        assertNoRecordsToConsume();
        assertThat(founds).containsOnly("Ygritte", "Arya Stark");
    }

    @Test
    public void shouldAllowForCustomSnapshot() throws Exception {

        final LogInterceptor logInterceptor = new LogInterceptor(CustomTestSnapshot.class);

        config = TestHelper.getConfiguration(mongo).edit()
                .with(MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST, "dbA.contacts")
                .with(CommonConnectorConfig.TOPIC_PREFIX, "mongo")
                .with(MongoDbConnectorConfig.SNAPSHOT_MODE, MongoDbConnectorConfig.SnapshotMode.CUSTOM)
                .with(CommonConnectorConfig.SNAPSHOT_MODE_CUSTOM_NAME, CustomTestSnapshot.class)
                .build();

        context = new MongoDbTaskContext(config);

        TestHelper.cleanDatabase(mongo, "dbA");

        try (var client = connect()) {
            // Create database and collection
            MongoDatabase db = client.getDatabase("dbA");
            MongoCollection<Document> contacts = db.getCollection("contacts");
            InsertOneOptions options = new InsertOneOptions().bypassDocumentValidation(true);
            contacts.insertOne(new Document().append("name", "Jon Snow"), options);
            contacts.insertOne(new Document().append("name", "Ygritte"), options);
            assertThat(db.getCollection("contacts").countDocuments()).isEqualTo(2);
        }

        // Start the connector
        start(MongoDbConnector.class, config);
        waitForSnapshotToBeCompleted("mongodb", "mongo");

        // Consume records
        List<SourceRecord> records = consumeRecordsByTopic(2).allRecordsInOrder();
        final Set<String> foundNames = new HashSet<>();
        records.forEach(record -> {
            VerifyRecord.isValid(record);

            final Struct value = (Struct) record.value();
            final String after = value.getString(Envelope.FieldName.AFTER);

            final Document document = Document.parse(after);
            foundNames.add(document.getString("name"));

            final Operation operation = Operation.forCode(value.getString(Envelope.FieldName.OPERATION));
            assertThat(operation).isEqualTo(Operation.READ);
        });
        assertNoRecordsToConsume();
        assertThat(foundNames).containsOnly("Jon Snow", "Ygritte");

        assertThat(logInterceptor.containsMessage("Should snapshot data true")).isTrue();
        assertThat(logInterceptor.containsMessage("Should stream false")).isTrue();
    }

    private String formatObjectId(ObjectId objId) {
        return "{\"$oid\": \"" + objId + "\"}";
    }

    private void deleteDocument(String dbName, String collectionName, ObjectId objectId) {
        try (var client = connect()) {
            MongoDatabase db = client.getDatabase(dbName);
            MongoCollection<Document> coll = db.getCollection(collectionName);
            Document filter = Document.parse("{\"_id\": {\"$oid\": \"" + objectId + "\"}}");
            coll.deleteOne(filter);
        }
    }

    private ObjectId toObjectId(String oid) {
        return new ObjectId(oid.substring(10, oid.length() - 2));
    }

    @Test
    @FixFor("DBZ-9427")
    public void shouldValidateGuardrailLimitsExceedsMaximumCollections() throws Exception {
        // This captures all logged messages, allowing us to verify log message was written.
        final LogInterceptor logInterceptor = new LogInterceptor(CommonConnectorConfig.class);

        // Clean the database first
        TestHelper.cleanDatabase(mongo, "dbit");

        // Create 10 collections
        try (var client = connect()) {
            MongoDatabase db = client.getDatabase("dbit");
            for (int i = 1; i <= 10; i++) {
                String collectionName = String.format("collection%d", i);
                db.createCollection(collectionName);
                MongoCollection<Document> collection = db.getCollection(collectionName);
                collection.insertOne(new Document("_id", new ObjectId()).append("data", "test" + i));
            }
        }

        // Configure with guardrail limit of 5 collections (less than the 10 we created)
        config = TestHelper.getConfiguration(mongo)
                .edit()
                .with(MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST, "dbit.*")
                .with(CommonConnectorConfig.TOPIC_PREFIX, "mongo")
                .with(CommonConnectorConfig.GUARDRAIL_TABLES_MAX, 5)
                .build();

        // The connector should continue to run even after exceeding the guardrail limit
        logger.info("Attempting to start connector with guardrail limit exceeded, expect a warning");
        start(MongoDbConnector.class, config, (success, msg, error) -> {
            assertThat(success).isTrue();
            assertThat(error).isNull();
        });
        assertConnectorIsRunning();
        assertThat(logInterceptor.containsWarnMessage("Guardrail limit exceeded")).isTrue();
    }

    @Test
    @FixFor("DBZ-9427")
    public void shouldValidateGuardrailLimitsExceedsMaximumCollectionsAndFailConnector() throws Exception {
        // Clean the database first
        TestHelper.cleanDatabase(mongo, "dbit");

        // Create 10 collections
        try (var client = connect()) {
            MongoDatabase db = client.getDatabase("dbit");
            for (int i = 1; i <= 10; i++) {
                String collectionName = String.format("collection%d", i);
                db.createCollection(collectionName);
                MongoCollection<Document> collection = db.getCollection(collectionName);
                collection.insertOne(new Document("_id", new ObjectId()).append("data", "test" + i));
            }
        }

        // Configure with guardrail limit of 5 collections (less than the 10 we created)
        config = TestHelper.getConfiguration(mongo)
                .edit()
                .with(MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST, "dbit.*")
                .with(CommonConnectorConfig.TOPIC_PREFIX, "mongo")
                .with(CommonConnectorConfig.GUARDRAIL_TABLES_MAX, 5)
                .with(CommonConnectorConfig.GUARDRAIL_LIMIT_ACTION, "fail")
                .build();

        // The connector should fail to start due to exceeding the guardrail limit
        logger.info("Attempting to start connector with guardrail limit exceeded, expect an error");
        start(MongoDbConnector.class, config, (success, msg, error) -> {
            assertThat(success).isFalse();
            assertThat(error).isNotNull();
            assertThat(error.getMessage()).contains("Guardrail limit exceeded");
        });
        assertConnectorNotRunning();
    }

    @Test
    @FixFor("DBZ-9427")
    public void shouldStartSuccessfullyWithinGuardrailLimits() throws Exception {
        // Clean the database first
        TestHelper.cleanDatabase(mongo, "dbit");

        // Create 3 collections (well below the limit of 10)
        try (var client = connect()) {
            MongoDatabase db = client.getDatabase("dbit");
            for (int i = 1; i <= 3; i++) {
                String collectionName = String.format("coll%d", i);
                db.createCollection(collectionName);
                MongoCollection<Document> collection = db.getCollection(collectionName);
                ObjectId objId = new ObjectId();
                collection.insertOne(new Document("_id", objId).append("name", "test" + i));
            }
        }

        // Configure with guardrail limit of 10 collections
        config = TestHelper.getConfiguration(mongo)
                .edit()
                .with(MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST, "dbit.*")
                .with(MongoDbConnectorConfig.SNAPSHOT_MODE, MongoDbConnectorConfig.SnapshotMode.NO_DATA)
                .with(CommonConnectorConfig.TOPIC_PREFIX, "mongo")
                .with(CommonConnectorConfig.GUARDRAIL_TABLES_MAX, 10)
                .build();

        // The connector should start successfully
        context = new MongoDbTaskContext(config);
        start(MongoDbConnector.class, config);
        assertConnectorIsRunning();

        // Wait for streaming to be running
        waitForStreamingRunning("mongodb", "mongo");

        // Insert one more document to verify streaming works
        insertDocuments("dbit", "coll1", new Document("_id", new ObjectId()).append("name", "streaming_test"));

        // Consume the streaming record to ensure the connector is working properly
        SourceRecords streamingRecords = consumeRecordsByTopic(1);
        assertThat(streamingRecords).isNotNull();
        assertThat(streamingRecords.allRecordsInOrder().size()).isEqualTo(1);

        // Verify the record
        SourceRecord record = streamingRecords.allRecordsInOrder().get(0);
        Struct value = (Struct) record.value();
        assertThat(value.getString(Envelope.FieldName.OPERATION)).isEqualTo(Operation.CREATE.code());
        String after = value.getString(Envelope.FieldName.AFTER);
        Document doc = Document.parse(after);
        assertThat(doc.getString("name")).isEqualTo("streaming_test");

        assertNoRecordsToConsume();
        stopConnector();
    }
}
