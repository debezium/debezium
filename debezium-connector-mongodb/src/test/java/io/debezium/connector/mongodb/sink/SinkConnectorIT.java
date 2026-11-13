/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.sink;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;

import io.debezium.connector.mongodb.MongoDbSinkConnector;
import io.debezium.connector.mongodb.TestHelper;
import io.debezium.connector.mongodb.sink.junit.MongoSinkTaskTestContext;
import io.debezium.connector.mongodb.sink.junit.SinkRecordFactory;
import io.debezium.sink.SinkConnectorConfig;
import io.debezium.sink.SinkConnectorConfig.BufferMode;
import io.debezium.testing.testcontainers.MongoDbDeployment;

public interface SinkConnectorIT {

    MongoDbDeployment getMongoDbDeployment();

    Logger LOGGER = LoggerFactory.getLogger(SinkConnectorIT.class);
    String DATABASE_NAME = "testdb";
    String TOPIC_PREFIX = "dbserver1";

    default Map<String, String> defaultSinkConfig() {
        var props = new HashMap<String, String>();
        props.put("connector.class", MongoDbSinkConnector.class.getName());
        props.put(MongoDbSinkConnectorConfig.CONNECTION_STRING.name(), getMongoDbDeployment().getConnectionString());
        props.put(MongoDbSinkConnectorConfig.SINK_DATABASE, DATABASE_NAME);
        props.put("openlineage.integration.enabled", "false");
        return props;
    }

    default SinkTask startSinkTask(Map<String, String> props) {
        try {
            SinkTask task = new MongoDbSinkConnectorTask();
            task.initialize(new MongoSinkTaskTestContext(props));
            task.start(props);
            return task;
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to start sink task", e);
        }
    }

    default void putAndFlush(SinkTask task, List<SinkRecord> records) {
        task.put(records);
        task.preCommit(Map.of());
    }

    default List<Document> findDocuments(String dbName, String collectionName) {
        try (var client = TestHelper.connect(getMongoDbDeployment())) {
            return client.getDatabase(dbName).getCollection(collectionName).find().into(new ArrayList<>());
        }
    }

    default List<String> listCollections(String dbName) {
        try (var client = TestHelper.connect(getMongoDbDeployment())) {
            MongoDatabase db = client.getDatabase(dbName);
            var collections = db.listCollectionNames();
            collections.forEach(c -> LOGGER.info("Found collection '{}' from database '{}'", c, dbName));
            return collections.into(new ArrayList<>());
        }
    }

    default void cleanDatabase(String dbName) {
        try (MongoClient client = TestHelper.connect(getMongoDbDeployment())) {
            client.getDatabase(dbName).drop();
        }
    }

    default void checkSinkConnectorWritesRecords(SinkRecordFactory factory) {
        var props = defaultSinkConfig();
        var task = startSinkTask(props);
        try {
            List<SinkRecord> records = List.of(
                    factory.createProduct(TOPIC_PREFIX, 101, "scooter", "Small 2-wheel scooter", 3.14).build(0),
                    factory.createProduct(TOPIC_PREFIX, 102, "car battery", "12V car battery", 8.1).build(1),
                    factory.createProduct(TOPIC_PREFIX, 103, "hammer", "12oz hammer", 0.75).build(2),
                    factory.createCustomer(TOPIC_PREFIX, 1001, "Sally", "Thomas", "sally.thomas@acme.com").build(0),
                    factory.createCustomer(TOPIC_PREFIX, 1002, "George", "Bailey", "gbailey@foobar.com").build(1));

            putAndFlush(task, records);

            List<String> collections = listCollections(DATABASE_NAME);
            LOGGER.info("Collections in {}: {}", DATABASE_NAME, collections);

            List<Document> products = findDocuments(DATABASE_NAME, "dbserver1_inventory_products");
            LOGGER.info("Products: {}", products);
            assertThat(products).hasSize(3);
            assertThat(products.get(0).getString("name")).isEqualTo("scooter");

            List<Document> customers = findDocuments(DATABASE_NAME, "dbserver1_inventory_customers");
            LOGGER.info("Customers: {}", customers);
            assertThat(customers).hasSize(2);
        }
        finally {
            task.stop();
        }
    }

    default void checkMultiTopicRoutesToSeparateCollections(SinkRecordFactory factory) {
        var props = defaultSinkConfig();
        var task = startSinkTask(props);
        try {
            putAndFlush(task, List.of(
                    factory.createProduct(TOPIC_PREFIX, 101, "scooter", "Small scooter", 3.14).build(0),
                    factory.createCustomer(TOPIC_PREFIX, 1001, "Sally", "Thomas", "sally@acme.com").build(0),
                    factory.createProduct(TOPIC_PREFIX, 102, "hammer", "12oz hammer", 0.75).build(1),
                    factory.createCustomer(TOPIC_PREFIX, 1002, "George", "Bailey", "george@acme.com").build(1)));

            List<String> collections = listCollections(DATABASE_NAME);
            assertThat(collections).containsExactlyInAnyOrder(
                    "dbserver1_inventory_products", "dbserver1_inventory_customers");

            List<Document> products = findDocuments(DATABASE_NAME, "dbserver1_inventory_products");
            assertThat(products).hasSize(2);
            assertThat(products).allMatch(doc -> doc.containsKey("weight"));
            assertThat(products).noneMatch(doc -> doc.containsKey("first_name"));

            List<Document> customers = findDocuments(DATABASE_NAME, "dbserver1_inventory_customers");
            assertThat(customers).hasSize(2);
            assertThat(customers).allMatch(doc -> doc.containsKey("first_name"));
            assertThat(customers).noneMatch(doc -> doc.containsKey("weight"));
        }
        finally {
            task.stop();
        }
    }

    default void checkFieldFiltering(SinkRecordFactory factory) {
        var props = defaultSinkConfig();
        props.put("field.exclude.list", "description");
        var task = startSinkTask(props);
        try {
            List<SinkRecord> records = List.of(
                    factory.createProduct(TOPIC_PREFIX, 101, "scooter", "Small 2-wheel scooter", 3.14).build(0),
                    factory.createProduct(TOPIC_PREFIX, 102, "car battery", "12V car battery", 8.1).build(1));

            putAndFlush(task, records);

            List<Document> products = findDocuments(DATABASE_NAME, "dbserver1_inventory_products");
            assertThat(products).hasSize(2);
            Document doc = products.get(0);
            assertThat(doc.getString("name")).isEqualTo("scooter");
            assertThat(doc.containsKey("weight")).isTrue();
            assertThat(doc.containsKey("description")).isFalse();
        }
        finally {
            task.stop();
        }
    }

    default void checkPrimaryKeyModeDeduplication(SinkRecordFactory factory) {
        var props = defaultSinkConfig();
        props.put("primary.key.mode", "record_key");
        var task = startSinkTask(props);
        try {
            List<SinkRecord> records = List.of(
                    factory.createProduct(TOPIC_PREFIX, 101, "scooter", "Small scooter", 3.14).build(0),
                    factory.updateProduct(TOPIC_PREFIX, 101, "scooter", "Updated scooter", 3.5).build(1),
                    factory.updateProduct(TOPIC_PREFIX, 101, "scooter", "Final scooter", 4.0).build(2));

            putAndFlush(task, records);

            List<Document> products = findDocuments(DATABASE_NAME, "dbserver1_inventory_products");
            assertThat(products).hasSize(1);
            assertThat(products.get(0).getDouble("weight")).isEqualTo(4.0);
        }
        finally {
            task.stop();
        }
    }

    default void checkDeleteRemovesDocument(SinkRecordFactory factory) {
        var props = defaultSinkConfig();
        props.put(MongoDbSinkConnectorConfig.DELETE_ENABLED_FIELD.name(), "true");
        props.put(MongoDbSinkConnectorConfig.PRIMARY_KEY_MODE_FIELD.name(), SinkConnectorConfig.PrimaryKeyMode.RECORD_KEY.getValue());
        var task = startSinkTask(props);
        try {
            putAndFlush(task, List.of(
                    factory.createProduct(TOPIC_PREFIX, 101, "scooter", "Small scooter", 3.14).build(0),
                    factory.createProduct(TOPIC_PREFIX, 102, "car battery", "12V battery", 8.1).build(1)));

            assertThat(findDocuments(DATABASE_NAME, "dbserver1_inventory_products")).hasSize(2);

            putAndFlush(task, List.of(
                    factory.deleteProduct(TOPIC_PREFIX, 101).build(2)));

            List<Document> remaining = findDocuments(DATABASE_NAME, "dbserver1_inventory_products");
            assertThat(remaining).hasSize(1);
            // FIXME
            Document id = remaining.get(0).get("_id", Document.class);
            assertThat(id.getInteger("id")).isEqualTo(102);
            assertThat(remaining.get(0).getString("name")).isEqualTo("car battery");
        }
        finally {
            task.stop();
        }
    }

    default void checkTombstoneIsSkipped(SinkRecordFactory factory) {
        var props = defaultSinkConfig();
        props.put("primary.key.mode", "record_key");
        var task = startSinkTask(props);
        try {
            putAndFlush(task, List.of(
                    factory.createProduct(TOPIC_PREFIX, 101, "scooter", "Small scooter", 3.14).build(0),
                    factory.tombstone(TOPIC_PREFIX, 101, 1)));

            List<Document> products = findDocuments(DATABASE_NAME, "dbserver1_inventory_products");
            assertThat(products).hasSize(1);
            assertThat(products.get(0).getString("name")).isEqualTo("scooter");
        }
        finally {
            task.stop();
        }
    }

    default void checkTruncateIsSkippedWhenDisabled(SinkRecordFactory factory) {
        var props = defaultSinkConfig();
        var task = startSinkTask(props);
        try {
            putAndFlush(task, List.of(
                    factory.createProduct(TOPIC_PREFIX, 101, "scooter", "Small scooter", 3.14).build(0),
                    factory.truncateProduct(TOPIC_PREFIX, 1)));

            List<Document> products = findDocuments(DATABASE_NAME, "dbserver1_inventory_products");
            assertThat(products).hasSize(1);
        }
        finally {
            task.stop();
        }
    }

    default void checkTruncateDeletesAllDocumentsWhenEnabled(SinkRecordFactory factory) {
        var props = defaultSinkConfig();
        props.put("truncate.enabled", "true");
        var task = startSinkTask(props);
        try {
            putAndFlush(task, List.of(
                    factory.createProduct(TOPIC_PREFIX, 101, "scooter", "Small scooter", 3.14).build(0),
                    factory.createProduct(TOPIC_PREFIX, 102, "hammer", "12oz hammer", 0.75).build(1)));

            assertThat(findDocuments(DATABASE_NAME, "dbserver1_inventory_products")).hasSize(2);

            putAndFlush(task, List.of(
                    factory.truncateProduct(TOPIC_PREFIX, 2)));

            List<Document> remaining = findDocuments(DATABASE_NAME, "dbserver1_inventory_products");
            assertThat(remaining).isEmpty();
        }
        finally {
            task.stop();
        }
    }

    default void checkKafkaPrimaryKeyModeCreatesOneDocumentPerRecord(SinkRecordFactory factory) {
        var props = defaultSinkConfig();
        props.put("primary.key.mode", "kafka");
        var task = startSinkTask(props);
        try {
            List<SinkRecord> records = List.of(
                    factory.createProduct(TOPIC_PREFIX, 101, "scooter", "Small scooter", 3.14).build(0),
                    factory.createProduct(TOPIC_PREFIX, 101, "scooter", "Small scooter", 3.14).build(1),
                    factory.createProduct(TOPIC_PREFIX, 101, "scooter", "Small scooter", 3.14).build(2));

            putAndFlush(task, records);

            List<Document> products = findDocuments(DATABASE_NAME, "dbserver1_inventory_products");
            assertThat(products).hasSize(3);
            Document id = products.get(0).get("_id", Document.class);
            assertThat(id.containsKey("__connect_topic")).isTrue();
            assertThat(id.containsKey("__connect_partition")).isTrue();
            assertThat(id.containsKey("__connect_offset")).isTrue();
        }
        finally {
            task.stop();
        }
    }

    default void checkFieldIncludeList(SinkRecordFactory factory) {
        var props = defaultSinkConfig();
        props.put("field.include.list", "name,weight");
        var task = startSinkTask(props);
        try {
            putAndFlush(task, List.of(
                    factory.createProduct(TOPIC_PREFIX, 101, "scooter", "Small scooter", 3.14).build(0)));

            List<Document> products = findDocuments(DATABASE_NAME, "dbserver1_inventory_products");
            assertThat(products).hasSize(1);
            Document doc = products.get(0);
            assertThat(doc.containsKey("name")).isTrue();
            assertThat(doc.containsKey("weight")).isTrue();
            assertThat(doc.containsKey("description")).isFalse();
            assertThat(doc.containsKey("id")).isFalse();
        }
        finally {
            task.stop();
        }
    }

    default void checkRecordValuePrimaryKeyMode(SinkRecordFactory factory) {
        var props = defaultSinkConfig();
        props.put("primary.key.mode", "record_value");
        props.put("primary.key.fields", "id");
        var task = startSinkTask(props);
        try {
            putAndFlush(task, List.of(
                    factory.createProduct(TOPIC_PREFIX, 101, "scooter", "Small scooter", 3.14).build(0)));

            List<Document> products = findDocuments(DATABASE_NAME, "dbserver1_inventory_products");
            assertThat(products).hasSize(1);
            Document id = products.get(0).get("_id", Document.class);
            assertThat(id.containsKey("id")).isTrue();
            assertThat(id.getInteger("id")).isEqualTo(101);
        }
        finally {
            task.stop();
        }
    }

    default void checkRecordHeaderPrimaryKeyMode(SinkRecordFactory factory) {
        var props = defaultSinkConfig();
        props.put("primary.key.mode", "record_header");
        var task = startSinkTask(props);
        try {
            SinkRecord record = factory.record(TOPIC_PREFIX + ".inventory.products")
                    .keyField("id", org.apache.kafka.connect.data.Schema.INT32_SCHEMA, 101)
                    .field("id", org.apache.kafka.connect.data.Schema.INT32_SCHEMA, 101)
                    .field("name", org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA, "scooter")
                    .header("tenant_id", org.apache.kafka.connect.data.Schema.STRING_SCHEMA, "acme")
                    .build(0);

            putAndFlush(task, List.of(record));

            List<Document> products = findDocuments(DATABASE_NAME, "dbserver1_inventory_products");
            assertThat(products).hasSize(1);
            Document id = products.get(0).get("_id", Document.class);
            assertThat(id.containsKey("tenant_id")).isTrue();
        }
        finally {
            task.stop();
        }
    }

    default void checkPrimaryKeyFieldsRestriction(SinkRecordFactory factory) {
        var props = defaultSinkConfig();
        props.put("primary.key.mode", "record_key");
        props.put("primary.key.fields", "id");
        var task = startSinkTask(props);
        try {
            SinkRecord record = factory.record(TOPIC_PREFIX + ".inventory.products")
                    .keyField("id", org.apache.kafka.connect.data.Schema.INT32_SCHEMA, 101)
                    .keyField("tenant", org.apache.kafka.connect.data.Schema.STRING_SCHEMA, "acme")
                    .field("id", org.apache.kafka.connect.data.Schema.INT32_SCHEMA, 101)
                    .field("name", org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA, "scooter")
                    .build(0);

            putAndFlush(task, List.of(record));

            List<Document> products = findDocuments(DATABASE_NAME, "dbserver1_inventory_products");
            assertThat(products).hasSize(1);
            Document id = products.get(0).get("_id", Document.class);
            assertThat(id.containsKey("id")).isTrue();
            assertThat(id.containsKey("tenant")).isFalse();
        }
        finally {
            task.stop();
        }
    }

    default void checkSameKeyUpsertProducesOneDocument(SinkRecordFactory factory) {
        var props = defaultSinkConfig();
        props.put("primary.key.mode", "record_key");
        var task = startSinkTask(props);
        try {
            putAndFlush(task, List.of(
                    factory.createProduct(TOPIC_PREFIX, 101, "scooter", "v1", 1.0).build(0),
                    factory.updateProduct(TOPIC_PREFIX, 101, "scooter", "v2", 2.0).build(1),
                    factory.updateProduct(TOPIC_PREFIX, 101, "scooter", "v3", 3.0).build(2)));

            List<Document> products = findDocuments(DATABASE_NAME, "dbserver1_inventory_products");
            assertThat(products).hasSize(1);
            assertThat(products.get(0).getString("description")).isEqualTo("v3");
        }
        finally {
            task.stop();
        }
    }

    // ─── Buffer mode: keyless passthrough ────────────────────────────────────────

    default void checkKeylessPassthroughPreservesAllRecords(SinkRecordFactory factory) {
        var props = defaultSinkConfig();
        var task = startSinkTask(props);
        try {
            putAndFlush(task, List.of(
                    factory.record(TOPIC_PREFIX + ".inventory.products")
                            .field("id", org.apache.kafka.connect.data.Schema.INT32_SCHEMA, 101)
                            .field("name", org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA, "scooter")
                            .build(0),
                    factory.record(TOPIC_PREFIX + ".inventory.products")
                            .field("id", org.apache.kafka.connect.data.Schema.INT32_SCHEMA, 102)
                            .field("name", org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA, "hammer")
                            .build(1),
                    factory.record(TOPIC_PREFIX + ".inventory.products")
                            .field("id", org.apache.kafka.connect.data.Schema.INT32_SCHEMA, 103)
                            .field("name", org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA, "wrench")
                            .build(2)));

            List<Document> products = findDocuments(DATABASE_NAME, "dbserver1_inventory_products");
            assertThat(products).hasSize(3);
        }
        finally {
            task.stop();
        }
    }

    default void checkKeylessPassthroughPreservesIdenticalRecords(SinkRecordFactory factory) {
        var props = defaultSinkConfig();
        var task = startSinkTask(props);
        try {
            putAndFlush(task, List.of(
                    factory.record(TOPIC_PREFIX + ".inventory.products")
                            .field("id", org.apache.kafka.connect.data.Schema.INT32_SCHEMA, 101)
                            .field("name", org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA, "scooter")
                            .build(0),
                    factory.record(TOPIC_PREFIX + ".inventory.products")
                            .field("id", org.apache.kafka.connect.data.Schema.INT32_SCHEMA, 101)
                            .field("name", org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA, "scooter")
                            .build(1)));

            List<Document> products = findDocuments(DATABASE_NAME, "dbserver1_inventory_products");
            assertThat(products).hasSize(2);
            assertThat(products).allMatch(doc -> "scooter".equals(doc.getString("name")));
        }
        finally {
            task.stop();
        }
    }

    // ─── Buffer mode: deduplicating (default) with keyed events ──────────────────

    default void checkDeduplicatingBufferCollapsesSameKeyUpdates(SinkRecordFactory factory) {
        var props = defaultSinkConfig();
        props.put("primary.key.mode", "record_key");
        var task = startSinkTask(props);
        try {
            putAndFlush(task, List.of(
                    factory.createProduct(TOPIC_PREFIX, 101, "scooter", "v1", 1.0).build(0),
                    factory.updateProduct(TOPIC_PREFIX, 101, "scooter", "v2", 2.0).build(1),
                    factory.createProduct(TOPIC_PREFIX, 102, "hammer", "heavy", 5.0).build(2)));

            List<Document> products = findDocuments(DATABASE_NAME, "dbserver1_inventory_products");
            assertThat(products).hasSize(2);

            Document scooter = products.stream()
                    .filter(d -> "scooter".equals(d.getString("name")))
                    .findFirst().orElseThrow();
            assertThat(scooter.getString("description")).isEqualTo("v2");
        }
        finally {
            task.stop();
        }
    }

    // ─── Buffer mode: passthrough with keyed events ──────────────────────────────

    default void checkPassthroughBufferAppliesAllKeyedEvents(SinkRecordFactory factory) {
        var props = defaultSinkConfig();
        props.put("primary.key.mode", "record_key");
        props.put(SinkConnectorConfig.BUFFER_MODE, BufferMode.PASSTHROUGH.getValue());
        var task = startSinkTask(props);
        try {
            putAndFlush(task, List.of(
                    factory.createProduct(TOPIC_PREFIX, 101, "scooter", "v1", 1.0).build(0),
                    factory.updateProduct(TOPIC_PREFIX, 101, "scooter", "v2", 2.0).build(1),
                    factory.updateProduct(TOPIC_PREFIX, 101, "scooter", "v3", 3.0).build(2)));

            List<Document> products = findDocuments(DATABASE_NAME, "dbserver1_inventory_products");
            assertThat(products).hasSize(1);
            assertThat(products.get(0).getString("description")).isEqualTo("v3");
            assertThat(products.get(0).getDouble("weight")).isEqualTo(3.0);
        }
        finally {
            task.stop();
        }
    }

    default void checkPassthroughBufferWithDistinctKeys(SinkRecordFactory factory) {
        var props = defaultSinkConfig();
        props.put("primary.key.mode", "record_key");
        props.put(SinkConnectorConfig.BUFFER_MODE, BufferMode.PASSTHROUGH.getValue());
        var task = startSinkTask(props);
        try {
            putAndFlush(task, List.of(
                    factory.createProduct(TOPIC_PREFIX, 101, "scooter", "Small scooter", 3.14).build(0),
                    factory.createProduct(TOPIC_PREFIX, 102, "hammer", "12oz hammer", 0.75).build(1),
                    factory.createProduct(TOPIC_PREFIX, 103, "wrench", "Adjustable wrench", 1.5).build(2)));

            List<Document> products = findDocuments(DATABASE_NAME, "dbserver1_inventory_products");
            assertThat(products).hasSize(3);
        }
        finally {
            task.stop();
        }
    }

    default void checkPassthroughBufferWithMixedKeys(SinkRecordFactory factory) {
        var props = defaultSinkConfig();
        props.put("primary.key.mode", "record_key");
        props.put(SinkConnectorConfig.BUFFER_MODE, BufferMode.PASSTHROUGH.getValue());
        var task = startSinkTask(props);
        try {
            putAndFlush(task, List.of(
                    factory.createProduct(TOPIC_PREFIX, 101, "scooter", "A1", 1.0).build(0),
                    factory.createProduct(TOPIC_PREFIX, 102, "hammer", "B1", 2.0).build(1),
                    factory.updateProduct(TOPIC_PREFIX, 101, "scooter", "A2", 1.5).build(2),
                    factory.updateProduct(TOPIC_PREFIX, 102, "hammer", "B2", 2.5).build(3),
                    factory.updateProduct(TOPIC_PREFIX, 101, "scooter", "A3", 1.9).build(4)));

            List<Document> products = findDocuments(DATABASE_NAME, "dbserver1_inventory_products");
            assertThat(products).hasSize(2);

            Document scooter = products.stream()
                    .filter(d -> "scooter".equals(d.getString("name")))
                    .findFirst().orElseThrow();
            assertThat(scooter.getString("description")).isEqualTo("A3");
            assertThat(scooter.getDouble("weight")).isEqualTo(1.9);

            Document hammer = products.stream()
                    .filter(d -> "hammer".equals(d.getString("name")))
                    .findFirst().orElseThrow();
            assertThat(hammer.getString("description")).isEqualTo("B2");
            assertThat(hammer.getDouble("weight")).isEqualTo(2.5);
        }
        finally {
            task.stop();
        }
    }
}
