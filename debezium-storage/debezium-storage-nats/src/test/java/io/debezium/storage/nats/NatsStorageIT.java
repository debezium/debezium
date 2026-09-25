/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.nats;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import io.debezium.config.Configuration;
import io.debezium.connector.mysql.antlr.MySqlAntlrDdlParser;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.ddl.DdlParser;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.relational.history.SchemaHistoryListener;
import io.debezium.storage.nats.history.NatsSchemaHistory;
import io.debezium.storage.nats.offset.NatsOffsetBackingStore;
import io.debezium.storage.nats.offset.NatsOffsetBackingStoreConfig;
import io.debezium.util.Collect;

/**
 * Integration tests for NATS storage implementations.
 * Tests both offset storage and schema history working together.
 *
 * @author Nick Chomey
 */
@Testcontainers
class NatsStorageIT {

    private static final String NATS_CONTAINER_IMAGE = "nats:2.12.0-alpine";
    private static final int NATS_PORT = 4222;

    @SuppressWarnings("resource")
    public GenericContainer<?> natsContainer = new GenericContainer<>(DockerImageName.parse(NATS_CONTAINER_IMAGE))
            .withExposedPorts(NATS_PORT)
            .withCommand("-js")
            .withLogConsumer(frame -> {
                if (frame != null && frame.getUtf8String() != null) {
                    System.out.print(frame.getUtf8String());
                }
            });

    private String natsUrl;
    private NatsSchemaHistory schemaHistory;
    private NatsOffsetBackingStore offsetStore;
    private DdlParser parser;

    @BeforeEach
    public void setUp() {
        natsContainer.start();
        natsUrl = "nats://" + natsContainer.getHost() + ":" + natsContainer.getFirstMappedPort();

        parser = new MySqlAntlrDdlParser();
        setupSchemaHistory();
        setupOffsetStore();
    }

    @AfterEach
    public void tearDown() {
        if (schemaHistory != null) {
            schemaHistory.stop();
        }
        if (offsetStore != null) {
            offsetStore.stop();
        }
        if (natsContainer != null) {
            natsContainer.stop();
        }
    }

    private void setupSchemaHistory() {
        Map<String, String> config = Collect.hashMapOf(
                SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + NatsCommonConfig.NATS_URL.name(), natsUrl,
                SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING
                        + io.debezium.storage.nats.history.NatsSchemaHistoryConfig.PROP_STREAM_NAME.name(),
                "integration-schema-history",
                SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING
                        + io.debezium.storage.nats.history.NatsSchemaHistoryConfig.PROP_SUBJECT.name(),
                "integration.schema.history",
                SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING
                        + io.debezium.storage.nats.history.NatsSchemaHistoryConfig.PROP_STORAGE_TYPE.name(),
                "memory",
                SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING
                        + io.debezium.storage.nats.history.NatsSchemaHistoryConfig.PROP_REPLICAS.name(),
                "1");

        Configuration configuration = Configuration.from(config);
        schemaHistory = new NatsSchemaHistory();
        // Use the NOOP listener for testing
        schemaHistory.configure(configuration, null, SchemaHistoryListener.NOOP, true);
        schemaHistory.initializeStorage();
        schemaHistory.start();
    }

    private void setupOffsetStore() {
        Map<String, String> config = new HashMap<>();
        config.put("offset.storage." + NatsCommonConfig.NATS_URL.name(), natsUrl);
        config.put("offset.storage." + NatsOffsetBackingStoreConfig.PROP_BUCKET_NAME.name(), "integration-offsets");
        config.put("offset.storage." + NatsOffsetBackingStoreConfig.PROP_RETRY_ENABLED.name(), "true");
        config.put("offset.storage." + NatsOffsetBackingStoreConfig.PROP_MAX_RETRIES.name(), "3");
        config.put("offset.storage." + NatsOffsetBackingStoreConfig.PROP_RETRY_DELAY_MS.name(), "100");

        offsetStore = new NatsOffsetBackingStore();
        offsetStore.configure(Configuration.from(config));
        offsetStore.start();
    }

    @Test
    @Timeout(30)
    @SuppressWarnings("deprecation")
    public void shouldHandleConcurrentSchemaAndOffsetOperations() throws Exception {
        // Test concurrent operations on both schema history and offset storage

        // Store schema changes
        Map<String, Object> source = Collect.linkMapOf("server", "test-server");
        Map<String, Object> position1 = Collect.linkMapOf("file", "test.log", "position", 100L, "entry", 1);
        Map<String, Object> position2 = Collect.linkMapOf("file", "test.log", "position", 200L, "entry", 2);

        schemaHistory.record(source, position1, "testdb", "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(50));");
        schemaHistory.record(source, position2, "testdb", "CREATE TABLE orders (id INT PRIMARY KEY, user_id INT);");

        // Store offsets
        ByteBuffer offsetKey1 = ByteBuffer.wrap("partition-1".getBytes(StandardCharsets.UTF_8));
        ByteBuffer offsetValue1 = ByteBuffer.wrap("{\"position\": 100}".getBytes(StandardCharsets.UTF_8));
        ByteBuffer offsetKey2 = ByteBuffer.wrap("partition-2".getBytes(StandardCharsets.UTF_8));
        ByteBuffer offsetValue2 = ByteBuffer.wrap("{\"position\": 200}".getBytes(StandardCharsets.UTF_8));

        Map<ByteBuffer, ByteBuffer> offsets = new HashMap<>();
        offsets.put(offsetKey1, offsetValue1);
        offsets.put(offsetKey2, offsetValue2);

        Future<Void> offsetFuture = offsetStore.set(offsets, null);
        offsetFuture.get(10, TimeUnit.SECONDS);

        // Verify schema history
        assertTrue(schemaHistory.exists());
        assertTrue(schemaHistory.storageExists());

        Tables recoveredTables = new Tables();
        schemaHistory.recover(source, position2, recoveredTables, parser);

        assertThat(recoveredTables.size()).isEqualTo(2);
        assertTrue(recoveredTables.forTable(new TableId("testdb", null, "users")) != null);
        assertTrue(recoveredTables.forTable(new TableId("testdb", null, "orders")) != null);

        // Verify offsets
        Future<Map<ByteBuffer, ByteBuffer>> getFuture = offsetStore.get(Collect.arrayListOf(offsetKey1, offsetKey2));
        Map<ByteBuffer, ByteBuffer> retrievedOffsets = getFuture.get(10, TimeUnit.SECONDS);

        assertThat(retrievedOffsets).hasSize(2);
        assertEquals(offsetValue1, retrievedOffsets.get(offsetKey1));
        assertEquals(offsetValue2, retrievedOffsets.get(offsetKey2));
    }

    @Test
    @Timeout(30)
    @SuppressWarnings("deprecation")
    public void shouldHandleStorageRecreation() throws Exception {
        // Store initial data
        Map<String, Object> source = Collect.linkMapOf("server", "test-server");
        Map<String, Object> position = Collect.linkMapOf("file", "test.log", "position", 100L, "entry", 1);

        schemaHistory.record(source, position, "testdb", "CREATE TABLE test_table (id INT);");

        ByteBuffer offsetKey = ByteBuffer.wrap("test-key".getBytes(StandardCharsets.UTF_8));
        ByteBuffer offsetValue = ByteBuffer.wrap("test-value".getBytes(StandardCharsets.UTF_8));

        Map<ByteBuffer, ByteBuffer> offsets = new HashMap<>();
        offsets.put(offsetKey, offsetValue);

        Future<Void> setFuture = offsetStore.set(offsets, null);
        setFuture.get(10, TimeUnit.SECONDS);

        // Stop both storages
        schemaHistory.stop();
        offsetStore.stop();

        // Recreate and restart
        setupSchemaHistory();
        setupOffsetStore();

        // Verify data persisted
        assertTrue(schemaHistory.exists());

        Tables recoveredTables = new Tables();
        schemaHistory.recover(source, position, recoveredTables, parser);
        assertThat(recoveredTables.size()).isEqualTo(1);

        Future<Map<ByteBuffer, ByteBuffer>> getFuture = offsetStore.get(Collect.arrayListOf(offsetKey));
        Map<ByteBuffer, ByteBuffer> retrievedOffsets = getFuture.get(10, TimeUnit.SECONDS);
        assertEquals(offsetValue, retrievedOffsets.get(offsetKey));
    }

    @Test
    @Timeout(30)
    @SuppressWarnings("deprecation")
    public void shouldHandleEmptyStorages() throws Exception {
        // Test behavior with empty storages
        assertFalse(schemaHistory.exists());
        assertTrue(schemaHistory.storageExists());

        // Empty recovery should work
        Tables emptyTables = new Tables();
        Map<String, Object> source = Collect.linkMapOf("server", "test-server");
        Map<String, Object> position = Collect.linkMapOf("file", "test.log", "position", 0L, "entry", 0);

        schemaHistory.recover(source, position, emptyTables, parser);
        assertThat(emptyTables.size()).isEqualTo(0);

        // Empty offset retrieval should work
        Future<Map<ByteBuffer, ByteBuffer>> getFuture = offsetStore.get(new ArrayList<>());
        Map<ByteBuffer, ByteBuffer> retrievedOffsets = getFuture.get(10, TimeUnit.SECONDS);
        assertThat(retrievedOffsets).isEmpty();
    }

    @Test
    @Timeout(30)
    @SuppressWarnings("deprecation")
    public void shouldHandleLargeSchemaHistory() throws Exception {
        // Test with multiple schema changes
        Map<String, Object> source = Collect.linkMapOf("server", "test-server");

        for (int i = 0; i < 10; i++) {
            Map<String, Object> position = Collect.linkMapOf("file", "test.log", "position", (long) (i * 100), "entry",
                    i);
            String ddl = String.format("CREATE TABLE table_%d (id INT PRIMARY KEY, data VARCHAR(100));", i);
            schemaHistory.record(source, position, "testdb", ddl);
        }

        // Recover all changes
        Tables recoveredTables = new Tables();
        Map<String, Object> finalPosition = Collect.linkMapOf("file", "test.log", "position", 900L, "entry", 9);
        schemaHistory.recover(source, finalPosition, recoveredTables, parser);

        // Should have all 10 tables
        assertThat(recoveredTables.size()).isEqualTo(10);

        for (int i = 0; i < 10; i++) {
            assertTrue(recoveredTables.forTable(new TableId("testdb", null, "table_" + i)) != null);
        }
    }
}
