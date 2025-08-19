/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.nats.history;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import io.debezium.config.Configuration;
import io.debezium.relational.Tables;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.relational.history.SchemaHistoryException;
import io.debezium.relational.history.SchemaHistoryListener;
import io.debezium.storage.nats.NatsCommonConfig;
import io.debezium.util.Collect;

/**
 * Tests for NATS-based schema history storage.
 *
 * @author Nick Chomey
 */
@Testcontainers
class NatsSchemaHistoryTest {

    private static final String NATS_CONTAINER_IMAGE = "nats:2.12.0-alpine";
    private static final int NATS_PORT = 4222;

    @Container
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
    private SchemaHistory history;

    @BeforeEach
    public void setUp() {
        natsUrl = "nats://localhost:" + natsContainer.getMappedPort(NATS_PORT);
        history = createHistory();
    }

    @AfterEach
    public void tearDown() {
        if (history != null) {
            history.stop();
        }
    }

    protected SchemaHistory createHistory() {
        Map<String, String> config = Collect.hashMapOf(
                SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + NatsCommonConfig.NATS_URL.name(), natsUrl,
                SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + NatsSchemaHistoryConfig.PROP_STREAM_NAME.name(),
                "test-schema-history",
                SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + NatsSchemaHistoryConfig.PROP_SUBJECT.name(),
                "test.schema.history");

        Configuration configuration = Configuration.from(config);
        NatsSchemaHistory history = new NatsSchemaHistory();
        history.configure(configuration, null, SchemaHistoryListener.NOOP, true);
        history.initializeStorage();
        history.start();
        return history;
    }

    protected Map<String, Object> server(String serverName) {
        return Collect.linkMapOf("server", serverName);
    }

    protected Map<String, Object> position(String filename, long position, int entry) {
        return Collect.linkMapOf("file", filename, "position", position, "entry", entry);
    }

    @Test
    public void shouldCreateAndInitializeStorage() {
        // Storage should be created and initialized
        assertTrue(history.storageExists());
        assertFalse(history.exists()); // No records yet
    }

    @Test
    public void shouldDetectExistenceAfterStoringRecord() throws InterruptedException {
        assertFalse(history.exists());

        // Store a record
        Map<String, Object> source = server("test-server");
        Map<String, Object> position = position("test.log", 1, 0);
        history.record(source, position, "testdb", "CREATE TABLE test (id INT);");

        // Now it should exist
        assertTrue(history.exists());
    }

    @Test
    public void shouldFailToStoreRecordBeforeStart() {
        NatsSchemaHistory newHistory = new NatsSchemaHistory();
        Map<String, String> config = Collect.hashMapOf(
                SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + NatsCommonConfig.NATS_URL.name(), natsUrl,
                SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + NatsSchemaHistoryConfig.PROP_STREAM_NAME.name(),
                "test-schema-history-2",
                SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + NatsSchemaHistoryConfig.PROP_SUBJECT.name(),
                "test.schema.history.2");

        Configuration configuration = Configuration.from(config);
        newHistory.configure(configuration, null, SchemaHistoryListener.NOOP, true);
        // Don't call start()

        assertThrows(SchemaHistoryException.class, () -> {
            Map<String, Object> source = server("test-server");
            Map<String, Object> position = position("test.log", 1, 0);
            newHistory.record(source, position, "testdb", "CREATE TABLE test (id INT);");
        });

        newHistory.stop();
    }

    @Test
    public void shouldHandleMultipleStreams() {
        // Create a second history with different stream
        Map<String, String> config2 = Collect.hashMapOf(
                SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + NatsCommonConfig.NATS_URL.name(), natsUrl,
                SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + NatsSchemaHistoryConfig.PROP_STREAM_NAME.name(),
                "test-schema-history-2",
                SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + NatsSchemaHistoryConfig.PROP_SUBJECT.name(),
                "test.schema.history.2",
                SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + NatsSchemaHistoryConfig.PROP_STORAGE_TYPE.name(),
                "memory",
                SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + NatsSchemaHistoryConfig.PROP_REPLICAS.name(), "1");

        Configuration configuration2 = Configuration.from(config2);
        NatsSchemaHistory history2 = new NatsSchemaHistory();
        history2.configure(configuration2, null, SchemaHistoryListener.NOOP, true);
        history2.initializeStorage();
        history2.start();

        try {
            // Both should be independent
            assertFalse(history.exists());
            assertFalse(history2.exists());

            // Store in first history
            Map<String, Object> source = server("test-server");
            Map<String, Object> position = position("test.log", 1, 0);
            history.record(source, position, "testdb", "CREATE TABLE test1 (id INT);");
            assertTrue(history.exists());
            assertFalse(history2.exists());

            // Store in second history
            history2.record(source, position, "testdb", "CREATE TABLE test2 (id INT);");
            assertTrue(history.exists());
            assertTrue(history2.exists());

        }
        finally {
            history2.stop();
        }
    }

    @Test
    @SuppressWarnings("deprecation")
    public void shouldRecoverFromEmptyStream() throws InterruptedException {
        // Recovery from empty stream should work
        Tables tables = new Tables();
        Map<String, Object> source = server("test-server");
        Map<String, Object> position = position("test.log", 0, 0);

        // This should not throw an exception
        history.recover(source, position, tables, null);
        assertThat(tables.size()).isEqualTo(0);
    }

    @Test
    @SuppressWarnings("deprecation")
    public void shouldHandleInterruptionDuringRecovery() {
        // Test interruption handling
        Thread.currentThread().interrupt();

        assertThrows(SchemaHistoryException.class, () -> {
            Tables tables = new Tables();
            Map<String, Object> source = server("test-server");
            Map<String, Object> position = position("test.log", 1, 0);
            history.recover(source, position, tables, null);
        });

        // Clear interrupt flag
        Thread.interrupted();
    }
}
