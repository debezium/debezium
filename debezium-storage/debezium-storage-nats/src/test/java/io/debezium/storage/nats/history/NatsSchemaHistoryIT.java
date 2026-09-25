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

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import io.debezium.config.Configuration;
import io.debezium.connector.mysql.antlr.MySqlAntlrDdlParser;
import io.debezium.relational.Tables;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.relational.history.SchemaHistoryException;
import io.debezium.relational.history.SchemaHistoryListener;
import io.debezium.storage.nats.NatsCommonConfig;
import io.debezium.storage.nats.NatsConnection;
import io.debezium.util.Collect;
import io.nats.client.api.ConsumerInfo;
import io.nats.client.api.StreamInfo;
import io.nats.client.support.NatsJetStreamConstants;

/**
 * Tests for NATS-based schema history storage.
 *
 * @author Nick Chomey
 */
@Testcontainers
class NatsSchemaHistoryIT {

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
        return createHistory(new HashMap<>());
    }

    protected SchemaHistory createHistory(Map<String, String> extraConfig) {
        Map<String, String> config = new HashMap<>();
        config.put(SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + NatsCommonConfig.NATS_URL.name(), natsUrl);
        config.put(SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + NatsSchemaHistoryConfig.PROP_STREAM_NAME.name(),
                "test-schema-history");
        config.put(SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + NatsSchemaHistoryConfig.PROP_SUBJECT.name(),
                "test.schema.history");
        config.putAll(extraConfig);

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
    public void shouldBeIdempotentOnInitializeStorage() {
        // The stream already exists (created by createHistory() with the
        // default file storage). Re-initializing with a different stream
        // configuration (memory storage) must not hard-fail; the existing
        // stream should be reused.
        Map<String, String> config = new HashMap<>();
        config.put(SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + NatsCommonConfig.NATS_URL.name(), natsUrl);
        config.put(SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + NatsSchemaHistoryConfig.PROP_STREAM_NAME.name(),
                "test-schema-history");
        config.put(SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + NatsSchemaHistoryConfig.PROP_SUBJECT.name(),
                "test.schema.history");
        config.put(SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + NatsSchemaHistoryConfig.PROP_STORAGE_TYPE.name(),
                "memory");

        NatsSchemaHistory reconfigured = new NatsSchemaHistory();
        reconfigured.configure(Configuration.from(config), null, SchemaHistoryListener.NOOP, true);
        reconfigured.initializeStorage();
        reconfigured.stop();
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

    @Test
    @Timeout(30)
    @SuppressWarnings("deprecation")
    public void shouldRecoverAllRecordsBeyondAttemptLimit() throws Exception {
        // 300 records with a short recovery deadline must still recover
        // everything; recovery must drain the stream rather than truncate.
        Map<String, String> extraConfig = new HashMap<>();
        extraConfig.put(SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING
                + NatsSchemaHistoryConfig.PROP_RECOVERY_TIMEOUT_MS.name(), "5000");
        extraConfig.put(SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING
                + NatsSchemaHistoryConfig.PROP_RECOVERY_POLL_INTERVAL_MS.name(), "10");
        history = createHistory(extraConfig);

        Map<String, Object> source = server("test-server");
        for (int i = 0; i < 300; i++) {
            history.record(source, position("test.log", i, 0), "testdb", "CREATE TABLE t" + i + " (id INT);");
        }

        Tables tables = new Tables();
        history.recover(source, position("test.log", 299, 0), tables, new MySqlAntlrDdlParser());

        assertThat(tables.size()).isEqualTo(300);
    }

    @Test
    @Timeout(30)
    @SuppressWarnings("deprecation")
    public void shouldNotLeakDurableConsumersAfterRecovery() throws Exception {
        // Recovery must use an ephemeral consumer; durable consumers with
        // random names would accumulate in the stream on every recovery.
        Map<String, Object> source = server("test-server");
        for (int i = 0; i < 10; i++) {
            history.record(source, position("test.log", i, 0), "testdb", "CREATE TABLE t" + i + " (id INT);");
        }

        Tables tables = new Tables();
        history.recover(source, position("test.log", 9, 0), tables, new MySqlAntlrDdlParser());
        assertThat(tables.size()).isEqualTo(10);

        NatsCommonConfig connConfig = new NatsCommonConfig(Configuration.from(Collect.hashMapOf(
                NatsCommonConfig.NATS_URL.name(), natsUrl)), "");
        NatsConnection conn = new NatsConnection(connConfig);
        try {
            List<ConsumerInfo> consumers = conn.getJetStreamManagement()
                    .getConsumers("test-schema-history");
            assertThat(consumers)
                    .noneMatch(c -> c.getName().startsWith("schema-history-recovery-"));
        }
        finally {
            conn.close();
        }
    }

    @Test
    @Timeout(30)
    @SuppressWarnings("deprecation")
    public void shouldFailWhenStreamDeleted() throws Exception {
        // If the stream is deleted out from under the history (e.g. by an
        // operator or a retention policy), every previously recorded DDL
        // statement is gone. Recreating the stream would let the connector
        // continue against an incomplete history without saying so, so storing
        // a record must fail instead.
        Map<String, Object> source = server("test-server");
        history.record(source, position("test.log", 1, 0), "testdb", "CREATE TABLE t1 (id INT);");

        NatsCommonConfig connConfig = new NatsCommonConfig(Configuration.from(Collect.hashMapOf(
                NatsCommonConfig.NATS_URL.name(), natsUrl)), "");
        NatsConnection conn = new NatsConnection(connConfig);
        try {
            conn.getJetStreamManagement().deleteStream("test-schema-history");
        }
        finally {
            conn.close();
        }

        SchemaHistoryException failure = assertThrows(SchemaHistoryException.class,
                () -> history.record(source, position("test.log", 2, 0), "testdb", "CREATE TABLE t2 (id INT);"));
        assertThat(failure.getMessage()).contains("test-schema-history");
        assertThat(failure.getMessage()).contains("recovery");
    }

    @Test
    @Timeout(30)
    @SuppressWarnings("deprecation")
    public void shouldFailRecoveryWhenRecoveryTimeoutExpires() throws Exception {
        // Exhausting recovery.timeout.ms with messages still pending means the
        // recovered model is incomplete, so the connector has to fail rather
        // than carry on and describe the wrong table structure.
        Map<String, Object> source = server("test-server");
        history.record(source, position("test.log", 1, 0), "testdb", "CREATE TABLE t1 (id INT);");

        NatsSchemaHistory impatient = new NatsSchemaHistory();
        impatient.configure(Configuration.from(Collect.hashMapOf(
                SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + NatsCommonConfig.NATS_URL.name(), natsUrl,
                SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + NatsSchemaHistoryConfig.PROP_STREAM_NAME.name(),
                "test-schema-history",
                SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + NatsSchemaHistoryConfig.PROP_SUBJECT.name(),
                "test.schema.history",
                SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + NatsSchemaHistoryConfig.PROP_RECOVERY_TIMEOUT_MS.name(),
                "0")), null, SchemaHistoryListener.NOOP, true);
        impatient.start();
        try {
            Tables tables = new Tables();
            SchemaHistoryException failure = assertThrows(SchemaHistoryException.class,
                    () -> impatient.recover(source, position("test.log", 1, 0), tables, new MySqlAntlrDdlParser()));
            assertThat(failure.getMessage()).contains("couldn't be recovered");
            assertThat(failure.getMessage()).contains(
                    NatsSchemaHistoryConfig.PROP_RECOVERY_TIMEOUT_MS.name());
        }
        finally {
            impatient.stop();
        }
    }

    @Test
    @Timeout(30)
    @SuppressWarnings("deprecation")
    public void shouldWarnOnlyWhenStreamRetentionIsBounded() throws Exception {
        // A stream that discards its oldest records will lose the beginning of the
        // schema history, so a later recovery silently rebuilds a partial schema.
        // checkStorageSettings() warns about that, and must stay quiet otherwise.
        NatsSchemaHistory bounded = new NatsSchemaHistory();
        bounded.configure(Configuration.from(Collect.hashMapOf(
                SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + NatsCommonConfig.NATS_URL.name(), natsUrl,
                SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + NatsSchemaHistoryConfig.PROP_STREAM_NAME.name(),
                "bounded-schema-history",
                SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + NatsSchemaHistoryConfig.PROP_SUBJECT.name(),
                "bounded.schema.history",
                SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + NatsSchemaHistoryConfig.PROP_MAX_AGE_MS.name(),
                "3600000",
                SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + NatsSchemaHistoryConfig.PROP_MAX_BYTES.name(),
                "1024")), null, SchemaHistoryListener.NOOP, true);

        NatsCommonConfig connConfig = new NatsCommonConfig(Configuration.from(Collect.hashMapOf(
                NatsCommonConfig.NATS_URL.name(), natsUrl)), "");
        NatsConnection conn = new NatsConnection(connConfig);
        try {
            bounded.initializeStorage();

            StreamInfo boundedInfo = conn.getJetStreamManagement().getStreamInfo("bounded-schema-history");
            String warning = NatsSchemaHistory.retentionWarning(boundedInfo.getConfiguration()).orElse(null);
            assertThat(warning).isNotNull();
            assertThat(warning).contains(NatsSchemaHistoryConfig.PROP_MAX_AGE_MS.name());
            assertThat(warning).contains(NatsSchemaHistoryConfig.PROP_MAX_BYTES.name());

            // The stream created by createHistory() keeps everything, so the check
            // has to report nothing about it.
            StreamInfo unlimitedInfo = conn.getJetStreamManagement().getStreamInfo("test-schema-history");
            assertThat(NatsSchemaHistory.retentionWarning(unlimitedInfo.getConfiguration())).isEmpty();

            // The check is advisory, so it must never stop the connector.
            bounded.checkStorageSettings();
        }
        finally {
            conn.close();
            bounded.stop();
        }
    }

    @Test
    @Timeout(30)
    @SuppressWarnings("deprecation")
    public void shouldWarnOnlyWhenDeduplicationCannotCoverARetry() throws Exception {
        // A retried publish is only deduplicated if the stream's duplicate window
        // covers the retry, so a disabled or too short window has to be reported.
        NatsCommonConfig connConfig = new NatsCommonConfig(Configuration.from(Collect.hashMapOf(
                NatsCommonConfig.NATS_URL.name(), natsUrl)), "");
        NatsConnection conn = new NatsConnection(connConfig);
        try {
            // The stream created by createHistory() carries the configured window, which
            // is the server default and therefore long enough, so nothing is reported.
            StreamInfo defaultInfo = conn.getJetStreamManagement().getStreamInfo("test-schema-history");
            assertThat(defaultInfo.getConfiguration().getDuplicateWindow())
                    .isEqualTo(Duration.ofMillis(NatsJetStreamConstants.SERVER_DEFAULT_DUPLICATE_WINDOW_MS));
            assertThat(NatsSchemaHistory.deduplicationWarning(defaultInfo.getConfiguration(), Duration.ofSeconds(2)))
                    .isEmpty();

            // Zero does not disable deduplication: the server substitutes its own
            // default, which is why zero is documented as "leave the default in place".
            NatsSchemaHistory zeroWindow = historyWithDuplicateWindow("zero-window-schema-history",
                    "zero.window.schema.history", "0");
            try {
                StreamInfo info = conn.getJetStreamManagement().getStreamInfo("zero-window-schema-history");
                assertThat(info.getConfiguration().getDuplicateWindow())
                        .isEqualTo(Duration.ofMillis(NatsJetStreamConstants.SERVER_DEFAULT_DUPLICATE_WINDOW_MS));
                assertThat(NatsSchemaHistory.deduplicationWarning(info.getConfiguration(), Duration.ofSeconds(2)))
                        .isEmpty();
            }
            finally {
                zeroWindow.stop();
            }

            // A window shorter than the time a publish can spend being retried.
            NatsSchemaHistory shortWindow = historyWithDuplicateWindow("short-window-schema-history",
                    "short.window.schema.history", "1000");
            try {
                StreamInfo info = conn.getJetStreamManagement().getStreamInfo("short-window-schema-history");
                assertThat(info.getConfiguration().getDuplicateWindow()).isEqualTo(Duration.ofSeconds(1));
                assertThat(NatsSchemaHistory.deduplicationWarning(info.getConfiguration(), Duration.ofSeconds(2)))
                        .isPresent();
            }
            finally {
                shortWindow.stop();
            }
        }
        finally {
            conn.close();
        }
    }

    @SuppressWarnings("deprecation")
    private NatsSchemaHistory historyWithDuplicateWindow(String streamName, String subject, String duplicateWindowMs) {
        NatsSchemaHistory configured = new NatsSchemaHistory();
        configured.configure(Configuration.from(Collect.hashMapOf(
                SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + NatsCommonConfig.NATS_URL.name(), natsUrl,
                SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + NatsSchemaHistoryConfig.PROP_STREAM_NAME.name(),
                streamName,
                SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + NatsSchemaHistoryConfig.PROP_SUBJECT.name(),
                subject,
                SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING
                        + NatsSchemaHistoryConfig.PROP_DUPLICATE_WINDOW_MS.name(),
                duplicateWindowMs)), null, SchemaHistoryListener.NOOP, true);
        configured.initializeStorage();
        return configured;
    }
}
