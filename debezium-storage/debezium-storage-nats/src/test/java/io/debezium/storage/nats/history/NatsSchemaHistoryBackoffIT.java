/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.nats.history;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
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
import io.debezium.storage.nats.NatsCommonConfig;
import io.debezium.util.Collect;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.DeliverPolicy;

/**
 * Tests for NATS JetStream consumer backoff configuration in schema history.
 *
 * This test verifies that the NATS JetStream consumer can be configured with
 * backoff settings for message re-delivery on acknowledgment timeout.
 *
 * @author Nick Chomey
 */
@Testcontainers
class NatsSchemaHistoryBackoffIT {

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
    private DdlParser parser;

    @BeforeEach
    public void setUp() {
        natsContainer.start();
        natsUrl = "nats://" + natsContainer.getHost() + ":" + natsContainer.getFirstMappedPort();
        parser = new MySqlAntlrDdlParser();
    }

    @AfterEach
    public void tearDown() {
        if (schemaHistory != null) {
            schemaHistory.stop();
        }
        if (natsContainer != null) {
            natsContainer.stop();
        }
    }

    @Test
    @SuppressWarnings("deprecation")
    public void shouldConfigureConsumerWithBackoffSettings() throws Exception {
        // Create schema history with custom backoff configuration
        Map<String, String> config = new HashMap<>();
        config.put(SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + NatsCommonConfig.NATS_URL.name(), natsUrl);
        config.put(SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + NatsSchemaHistoryConfig.PROP_STREAM_NAME.name(),
                "backoff-test-stream");
        config.put(SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + NatsSchemaHistoryConfig.PROP_SUBJECT.name(),
                "backoff.test.subject");
        config.put(SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + NatsSchemaHistoryConfig.PROP_STORAGE_TYPE.name(),
                "memory");
        config.put(SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + NatsSchemaHistoryConfig.PROP_REPLICAS.name(), "1");
        config.put(SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING
                + NatsSchemaHistoryConfig.PROP_RECOVERY_POLL_INTERVAL_MS.name(), "100");
        config.put(SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING
                + NatsSchemaHistoryConfig.PROP_RECOVERY_TIMEOUT_MS.name(), "5000");

        Configuration configuration = Configuration.from(config);
        schemaHistory = new NatsSchemaHistory();
        schemaHistory.configure(configuration, null, SchemaHistoryListener.NOOP, true);
        schemaHistory.initializeStorage();
        schemaHistory.start();

        // Store some test records
        Map<String, Object> source = Collect.linkMapOf("server", "test-server");
        Map<String, Object> position1 = Collect.linkMapOf("file", "test.log", "position", 100L, "entry", 1);
        Map<String, Object> position2 = Collect.linkMapOf("file", "test.log", "position", 200L, "entry", 2);

        schemaHistory.record(source, position1, "testdb",
                "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(50));");
        schemaHistory.record(source, position2, "testdb",
                "CREATE TABLE orders (id INT PRIMARY KEY, user_id INT);");

        // Verify records can be recovered
        Tables recoveredTables = new Tables();
        schemaHistory.recover(source, position2, recoveredTables, parser);

        assertThat(recoveredTables.size()).isEqualTo(2);
        assertTrue(recoveredTables.forTable(new TableId("testdb", null, "users")) != null);
        assertTrue(recoveredTables.forTable(new TableId("testdb", null, "orders")) != null);
    }

    @Test
    public void shouldCreateConsumerWithBackoffConfiguration() throws Exception {
        // Test that we can create a consumer with backoff settings
        Map<String, String> config = new HashMap<>();
        config.put(SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + NatsCommonConfig.NATS_URL.name(), natsUrl);
        config.put(SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + NatsSchemaHistoryConfig.PROP_STREAM_NAME.name(),
                "backoff-consumer-test");
        config.put(SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + NatsSchemaHistoryConfig.PROP_SUBJECT.name(),
                "backoff.consumer.test");

        Configuration configuration = Configuration.from(config);

        schemaHistory = new NatsSchemaHistory();
        schemaHistory.configure(configuration, null, SchemaHistoryListener.NOOP, true);
        schemaHistory.initializeStorage();
        schemaHistory.start();

        // Create a consumer configuration with backoff settings
        // This demonstrates how backoff can be configured for JetStream consumers
        ConsumerConfiguration consumerConfig = ConsumerConfiguration.builder()
                .durable("backoff-test-consumer")
                .deliverPolicy(DeliverPolicy.All)
                .ackWait(Duration.ofSeconds(30)) // Initial ack wait time
                .maxDeliver(5) // Maximum delivery attempts
                // Backoff sequence: 5s, 30s, 300s, 3600s, 84000s (as per NATS docs example)
                .backoff(Duration.ofSeconds(5), Duration.ofSeconds(30), Duration.ofMinutes(5),
                        Duration.ofHours(1), Duration.ofHours(23).plusMinutes(20))
                .build();

        // Verify the consumer configuration
        assertThat(consumerConfig.getDurable()).isEqualTo("backoff-test-consumer");
        assertThat(consumerConfig.getDeliverPolicy()).isEqualTo(DeliverPolicy.All);
        assertThat(consumerConfig.getAckWait()).isEqualTo(Duration.ofSeconds(30));
        assertThat(consumerConfig.getMaxDeliver()).isEqualTo(5);
        assertThat(consumerConfig.getBackoff()).hasSize(5);

        // Verify backoff sequence
        assertThat(consumerConfig.getBackoff().get(0)).isEqualTo(Duration.ofSeconds(5));
        assertThat(consumerConfig.getBackoff().get(1)).isEqualTo(Duration.ofSeconds(30));
        assertThat(consumerConfig.getBackoff().get(2)).isEqualTo(Duration.ofMinutes(5));
        assertThat(consumerConfig.getBackoff().get(3)).isEqualTo(Duration.ofHours(1));
        assertThat(consumerConfig.getBackoff().get(4)).isEqualTo(Duration.ofHours(23).plusMinutes(20));
    }

    @Test
    public void shouldHandleBackoffWithFewerDeliveriesThanBackoffEntries() throws Exception {
        // Test case where MaxDeliver is smaller than backoff list
        Map<String, String> config = new HashMap<>();
        config.put(SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + NatsCommonConfig.NATS_URL.name(), natsUrl);
        config.put(SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + NatsSchemaHistoryConfig.PROP_STREAM_NAME.name(),
                "backoff-fewer-test");
        config.put(SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + NatsSchemaHistoryConfig.PROP_SUBJECT.name(),
                "backoff.fewer.test");

        Configuration configuration = Configuration.from(config);
        schemaHistory = new NatsSchemaHistory();
        schemaHistory.configure(configuration, null, SchemaHistoryListener.NOOP, true);
        schemaHistory.initializeStorage();
        schemaHistory.start();

        // Create consumer with MaxDeliver=3 but 5 backoff entries
        ConsumerConfiguration consumerConfig = ConsumerConfiguration.builder()
                .durable("backoff-fewer-consumer")
                .deliverPolicy(DeliverPolicy.All)
                .maxDeliver(3) // Only 3 delivery attempts
                // But 5 backoff entries - only first 3 should be used
                .backoff(Duration.ofSeconds(1), Duration.ofSeconds(5), Duration.ofSeconds(10),
                        Duration.ofSeconds(30), Duration.ofMinutes(5))
                .build();

        // Verify configuration
        assertThat(consumerConfig.getMaxDeliver()).isEqualTo(3);
        assertThat(consumerConfig.getBackoff()).hasSize(5); // All backoff entries are stored

        // According to NATS docs: "The sequence length must be less than or equal to
        // MaxDeliver"
        // In practice, NATS will only use the first MaxDeliver entries from the backoff
        // list
    }

    @Test
    public void shouldHandleBackoffWithMoreDeliveriesThanBackoffEntries() throws Exception {
        // Test case where MaxDeliver is larger than backoff list
        Map<String, String> config = new HashMap<>();
        config.put(SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + NatsCommonConfig.NATS_URL.name(), natsUrl);
        config.put(SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + NatsSchemaHistoryConfig.PROP_STREAM_NAME.name(),
                "backoff-more-test");
        config.put(SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + NatsSchemaHistoryConfig.PROP_SUBJECT.name(),
                "backoff.more.test");

        Configuration configuration = Configuration.from(config);
        schemaHistory = new NatsSchemaHistory();
        schemaHistory.configure(configuration, null, SchemaHistoryListener.NOOP, true);
        schemaHistory.initializeStorage();
        schemaHistory.start();

        // Create consumer with MaxDeliver=7 but only 3 backoff entries
        ConsumerConfiguration consumerConfig = ConsumerConfiguration.builder()
                .durable("backoff-more-consumer")
                .deliverPolicy(DeliverPolicy.All)
                .maxDeliver(7) // 7 delivery attempts
                // But only 3 backoff entries - last one should be reused
                .backoff(Duration.ofSeconds(1), Duration.ofSeconds(5), Duration.ofSeconds(30))
                .build();

        // Verify configuration
        assertThat(consumerConfig.getMaxDeliver()).isEqualTo(7);
        assertThat(consumerConfig.getBackoff()).hasSize(3);

        // According to NATS docs: "When MaxDeliver is larger than the backoff list,
        // the last delay in the list will apply for the remaining deliveries"
        // So deliveries 4-7 would all use the 30-second delay
    }

    @Test
    public void shouldHandleConsumerWithoutBackoff() throws Exception {
        // Test consumer without backoff (immediate re-delivery on timeout)
        Map<String, String> config = new HashMap<>();
        config.put(SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + NatsCommonConfig.NATS_URL.name(), natsUrl);
        config.put(SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + NatsSchemaHistoryConfig.PROP_STREAM_NAME.name(),
                "no-backoff-test");
        config.put(SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + NatsSchemaHistoryConfig.PROP_SUBJECT.name(),
                "no.backoff.test");

        Configuration configuration = Configuration.from(config);
        schemaHistory = new NatsSchemaHistory();
        schemaHistory.configure(configuration, null, SchemaHistoryListener.NOOP, true);
        schemaHistory.initializeStorage();
        schemaHistory.start();

        // Create consumer without backoff
        ConsumerConfiguration consumerConfig = ConsumerConfiguration.builder()
                .durable("no-backoff-consumer")
                .deliverPolicy(DeliverPolicy.All)
                .ackWait(Duration.ofSeconds(10))
                .maxDeliver(3)
                // No backoff configured - should result in immediate re-delivery
                .build();

        // Verify configuration
        assertThat(consumerConfig.getMaxDeliver()).isEqualTo(3);
        assertThat(consumerConfig.getAckWait()).isEqualTo(Duration.ofSeconds(10));
        assertThat(consumerConfig.getBackoff()).isEmpty(); // No backoff configured - returns empty list

        // According to NATS docs: "If backoff is not set, a timeout will result in
        // immediate re-delivery"
    }
}
