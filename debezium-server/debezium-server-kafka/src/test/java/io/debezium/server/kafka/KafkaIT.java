/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import javax.enterprise.event.Observes;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import io.debezium.server.events.ConnectorCompletedEvent;
import io.debezium.server.events.ConnectorStartedEvent;
import io.debezium.testing.testcontainers.PostgresTestResourceLifecycleManager;
import io.debezium.util.Testing;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;

/**
 * Integration test for verifying that the Kafka sink adapter can stream change events from a PostgreSQL database
 * to a configured Apache Kafka broker.
 *
 * @author Alfusainey Jallow
 */
@QuarkusTest
@QuarkusTestResource(PostgresTestResourceLifecycleManager.class)
@QuarkusTestResource(KafkaTestResourceLifecycleManager.class)
public class KafkaIT {

    private static final String TOPIC_NAME = "testc.inventory.customers";
    private static final int MESSAGE_COUNT = 4;

    private static KafkaConsumer<String, String> consumer;

    {
        Testing.Files.delete(KafkaTestConfigSource.OFFSET_STORE_PATH);
        Testing.Files.createTestingFile(KafkaTestConfigSource.OFFSET_STORE_PATH);
    }

    void setupDependencies(@Observes final ConnectorStartedEvent event) {
        Testing.Print.enable();

        final Map<String, Object> configs = new ConcurrentHashMap<>();

        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaTestResourceLifecycleManager.getBootstrapServers());
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, "test-" + UUID.randomUUID());

        consumer = new KafkaConsumer<>(configs, new StringDeserializer(), new StringDeserializer());
    }

    void connectorCompleted(@Observes final ConnectorCompletedEvent event) throws Exception {
        if (!event.isSuccess()) {
            throw (Exception) event.getError().get();
        }
    }

    @AfterAll
    static void stop() {
        if (consumer != null) {
            consumer.unsubscribe();
            consumer.close();
        }
    }

    @Test
    public void testKafka() {
        Awaitility.await().atMost(Duration.ofSeconds(KafkaTestConfigSource.waitForSeconds())).until(() -> {
            return consumer != null;
        });
        consumer.subscribe(Arrays.asList(TOPIC_NAME));

        final List<ConsumerRecord<String, String>> actual = new ArrayList<>();

        Awaitility.await()
                .atMost(Duration.ofSeconds(KafkaTestConfigSource.waitForSeconds()))
                .until(() -> {
                    consumer.poll(Duration.ofSeconds(KafkaTestConfigSource.waitForSeconds()))
                            .iterator()
                            .forEachRemaining(actual::add);
                    return actual.size() >= MESSAGE_COUNT;
                });
        assertThat(actual.size()).isGreaterThanOrEqualTo(MESSAGE_COUNT);
    }
}
