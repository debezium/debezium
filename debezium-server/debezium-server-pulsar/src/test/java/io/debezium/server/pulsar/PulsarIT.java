/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.pulsar;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import javax.enterprise.event.Observes;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.awaitility.Awaitility;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.Test;

import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.server.TestConfigSource;
import io.debezium.server.events.ConnectorCompletedEvent;
import io.debezium.server.events.ConnectorStartedEvent;
import io.debezium.testing.testcontainers.PostgresTestResourceLifecycleManager;
import io.debezium.util.Testing;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;

/**
 * Integration test that verifies basic reading from PostgreSQL database and writing to an Apache Pulsar topic.
 *
 * @author Jiri Pechanec
 */
@QuarkusTest
@QuarkusTestResource(PostgresTestResourceLifecycleManager.class)
@QuarkusTestResource(PulsarTestResourceLifecycleManager.class)
public class PulsarIT {

    private static final int MESSAGE_COUNT = 4;
    private static final String TOPIC_NAME = "testc.inventory.customers";
    private static final String NOKEY_TOPIC_NAME = "testc.inventory.nokey";

    @ConfigProperty(name = "debezium.source.database.hostname")
    String dbHostname;

    @ConfigProperty(name = "debezium.source.database.port")
    String dbPort;

    @ConfigProperty(name = "debezium.source.database.user")
    String dbUser;

    @ConfigProperty(name = "debezium.source.database.password")
    String dbPassword;

    @ConfigProperty(name = "debezium.source.database.dbname")
    String dbName;

    protected static PulsarClient pulsarClient;

    {
        Testing.Files.delete(TestConfigSource.OFFSET_STORE_PATH);
        Testing.Files.createTestingFile(PulsarTestConfigSource.OFFSET_STORE_PATH);
    }

    void setupDependencies(@Observes ConnectorStartedEvent event) throws IOException {
        Testing.Print.enable();

        pulsarClient = PulsarClient.builder()
                .serviceUrl(PulsarTestResourceLifecycleManager.getPulsarServiceUrl())
                .build();

    }

    void connectorCompleted(@Observes ConnectorCompletedEvent event) throws Exception {
        if (!event.isSuccess()) {
            throw new RuntimeException(event.getError().get());
        }
    }

    @Test
    public void testPulsar() throws Exception {
        Awaitility.await().atMost(Duration.ofSeconds(PulsarTestConfigSource.waitForSeconds())).until(() -> {
            return pulsarClient != null;
        });
        final Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(TOPIC_NAME)
                .subscriptionName("test-" + UUID.randomUUID())
                .subscribe();
        final List<Message<String>> records = new ArrayList<>();
        Awaitility.await().atMost(Duration.ofSeconds(PulsarTestConfigSource.waitForSeconds())).until(() -> {
            records.add(consumer.receive());
            return records.size() >= MESSAGE_COUNT;
        });
        final JdbcConfiguration config = JdbcConfiguration.create()
                .with("hostname", dbHostname)
                .with("port", dbPort)
                .with("user", dbUser)
                .with("password", dbPassword)
                .with("dbname", dbName)
                .build();
        try (PostgresConnection connection = new PostgresConnection(config, "Debezium Pulsar Test")) {
            connection.execute(
                    "CREATE TABLE inventory.nokey (val INT);",
                    "INSERT INTO inventory.nokey VALUES (1)",
                    "INSERT INTO inventory.nokey VALUES (2)",
                    "INSERT INTO inventory.nokey VALUES (3)",
                    "INSERT INTO inventory.nokey VALUES (4)");
        }
        final Consumer<String> nokeyConsumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(NOKEY_TOPIC_NAME)
                .subscriptionName("test-" + UUID.randomUUID())
                .subscribe();
        final List<Message<String>> nokeyRecords = new ArrayList<>();
        Awaitility.await().atMost(Duration.ofSeconds(PulsarTestConfigSource.waitForSeconds())).until(() -> {
            nokeyRecords.add(nokeyConsumer.receive());
            return nokeyRecords.size() >= MESSAGE_COUNT;
        });
    }
}
