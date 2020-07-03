/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.eventhubs;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import javax.enterprise.event.Observes;
import javax.inject.Inject;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import com.azure.core.util.IterableStream;
import com.azure.messaging.eventhubs.EventHubClientBuilder;
import com.azure.messaging.eventhubs.EventHubConsumerClient;
import com.azure.messaging.eventhubs.EventHubProducerClient;
import com.azure.messaging.eventhubs.models.EventPosition;
import com.azure.messaging.eventhubs.models.PartitionEvent;

import io.debezium.server.DebeziumServer;
import io.debezium.server.TestDatabase;
import io.debezium.server.events.ConnectorCompletedEvent;
import io.debezium.server.events.ConnectorStartedEvent;
import io.debezium.util.Testing;
import io.quarkus.test.junit.QuarkusTest;

/**
 * Integration test that verifies basic reading from PostgreSQL database and
 * writing to Azure Event Hubs.
 *
 * @author Abhishek Gupta
 */
@QuarkusTest
public class EventHubsIT {

    private static final int MESSAGE_COUNT = 4;
    private static final String CONSUMER_GROUP = "$Default";

    protected static TestDatabase db = null;
    protected static EventHubProducerClient producer = null;

    {
        Testing.Files.delete(EventHubsConfigSource.OFFSET_STORE_PATH);
        Testing.Files.createTestingFile(EventHubsConfigSource.OFFSET_STORE_PATH);
    }

    @AfterAll
    static void stop() {
        if (db != null) {
            db.stop();
        }
        if (producer != null) {
            producer.close();
        }
    }

    @Inject
    DebeziumServer server;

    void setupDependencies(@Observes ConnectorStartedEvent event) {
        producer = new EventHubClientBuilder().connectionString(EventHubsConfigSource.EVENTHUBS_CONNECTION_STRING)
                .buildProducerClient();
        db = new TestDatabase();
        db.start();
    }

    void connectorCompleted(@Observes ConnectorCompletedEvent event) throws Exception {
        if (!event.isSuccess()) {
            throw (Exception) event.getError().get();
        }
    }

    @Test
    public void testEventHubs() throws Exception {
        Testing.Print.enable();
        EventHubConsumerClient consumer = new EventHubClientBuilder()
                .connectionString(EventHubsConfigSource.EVENTHUBS_CONNECTION_STRING).consumerGroup(CONSUMER_GROUP)
                .buildConsumerClient();

        final List<PartitionEvent> expected = new ArrayList<>();

        Awaitility.await().atMost(Duration.ofSeconds(EventHubsConfigSource.waitForSeconds())).until(() -> {
            IterableStream<PartitionEvent> events = consumer.receiveFromPartition("0", MESSAGE_COUNT,
                    EventPosition.latest());

            events.forEach(event -> expected.add(event));
            return expected.size() >= MESSAGE_COUNT;
        });
    }
}
