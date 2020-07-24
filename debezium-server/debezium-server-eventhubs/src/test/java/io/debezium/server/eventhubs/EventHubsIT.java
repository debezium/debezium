/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.eventhubs;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import javax.enterprise.event.Observes;
import javax.inject.Inject;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private static final Logger LOGGER = LoggerFactory.getLogger(EventHubsIT.class);

    private static final int MESSAGE_COUNT = 4;
    private static final String CONSUMER_GROUP = "$Default";

    protected static TestDatabase db = null;
    protected static EventHubProducerClient producer = null;
    protected static EventHubConsumerClient consumer = null;

    {
        Testing.Files.delete(EventHubsTestConfigSource.OFFSET_STORE_PATH);
        Testing.Files.createTestingFile(EventHubsTestConfigSource.OFFSET_STORE_PATH);
    }

    @AfterAll
    static void stop() {
        if (db != null) {
            db.stop();
        }
        if (producer != null) {
            producer.close();
        }
        if (consumer != null) {
            consumer.close();
        }
    }

    @Inject
    DebeziumServer server;

    void setupDependencies(@Observes ConnectorStartedEvent event) {
        String finalConnectionString = String.format("%s;EntityPath=%s",
                EventHubsTestConfigSource.getEventHubsConnectionString(), EventHubsTestConfigSource.getEventHubsName());

        producer = new EventHubClientBuilder().connectionString(finalConnectionString).buildProducerClient();
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

        String finalConnectionString = String.format("%s;EntityPath=%s",
                EventHubsTestConfigSource.getEventHubsConnectionString(), EventHubsTestConfigSource.getEventHubsName());

        consumer = new EventHubClientBuilder().connectionString(finalConnectionString).consumerGroup(CONSUMER_GROUP)
                .buildConsumerClient();

        final List<PartitionEvent> expected = new ArrayList<>();

        Awaitility.await().atMost(Duration.ofSeconds(EventHubsTestConfigSource.waitForSeconds())).until(() -> {
            IterableStream<PartitionEvent> events = consumer.receiveFromPartition("0", MESSAGE_COUNT,
                    EventPosition.latest());

            events.forEach(event -> expected.add(event));
            return expected.size() >= MESSAGE_COUNT;
        });

        // check whether the event data contains expected id i.e. 1001, 1002, 1003 and
        // 1004
        String eventBody = null;
        String expectedID = null;
        final String idPart = "\"id\":100";

        // since all messages go to same partition, ordering will be maintained
        // (assuming no errors)
        for (int i = 0; i < MESSAGE_COUNT; i++) {
            eventBody = expected.get(i).getData().getBodyAsString();
            expectedID = idPart + String.valueOf(i + 1);
            assertTrue(eventBody.contains(expectedID), expectedID + " not found in payload");
        }
    }
}
