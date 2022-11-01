/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.nats.streaming;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.enterprise.event.Observes;

import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import io.debezium.server.events.ConnectorCompletedEvent;
import io.debezium.server.events.ConnectorStartedEvent;
import io.debezium.testing.testcontainers.PostgresTestResourceLifecycleManager;
import io.debezium.util.Testing;
import io.nats.streaming.Message;
import io.nats.streaming.MessageHandler;
import io.nats.streaming.NatsStreaming;
import io.nats.streaming.Options;
import io.nats.streaming.StreamingConnection;
import io.nats.streaming.Subscription;
import io.nats.streaming.SubscriptionOptions;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;

/**
 * Integration test that verifies basic reading from PostgreSQL database and writing to NATS Streaming subject.
 *
 * @author Thiago Avancini
 */
@QuarkusTest
@QuarkusTestResource(PostgresTestResourceLifecycleManager.class)
@QuarkusTestResource(NatsStreamingTestResourceLifecycleManager.class)
public class NatsStreamingIT {
    private static final int MESSAGE_COUNT = 4;
    private static final String SUBJECT_NAME = "testc.inventory.customers";
    private static final String CLUSTER_ID = "debezium";
    private static final String CLIENT_ID = "debezium-test";

    protected static StreamingConnection sc;
    protected static Subscription subscription;

    {
        Testing.Files.delete(NatsStreamingTestConfigSource.OFFSET_STORE_PATH);
        Testing.Files.createTestingFile(NatsStreamingTestConfigSource.OFFSET_STORE_PATH);
    }

    private static final List<Message> messages = Collections.synchronizedList(new ArrayList<>());

    void setupDependencies(@Observes ConnectorStartedEvent event) {
        Testing.Print.enable();

        // Setup NATS Streaming connection
        Options stanOptions = new Options.Builder()
                .natsUrl(NatsStreamingTestResourceLifecycleManager.getNatsStreamingContainerUrl())
                .build();
        try {
            sc = NatsStreaming.connect(CLUSTER_ID, CLIENT_ID, stanOptions);
        }
        catch (Exception e) {
            Testing.print("Could not connect to NATS Streaming");
        }

        // Setup message handler
        try {
            subscription = sc.subscribe(SUBJECT_NAME, new MessageHandler() {
                public void onMessage(Message m) {
                    messages.add(m);
                }
            }, new SubscriptionOptions.Builder().deliverAllAvailable().build());
        }
        catch (Exception e) {
            Testing.print("Could not register message handler");
        }
    }

    void connectorCompleted(@Observes ConnectorCompletedEvent event) throws Exception {
        if (!event.isSuccess()) {
            throw (Exception) event.getError().get();
        }
    }

    @AfterAll
    static void stop() throws Exception {
        if (subscription != null) {
            subscription.unsubscribe();
        }
        sc.close();
    }

    @Test
    public void testNatsStreaming() throws Exception {
        Awaitility.await().atMost(Duration.ofSeconds(NatsStreamingTestConfigSource.waitForSeconds())).until(() -> messages.size() >= MESSAGE_COUNT);
        Assertions.assertThat(messages.size() >= MESSAGE_COUNT);
    }
}
