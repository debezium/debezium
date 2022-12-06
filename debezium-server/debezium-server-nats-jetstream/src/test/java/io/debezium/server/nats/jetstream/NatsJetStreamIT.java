/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.nats.jetstream;

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
import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.JetStream;
import io.nats.client.Message;
import io.nats.client.Nats;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;

/**
 * Integration test that verifies basic reading from PostgreSQL database and writing to NATS Jetstream subject.
 *
 * @author Thiago Avancini
 */
@QuarkusTest
@QuarkusTestResource(PostgresTestResourceLifecycleManager.class)
@QuarkusTestResource(NatsJetStreamTestResourceLifecycleManager.class)
class NatsJetStreamIT {
    private static final int MESSAGE_COUNT = 4;
    private static final String SUBJECT_NAME = "testc.inventory.customers";

    protected static Connection nc;
    protected static JetStream js;
    protected static Dispatcher d;

    {
        Testing.Files.delete(NatsJetStreamTestConfigSource.OFFSET_STORE_PATH);
        Testing.Files.createTestingFile(NatsJetStreamTestConfigSource.OFFSET_STORE_PATH);
    }

    private static final List<Message> messages = Collections.synchronizedList(new ArrayList<>());

    void setupDependencies(@Observes ConnectorStartedEvent event) {
        Testing.Print.enable();

        // Setup NATS Jetstream connection
        try {
            nc = Nats.connect(NatsJetStreamTestResourceLifecycleManager.getNatsContainerUrl());
            js = nc.jetStream();
        }
        catch (Exception e) {
            Testing.print("Could not connect to NATS Jetstream");
        }

        // Setup message handler
        try {
            d = nc.createDispatcher();
            js.subscribe(SUBJECT_NAME, d, messages::add, true);
        }
        catch (Exception e) {
            Testing.print("Could not register message handler: " + e.getMessage());
        }
    }

    void connectorCompleted(@Observes ConnectorCompletedEvent event) throws Exception {
        if (!event.isSuccess()) {
            throw (Exception) event.getError().get();
        }
    }

    @AfterAll
    static void stop() throws Exception {
        if (d != null) {
            d.unsubscribe(SUBJECT_NAME);
        }
    }

    @Test
    void testNatsStreaming() throws Exception {
        Awaitility.await().atMost(Duration.ofSeconds(NatsJetStreamTestConfigSource.waitForSeconds())).until(() -> messages.size() >= MESSAGE_COUNT);
        Assertions.assertThat(messages.size() >= MESSAGE_COUNT).isTrue();
    }
}
