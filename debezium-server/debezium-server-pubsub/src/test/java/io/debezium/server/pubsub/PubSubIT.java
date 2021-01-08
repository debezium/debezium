/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.pubsub;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.enterprise.event.Observes;
import javax.inject.Inject;

import org.awaitility.Awaitility;
import org.fest.assertions.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import com.google.cloud.ServiceOptions;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PushConfig;
import com.google.pubsub.v1.TopicName;

import io.debezium.server.DebeziumServer;
import io.debezium.server.TestConfigSource;
import io.debezium.server.events.ConnectorCompletedEvent;
import io.debezium.server.events.ConnectorStartedEvent;
import io.debezium.testing.testcontainers.PostgresTestResourceLifecycleManager;
import io.debezium.util.Testing;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;

/**
 * Integration test that verifies basic reading from PostgreSQL database and writing to a Google Cloud PubSub stream.
 *
 * @author Jiri Pechanec
 */
@QuarkusTest
@QuarkusTestResource(PostgresTestResourceLifecycleManager.class)
public class PubSubIT {

    private static final int MESSAGE_COUNT = 4;
    // The topic of this name must exist and be empty
    private static final String STREAM_NAME = "testc.inventory.customers";
    private static final String SUBSCRIPTION_NAME = "testsubs";

    protected static Subscriber subscriber;
    private static ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of(ServiceOptions.getDefaultProjectId(), SUBSCRIPTION_NAME);

    {
        Testing.Files.delete(TestConfigSource.OFFSET_STORE_PATH);
        Testing.Files.createTestingFile(PubSubTestConfigSource.OFFSET_STORE_PATH);
    }

    @AfterAll
    static void stop() throws IOException {
        if (subscriber != null) {
            subscriber.stopAsync();
            subscriber.awaitTerminated();

            try (SubscriptionAdminClient subscriptionAdminClient = SubscriptionAdminClient.create()) {
                subscriptionAdminClient.deleteSubscription(subscriptionName);
            }
        }
    }

    @Inject
    DebeziumServer server;

    private static final List<PubsubMessage> messages = Collections.synchronizedList(new ArrayList<>());

    class TestMessageReceiver implements MessageReceiver {

        @Override
        public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
            Testing.print("Message arrived: " + message);
            messages.add(message);
            consumer.ack();
        }
    }

    void setupDependencies(@Observes ConnectorStartedEvent event) throws IOException {
        Testing.Print.enable();

        try (SubscriptionAdminClient subscriptionAdminClient = SubscriptionAdminClient.create()) {
            final TopicName topic = TopicName.ofProjectTopicName(ServiceOptions.getDefaultProjectId(), STREAM_NAME);
            int ackDeadlineSeconds = 0;
            subscriptionAdminClient.createSubscription(subscriptionName, topic,
                    PushConfig.newBuilder().build(), ackDeadlineSeconds);
        }

        subscriber = Subscriber.newBuilder(subscriptionName, new TestMessageReceiver()).build();
        subscriber.startAsync().awaitRunning();

    }

    void connectorCompleted(@Observes ConnectorCompletedEvent event) throws Exception {
        if (!event.isSuccess()) {
            throw (Exception) event.getError().get();
        }
    }

    @Test
    public void testPubSub() throws Exception {
        Awaitility.await().atMost(Duration.ofSeconds(TestConfigSource.waitForSeconds())).until(() -> messages.size() >= MESSAGE_COUNT);
        Assertions.assertThat(messages.size() >= MESSAGE_COUNT);
    }
}
