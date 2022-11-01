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
import java.util.concurrent.ExecutionException;

import javax.enterprise.event.Observes;
import javax.inject.Inject;

import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import com.google.cloud.ServiceOptions;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsublite.AdminClient;
import com.google.cloud.pubsublite.AdminClientSettings;
import com.google.cloud.pubsublite.CloudRegion;
import com.google.cloud.pubsublite.CloudZone;
import com.google.cloud.pubsublite.ProjectId;
import com.google.cloud.pubsublite.SubscriptionName;
import com.google.cloud.pubsublite.SubscriptionPath;
import com.google.cloud.pubsublite.TopicName;
import com.google.cloud.pubsublite.TopicPath;
import com.google.cloud.pubsublite.cloudpubsub.FlowControlSettings;
import com.google.cloud.pubsublite.cloudpubsub.Subscriber;
import com.google.cloud.pubsublite.cloudpubsub.SubscriberSettings;
import com.google.cloud.pubsublite.proto.Subscription;
import com.google.pubsub.v1.PubsubMessage;

import io.debezium.server.DebeziumServer;
import io.debezium.server.TestConfigSource;
import io.debezium.server.events.ConnectorCompletedEvent;
import io.debezium.server.events.ConnectorStartedEvent;
import io.debezium.testing.testcontainers.PostgresTestResourceLifecycleManager;
import io.debezium.util.Testing;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;

/**
 * Integration test that verifies basic reading from PostgreSQL database and writing to a Google Cloud PubSub Lite stream.
 */
@QuarkusTest
@QuarkusTestResource(PostgresTestResourceLifecycleManager.class)
@EnabledIfSystemProperty(named = "debezium.sink.type", matches = "pubsublite")
public class PubSubLiteIT {
    private static final int MESSAGE_COUNT = 4;
    private static final String STREAM_NAME = "testc.inventory.customers";
    private static final String SUBSCRIPTION_NAME = "testsubs";
    private static final String cloudRegion = "us-central1";
    private static final char zoneId = 'b';
    protected static Subscriber subscriber;
    private static final String projectId = ServiceOptions.getDefaultProjectId();

    private static final SubscriptionPath subscriptionPath = SubscriptionPath.newBuilder()
            .setLocation(CloudZone.of(CloudRegion.of(cloudRegion), zoneId))
            .setProject(ProjectId.of(projectId))
            .setName(SubscriptionName.of(SUBSCRIPTION_NAME))
            .build();

    {
        Testing.Files.delete(TestConfigSource.OFFSET_STORE_PATH);
        Testing.Files.createTestingFile(PubSubLiteTestConfigSource.OFFSET_STORE_PATH);
    }

    @AfterAll
    static void stop() throws IOException {
        if (subscriber != null) {
            subscriber.stopAsync();
            subscriber.awaitTerminated();

            AdminClientSettings adminClientSettings = AdminClientSettings.newBuilder().setRegion(CloudRegion.of(cloudRegion)).build();
            try (AdminClient adminClient = AdminClient.create(adminClientSettings)) {
                adminClient.deleteSubscription(subscriptionPath).get();
            }
            catch (ExecutionException | InterruptedException e) {
                Testing.printError(e);
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

        TopicPath topicPath = TopicPath.newBuilder()
                .setProject(ProjectId.of(projectId))
                .setLocation(CloudZone.of(CloudRegion.of(cloudRegion), zoneId))
                .setName(TopicName.of(STREAM_NAME))
                .build();

        Subscription subscription = Subscription.newBuilder()
                .setDeliveryConfig(
                        Subscription.DeliveryConfig.newBuilder()
                                .setDeliveryRequirement(Subscription.DeliveryConfig.DeliveryRequirement.DELIVER_IMMEDIATELY))
                .setName(subscriptionPath.toString())
                .setTopic(topicPath.toString())
                .build();
        AdminClientSettings adminClientSettings = AdminClientSettings.newBuilder().setRegion(CloudRegion.of(cloudRegion)).build();

        FlowControlSettings flowControlSettings = FlowControlSettings.builder()
                .setBytesOutstanding(10 * 1024 * 1024L)
                .setMessagesOutstanding(1000L)
                .build();

        SubscriberSettings subscriberSettings = SubscriberSettings.newBuilder()
                .setSubscriptionPath(subscriptionPath)
                .setReceiver(new TestMessageReceiver())
                .setPerPartitionFlowControlSettings(flowControlSettings)
                .build();

        try (AdminClient adminClient = AdminClient.create(adminClientSettings)) {
            adminClient.createSubscription(subscription).get();
        }
        catch (ExecutionException | InterruptedException e) {
            Testing.printError(e);
        }

        Subscriber subscriber = Subscriber.create(subscriberSettings);
        subscriber.startAsync().awaitRunning();
    }

    void connectorCompleted(@Observes ConnectorCompletedEvent event) throws Exception {
        if (!event.isSuccess()) {
            throw (Exception) event.getError().get();
        }
    }

    @Test
    public void testPubSubLite() throws Exception {
        Awaitility.await().atMost(Duration.ofSeconds(TestConfigSource.waitForSeconds())).until(() -> messages.size() >= MESSAGE_COUNT);
        Assertions.assertThat(messages.size() >= MESSAGE_COUNT);
    }

}
