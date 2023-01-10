/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.pubsub;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.enterprise.event.Observes;
import javax.inject.Inject;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.NotFoundException;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.ServiceOptions;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PushConfig;
import com.google.pubsub.v1.Topic;
import com.google.pubsub.v1.TopicName;

import io.debezium.server.DebeziumServer;
import io.debezium.server.TestConfigSource;
import io.debezium.server.events.ConnectorCompletedEvent;
import io.debezium.server.events.ConnectorStartedEvent;
import io.debezium.testing.testcontainers.PostgresTestResourceLifecycleManager;
import io.debezium.util.Testing;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;

/**
 * Integration test that verifies basic reading from PostgreSQL database and writing to a Google Cloud PubSub stream running on a Google PubSub Emulator
 *
 * @author Jiri Pechanec
 */
@QuarkusTest
@QuarkusTestResource(PostgresTestResourceLifecycleManager.class)
@QuarkusTestResource(PubSubTestResourceLifecycleManager.class)
public class PubSubIT {

    private static final int MESSAGE_COUNT = 4;
    // The topic of this name must exist and be empty
    private static final String STREAM_NAME = "testc.inventory.customers";
    private static final String SUBSCRIPTION_NAME = "testsubs";
    protected static Subscriber subscriber;
    private static ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of(ServiceOptions.getDefaultProjectId(), SUBSCRIPTION_NAME);
    private static TopicName topicName = TopicName.of(ServiceOptions.getDefaultProjectId(), STREAM_NAME);
    {
        Testing.Files.delete(TestConfigSource.OFFSET_STORE_PATH);
        Testing.Files.createTestingFile(PubSubTestConfigSource.OFFSET_STORE_PATH);
    }

    private static ManagedChannel channel;
    private static TransportChannelProvider channelProvider;
    private static CredentialsProvider credentialsProvider;

    @AfterAll
    static void stop() throws IOException {
        if (subscriber != null) {
            subscriber.stopAsync();
            subscriber.awaitTerminated();

            try (SubscriptionAdminClient subscriptionAdminClient = createSubscriptionAdminClient()) {
                subscriptionAdminClient.deleteSubscription(subscriptionName);
            }

            try (TopicAdminClient topicAdminClient = createTopicAdminClient()) {
                topicAdminClient.deleteTopic(topicName);
            }
        }
        if (channel != null && !channel.isShutdown()) {
            channel.shutdown();
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

        createChannel();

        // get into a clean state before running the test
        try (SubscriptionAdminClient subscriptionAdminClient = createSubscriptionAdminClient()) {
            subscriptionAdminClient.deleteSubscription(subscriptionName);
        }
        catch (NotFoundException e) {
        }

        try (TopicAdminClient topicAdminClient = createTopicAdminClient()) {
            topicAdminClient.deleteTopic(topicName);
        }
        catch (NotFoundException e) {
        }

        // setup topic and sub
        try (TopicAdminClient topicAdminClient = createTopicAdminClient()) {
            Topic topic = topicAdminClient.createTopic(topicName);
            Testing.print("Created topic: " + topic.getName());
        }

        try (SubscriptionAdminClient subscriptionAdminClient = createSubscriptionAdminClient()) {
            int ackDeadlineSeconds = 0;
            subscriptionAdminClient.createSubscription(subscriptionName, topicName,
                    PushConfig.newBuilder().build(), ackDeadlineSeconds);
        }

        subscriber = createSubscriber();
        subscriber.startAsync().awaitRunning();

    }

    void createChannel() {
        channel = ManagedChannelBuilder.forTarget(PubSubTestResourceLifecycleManager.getEmulatorEndpoint()).usePlaintext().build();
        channelProvider = FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel));
        credentialsProvider = NoCredentialsProvider.create();
        Testing.print("Executing test towards pubsub emulator running at: " + PubSubTestResourceLifecycleManager.getEmulatorEndpoint());
    }

    Subscriber createSubscriber() {
        return Subscriber.newBuilder(subscriptionName, new TestMessageReceiver())
                .setChannelProvider(channelProvider)
                .setCredentialsProvider(credentialsProvider)
                .build();

    }

    static SubscriptionAdminClient createSubscriptionAdminClient() throws IOException {
        return SubscriptionAdminClient.create(
                SubscriptionAdminSettings.newBuilder()
                        .setTransportChannelProvider(channelProvider)
                        .setCredentialsProvider(credentialsProvider)
                        .build());

    }

    static TopicAdminClient createTopicAdminClient() throws IOException {
        return TopicAdminClient.create(
                TopicAdminSettings.newBuilder()
                        .setTransportChannelProvider(channelProvider)
                        .setCredentialsProvider(credentialsProvider)
                        .build());

    }

    void connectorCompleted(@Observes ConnectorCompletedEvent event) throws Exception {
        if (!event.isSuccess()) {
            throw (Exception) event.getError().get();
        }
    }

    @Test
    public void testPubSub() {
        Awaitility.await()
                .atMost(Duration.ofSeconds(TestConfigSource.waitForSeconds()))
                .until(() -> messages.size() >= MESSAGE_COUNT);

        assertThat(messages.size()).isGreaterThanOrEqualTo(MESSAGE_COUNT);
    }
}
