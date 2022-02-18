package io.debezium.server.pubsub;

import com.google.cloud.pubsublite.v1.AdminServiceClient;
import com.google.pubsub.v1.PubsubMessage;
import io.debezium.server.DebeziumServer;
import io.debezium.server.TestConfigSource;
import io.debezium.server.events.ConnectorStartedEvent;
import io.debezium.testing.testcontainers.PostgresTestResourceLifecycleManager;
import io.debezium.util.Testing;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;

import javax.enterprise.event.Observes;
import javax.inject.Inject;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Integration test that verifies basic reading from PostgreSQL database and writing to a Google Cloud PubSub Lite stream.
 */
@QuarkusTest
@QuarkusTestResource(PostgresTestResourceLifecycleManager.class)
public class PubSubLiteIT {
    private static final int MESSAGE_COUNT = 4;
    // The topic of this name must exist and be empty
    private static final String STREAM_NAME = "testc.inventory.customers";
    private static final String SUBSCRIPTION_NAME = "testsubs";

    {
        Testing.Files.delete(TestConfigSource.OFFSET_STORE_PATH);
        Testing.Files.createTestingFile(PubSubLiteTestConfigSource.OFFSET_STORE_PATH);
    }

    @Inject
    DebeziumServer server;

    private static final List<PubsubMessage> messages = Collections.synchronizedList(new ArrayList<>());

    void setupDependencies(@Observes ConnectorStartedEvent event) throws IOException {
        Testing.Print.enable();
        try (AdminServiceClient adminServiceClient = AdminServiceClient.create()) {
            LocationName parent = LocationName.of("[PROJECT]", "[LOCATION]");
            Topic topic = Topic.newBuilder().build();
            String topicId = "topicId-1139259734";
            Topic response = adminServiceClient.createTopic(parent, topic, topicId);
        }

    }


}
