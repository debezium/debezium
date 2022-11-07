/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server;

import java.time.Duration;

import javax.enterprise.event.Observes;
import javax.inject.Inject;

import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.server.events.ConnectorCompletedEvent;
import io.debezium.server.events.ConnectorStartedEvent;
import io.debezium.testing.testcontainers.PostgresTestResourceLifecycleManager;
import io.debezium.util.Testing;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;

/**
 * Integration test that verifies basic reading from PostgreSQL database.
 *
 * @author Jiri Pechanec
 */
@QuarkusTest
@QuarkusTestResource(PostgresTestResourceLifecycleManager.class)
@EnabledIfSystemProperty(named = "test.apicurio", matches = "false", disabledReason = "DebeziumServerIT doesn't run with apicurio profile.")
@DisabledIfSystemProperty(named = "debezium.format.key", matches = "protobuf")
@DisabledIfSystemProperty(named = "debezium.format.value", matches = "protobuf")
public class DebeziumServerIT {

    private static final int MESSAGE_COUNT = 4;
    @Inject
    DebeziumServer server;

    @Inject
    DebeziumMetrics metrics;

    {
        Testing.Files.delete(TestConfigSource.OFFSET_STORE_PATH);
    }

    void setupDependencies(@Observes ConnectorStartedEvent event) {
        if (!TestConfigSource.isItTest()) {
            return;
        }

    }

    void connectorCompleted(@Observes ConnectorCompletedEvent event) throws Exception {
        if (!event.isSuccess()) {
            throw (Exception) event.getError().get();
        }
    }

    @Test
    public void testPostgresWithJson() throws Exception {
        Testing.Print.enable();
        final TestConsumer testConsumer = (TestConsumer) server.getConsumer();
        Awaitility.await().atMost(Duration.ofSeconds(TestConfigSource.waitForSeconds()))
                .until(() -> (testConsumer.getValues().size() >= MESSAGE_COUNT));
        Assertions.assertThat(testConsumer.getValues().size()).isEqualTo(MESSAGE_COUNT);
        Assertions.assertThat(((String) testConsumer.getValues().get(MESSAGE_COUNT - 1))).contains(
                "\"after\":{\"id\":1004,\"first_name\":\"Anne\",\"last_name\":\"Kretchmar\",\"email\":\"annek@noanswer.org\"}");
    }

    @Test
    public void testDebeziumMetricsWithPostgres() {
        Testing.Print.enable();

        Awaitility.await().atMost(Duration.ofSeconds(TestConfigSource.waitForSeconds())).until(() -> {
            try {
                // snapshot process finished
                // and consuming events finished!
                return metrics.snapshotCompleted()
                        && metrics.streamingQueueCurrentSize() == 0
                        && metrics.maxQueueSize() == CommonConnectorConfig.DEFAULT_MAX_QUEUE_SIZE;
            }
            catch (Exception e) {
                return false;
            }
        });
    }
}
