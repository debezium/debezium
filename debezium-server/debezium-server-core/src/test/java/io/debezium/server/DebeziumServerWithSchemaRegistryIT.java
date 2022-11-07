/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;

import javax.enterprise.event.Observes;
import javax.inject.Inject;

import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import io.debezium.server.events.ConnectorCompletedEvent;
import io.debezium.server.events.ConnectorStartedEvent;
import io.debezium.testing.testcontainers.PostgresTestResourceLifecycleManager;
import io.debezium.util.Testing;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;

@QuarkusTest
@QuarkusTestResource(PostgresTestResourceLifecycleManager.class)
@TestProfile(DebeziumServerSchemaRegistryProfile.class)
@EnabledIfSystemProperty(named = "debezium.format.key", matches = "protobuf")
@EnabledIfSystemProperty(named = "debezium.format.value", matches = "protobuf")
public class DebeziumServerWithSchemaRegistryIT {

    private static final int MESSAGE_COUNT = 4;
    @Inject
    DebeziumServer server;

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
    public void testPostgresWithProtobuf() throws Exception {
        Testing.Print.enable();
        final TestConsumer testConsumer = (TestConsumer) server.getConsumer();
        Awaitility.await().atMost(Duration.ofSeconds(TestConfigSource.waitForSeconds()))
                .until(() -> (testConsumer.getValues().size() >= MESSAGE_COUNT));
        Assertions.assertThat(testConsumer.getValues().size()).isEqualTo(MESSAGE_COUNT);
        Assertions.assertThat(testConsumer.getValues().get(0)).isInstanceOf(byte[].class);
        Assertions.assertThat(testConsumer.getValues().get(0)).isNotNull();
        assertThat(((byte[]) testConsumer.getValues().get(0))[0]).isEqualTo((byte) 0);
    }
}
