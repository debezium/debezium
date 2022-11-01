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

import io.debezium.server.events.ConnectorCompletedEvent;
import io.debezium.server.events.ConnectorStartedEvent;
import io.debezium.testing.testcontainers.PostgresTestResourceLifecycleManager;
import io.debezium.util.Testing;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;

/**
 * Integration test that verifies basic reading from PostgreSQL database.
 *
 * @author Oren Elias
 */
@QuarkusTest
@TestProfile(DebeziumServerFileConfigProviderProfile.class)
@QuarkusTestResource(PostgresTestResourceLifecycleManager.class)
@EnabledIfSystemProperty(named = "test.apicurio", matches = "false", disabledReason = "DebeziumServerConfigProvidersIT doesn't run with apicurio profile.")
@DisabledIfSystemProperty(named = "debezium.format.key", matches = "protobuf")
@DisabledIfSystemProperty(named = "debezium.format.value", matches = "protobuf")
public class DebeziumServerConfigProvidersIT {

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
    public void testPostgresWithJson() throws Exception {
        Testing.Print.enable();
        final TestConsumer testConsumer = (TestConsumer) server.getConsumer();
        Awaitility.await().atMost(Duration.ofSeconds(TestConfigSource.waitForSeconds()))
                .until(() -> (testConsumer.getValues().size() >= MESSAGE_COUNT));
        Assertions.assertThat(testConsumer.getValues().size()).isEqualTo(MESSAGE_COUNT);
        Assertions.assertThat(((String) testConsumer.getValues().get(MESSAGE_COUNT - 1))).contains(
                "\"after\":{\"id\":1004,\"first_name\":\"Anne\",\"last_name\":\"Kretchmar\",\"email\":\"annek@noanswer.org\"}");
    }
}
