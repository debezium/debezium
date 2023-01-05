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

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import io.debezium.DebeziumException;
import io.debezium.server.events.ConnectorCompletedEvent;
import io.debezium.server.events.ConnectorStartedEvent;
import io.debezium.testing.testcontainers.PostgresTestResourceLifecycleManager;
import io.debezium.util.Testing;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;

@QuarkusTest
@QuarkusTestResource(PostgresTestResourceLifecycleManager.class)
@TestProfile(DebeziumServerApicurioProfile.class)
@EnabledIfSystemProperty(named = "test.apicurio", matches = "true", disabledReason = "DebeziumServerWithApicurioIT only runs when apicurio profile is enabled.")
public class DebeziumServerWithApicurioIT {

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
            throw new DebeziumException(event.getError().get());
        }
    }

    @Test
    @EnabledIfSystemProperty(named = "test.apicurio.converter.format", matches = "avro")
    public void testPostgresWithApicurioAvro() throws Exception {
        Testing.Print.enable();
        final TestConsumer testConsumer = (TestConsumer) server.getConsumer();
        Awaitility.await().atMost(Duration.ofSeconds(TestConfigSource.waitForSeconds()))
                .until(() -> (testConsumer.getValues().size() >= MESSAGE_COUNT));
        assertThat(testConsumer.getValues().size()).isEqualTo(MESSAGE_COUNT);
        assertThat(testConsumer.getValues().get(0)).isInstanceOf(byte[].class);
        assertThat(testConsumer.getValues().get(0)).isNotNull();
        assertThat(((byte[]) testConsumer.getValues().get(0))[0]).isEqualTo((byte) 0);
    }

    @Test
    @EnabledIfSystemProperty(named = "test.apicurio.converter.format", matches = "json")
    public void testPostgresWithApicurioExtJson() throws Exception {
        Testing.Print.enable();
        final TestConsumer testConsumer = (TestConsumer) server.getConsumer();
        Awaitility.await().atMost(Duration.ofSeconds(TestConfigSource.waitForSeconds()))
                .until(() -> (testConsumer.getValues().size() >= MESSAGE_COUNT));
        assertThat(testConsumer.getValues().size()).isEqualTo(MESSAGE_COUNT);
        assertThat(testConsumer.getValues().get(0)).isInstanceOf(String.class);
        assertThat(((String) testConsumer.getValues().get(MESSAGE_COUNT - 1))).contains(
                "\"after\":{\"id\":1004,\"first_name\":\"Anne\",\"last_name\":\"Kretchmar\",\"email\":\"annek@noanswer.org\"}");
    }
}
