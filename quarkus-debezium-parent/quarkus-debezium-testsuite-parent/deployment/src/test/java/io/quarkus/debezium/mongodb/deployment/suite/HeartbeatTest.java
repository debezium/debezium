/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.mongodb.deployment.suite;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.given;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.debezium.runtime.events.DebeziumHeartbeat;
import io.quarkus.debezium.mongodb.deployment.SuiteTags;
import io.quarkus.test.QuarkusUnitTest;

@Tag(SuiteTags.DEFAULT)
public class HeartbeatTest {

    @Inject
    HeartbeatHandler heartbeatHandler;

    @RegisterExtension
    static final QuarkusUnitTest setup = new QuarkusUnitTest()
            .withConfigurationResource("quarkus-debezium-testsuite.properties");

    @Test
    @DisplayName("should observe heartbeat events")
    void shouldObserveHeartbeatEvents() {
        given().await()
                .atMost(100, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(heartbeatHandler.isInvoked()).isTrue());
    }

    @ApplicationScoped
    public static class HeartbeatHandler {
        private final AtomicBoolean invoked = new AtomicBoolean(false);

        public void observe(@Observes DebeziumHeartbeat heartBeat) {
            invoked.set(true);
        }

        public boolean isInvoked() {
            return invoked.getAndSet(false);
        }
    }
}
