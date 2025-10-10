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

import io.quarkus.debezium.mongodb.deployment.SuiteTags;
import io.quarkus.debezium.notification.DebeziumNotification;
import io.quarkus.debezium.notification.SnapshotEvent;
import io.quarkus.test.QuarkusUnitTest;

@Tag(SuiteTags.DEFAULT)
public class NotificationTest {

    @Inject
    SnapshotNotificationsHandler snapshotNotificationsHandler;

    @Inject
    DebeziumNotificationsHandler debeziumNotificationsHandler;

    @RegisterExtension
    static final QuarkusUnitTest setup = new QuarkusUnitTest()
            .withConfigurationResource("quarkus-debezium-testsuite.properties");

    @Test
    @DisplayName("should observe events for snapshot")
    void shouldObserveSnapshotEvents() {
        given().await()
                .atMost(100, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(snapshotNotificationsHandler.isInvoked()).isTrue());

    }

    @Test
    @DisplayName("should observe general events")
    void shouldObserveDebeziumEvents() {
        given().await()
                .atMost(100, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(debeziumNotificationsHandler.isInvoked()).isTrue());

    }

    @ApplicationScoped
    static class SnapshotNotificationsHandler {
        private final AtomicBoolean invoked = new AtomicBoolean(false);

        public void observe(@Observes SnapshotEvent event) {
            invoked.set(true);
        }

        public boolean isInvoked() {
            return invoked.getAndSet(false);
        }
    }

    @ApplicationScoped
    static class DebeziumNotificationsHandler {
        private final AtomicBoolean invoked = new AtomicBoolean(false);

        public void observe(@Observes DebeziumNotification event) {
            invoked.set(true);
        }

        public boolean isInvoked() {
            return invoked.getAndSet(false);
        }
    }
}
