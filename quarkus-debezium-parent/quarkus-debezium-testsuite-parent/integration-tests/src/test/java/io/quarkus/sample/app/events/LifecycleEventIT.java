/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.quarkus.sample.app.events;

import static io.restassured.RestAssured.get;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.debezium.runtime.events.ConnectorStartedEvent;
import io.debezium.runtime.events.PollingStartedEvent;
import io.debezium.runtime.events.TasksStartedEvent;
import io.quarkus.sample.app.conditions.DisableIfSingleEngine;
import io.quarkus.test.junit.QuarkusIntegrationTest;

/**
 * @author Chris Cranford
 */
@Tag("external-suite-only")
@QuarkusIntegrationTest
public class LifecycleEventIT {

    @Test
    @DisplayName("Test Lifecycle Events for SingleEngine")
    public void testLifecycleEventsForSingleEngine() {
        // Only concerned with up to polling started because we don't stop the connector
        await().untilAsserted(() -> assertThat(
                get("/lifecycle-events")
                        .then()
                        .statusCode(200)
                        .extract().body().jsonPath().getList(".", String.class))
                .containsExactly(
                        ConnectorStartedEvent.class.getName(),
                        TasksStartedEvent.class.getName(),
                        PollingStartedEvent.class.getName()));
    }

    @Test
    @DisplayName("Test Lifecycle Events for multiEngine")
    @DisableIfSingleEngine
    public void testLifecycleEventsForMultiEngine() {
        // Only concerned with up to polling started because we don't stop the connector
        await().untilAsserted(() -> assertThat(
                get("/lifecycle-events?engine=alternative")
                        .then()
                        .statusCode(200)
                        .extract().body().jsonPath().getList(".", String.class))
                .containsExactlyInAnyOrder(
                        ConnectorStartedEvent.class.getName(),
                        TasksStartedEvent.class.getName(),
                        PollingStartedEvent.class.getName()));
    }

}
