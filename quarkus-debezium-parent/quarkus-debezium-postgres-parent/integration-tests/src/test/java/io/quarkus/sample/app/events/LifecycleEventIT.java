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
import org.junit.jupiter.api.Test;

import io.debezium.runtime.events.ConnectorStartedEvent;
import io.debezium.runtime.events.PollingStartedEvent;
import io.debezium.runtime.events.TasksStartedEvent;
import io.quarkus.sample.app.test.DisableIfMultiEngine;
import io.quarkus.sample.app.test.DisableIfSingleEngine;
import io.quarkus.test.junit.QuarkusIntegrationTest;

/**
 * @author Chris Cranford
 */
@QuarkusIntegrationTest
public class LifecycleEventIT {

    /**
     * TODO: the following test show the heartbeat functionality should be expressed by engine
     */
    @Test
    @DisplayName("Test Lifecycle Events for SingleEngine")
    @DisableIfMultiEngine
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
                get("/lifecycle-events")
                        .then()
                        .statusCode(200)
                        .extract().body().jsonPath().getList(".", String.class))
                .containsExactlyInAnyOrder(
                        ConnectorStartedEvent.class.getName(),
                        TasksStartedEvent.class.getName(),
                        PollingStartedEvent.class.getName(),
                        ConnectorStartedEvent.class.getName(),
                        TasksStartedEvent.class.getName(),
                        PollingStartedEvent.class.getName()));
    }

}
