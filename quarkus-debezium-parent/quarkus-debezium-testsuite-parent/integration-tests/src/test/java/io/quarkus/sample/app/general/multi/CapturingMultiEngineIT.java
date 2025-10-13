/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.quarkus.sample.app.general.multi;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.quarkus.sample.app.conditions.DisableIfSingleEngine;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.RestAssured;

@Tag("external-suite-only")
@QuarkusIntegrationTest
public class CapturingMultiEngineIT {

    @Test
    @DisplayName("Debezium should capture deserialized events from another engine")
    @DisableIfSingleEngine
    void shouldCaptureDeserializedEventsFromAnotherEngine() {
        await().untilAsserted(() -> RestAssured
                .given()
                .redirects().follow(false)
                .when()
                .get("/captured/orders")
                .then()
                .statusCode(200)
                .body("$", hasSize(2))
                .body("[0].id", equalTo(1))
                .body("[0].name", equalTo("pizza"))
                .body("[0].description", equalTo("pizza with peperoni"))
                .body("[1].id", equalTo(2))
                .body("[1].name", equalTo("kebab"))
                .body("[1].description", equalTo("kebab with mayonnaise")));
    }
}
