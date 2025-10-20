/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.quarkus.sample.app.general.multi;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.startsWith;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.quarkus.sample.app.conditions.DisableIfSingleEngine;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.RestAssured;

@Tag("external-suite-only")
@QuarkusIntegrationTest
public class MultiEngineIT {

    @Test
    @DisplayName("should get multiple engines")
    @DisableIfSingleEngine
    void shouldGetMultipleEngines() {
        await().untilAsserted(() -> RestAssured
                .given()
                .redirects().follow(false)
                .when()
                .get("/engine/manifest")
                .then()
                .statusCode(200)
                .body("$", hasSize(2))
                .body("[0].id", equalTo("default"))
                .body("[0].connector", startsWith("io.debezium.connector"))
                .body("[1].id", equalTo("alternative"))
                .body("[1].connector", startsWith("io.debezium.connector")));
    }

}
