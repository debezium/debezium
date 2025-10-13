/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.quarkus.sample.app.general.single;

import static io.restassured.RestAssured.get;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.startsWith;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.debezium.runtime.DebeziumStatus;
import io.quarkus.sample.app.conditions.DisableIfMultiEngine;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.RestAssured;

@Tag("external-suite-only")
@QuarkusIntegrationTest
public class SingleEngineIT {

    @Test
    @DisplayName("should get single engine")
    @DisableIfMultiEngine
    void shouldGetSingleEngine() {
        await().untilAsserted(() -> RestAssured
                .given()
                .redirects().follow(false)
                .when()
                .get("/engine/manifest")
                .then()
                .statusCode(200)
                .body("$", hasSize(1))
                .body("[0].id", equalTo("default"))
                .body("[0].connector", startsWith("io.debezium.connector")));
    }

    @Test
    @DisplayName("Debezium should start polling")
    void shouldDebeziumStartPolling() {
        await().untilAsserted(() -> assertThat(
                get("/engine/status")
                        .then()
                        .statusCode(200)
                        .extract().body().as(DebeziumStatus.class))
                .isEqualTo(new DebeziumStatus(DebeziumStatus.State.POLLING)));
    }
}
