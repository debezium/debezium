/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.quarkus.sample.app;

import static io.restassured.RestAssured.get;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import io.debezium.runtime.DebeziumStatus;
import io.quarkus.sample.app.test.DisableIfMultiEngine;
import io.quarkus.sample.app.test.DisableIfSingleEngine;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.RestAssured;

@QuarkusIntegrationTest
public class EngineIT {

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
                .body("[0].connector", equalTo("io.debezium.connector.postgresql.PostgresConnector")));
    }

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
                .body("[0].connector", equalTo("io.debezium.connector.postgresql.PostgresConnector"))
                .body("[1].id", equalTo("alternative"))
                .body("[1].connector", equalTo("io.debezium.connector.postgresql.PostgresConnector")));
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
