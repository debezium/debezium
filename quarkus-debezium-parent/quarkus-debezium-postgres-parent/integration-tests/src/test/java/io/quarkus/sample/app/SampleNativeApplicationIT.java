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

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import io.debezium.runtime.DebeziumStatus;
import io.quarkus.debezium.notification.SnapshotCompleted;
import io.quarkus.debezium.notification.SnapshotInProgress;
import io.quarkus.debezium.notification.SnapshotStarted;
import io.quarkus.debezium.notification.SnapshotTableScanCompleted;
import io.quarkus.sample.app.test.DisableIfSingleEngine;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.RestAssured;

@QuarkusIntegrationTest
public class SampleNativeApplicationIT {

    @Test
    @DisplayName("Debezium should start polling")
    void shouldDebeziumStartPolling() {
        await().untilAsserted(() -> assertThat(
                get("/api/debezium/status")
                        .then()
                        .statusCode(200)
                        .extract().body().as(DebeziumStatus.class))
                .isEqualTo(new DebeziumStatus(DebeziumStatus.State.POLLING)));
    }

    @Test
    @DisplayName("Debezium should capture events")
    void shouldDebeziumSendCaptureEvents() {
        await().untilAsserted(() -> RestAssured
                .given()
                .redirects().follow(false)
                .when()
                .get("/api/debezium/captured")
                .then()
                .statusCode(302));
    }

    @Test
    @DisplayName("Debezium should capture deserialized events")
    void shouldCaptureDeserializedEvents() {
        await().untilAsserted(() -> RestAssured
                .given()
                .redirects().follow(false)
                .when()
                .get("/api/debezium/products")
                .then()
                .statusCode(200)
                .body("$", hasSize(2))
                .body("[0].id", equalTo(1))
                .body("[0].name", equalTo("t-shirt"))
                .body("[0].description", equalTo("red hat t-shirt"))
                .body("[1].id", equalTo(2))
                .body("[1].name", equalTo("sweatshirt"))
                .body("[1].description", equalTo("blue ibm sweatshirt")));
    }

    @Test
    @DisplayName("Debezium should capture deserialized events from another engine")
    @DisableIfSingleEngine
    void shouldCaptureDeserializedEventsFromAnotherEngine() {
        await().untilAsserted(() -> RestAssured
                .given()
                .redirects().follow(false)
                .when()
                .get("/api/debezium/orders")
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

    @Test
    @DisplayName("should get multiple engines")
    @DisableIfSingleEngine
    void shouldGetMultipleEngines() {
        await().untilAsserted(() -> RestAssured
                .given()
                .redirects().follow(false)
                .when()
                .get("/api/debezium/engines")
                .then()
                .statusCode(200)
                .body("$", hasSize(2))
                .body("[0].group", equalTo("default"))
                .body("[0].connector", equalTo("io.debezium.connector.postgresql.PostgresConnector"))
                .body("[1].group", equalTo("alternative"))
                .body("[1].connector", equalTo("io.debezium.connector.postgresql.PostgresConnector")));
    }

    @Test
    @DisplayName("should get snapshot notifications")
    void shouldGetSnapshotNotifications() {
        await().untilAsserted(() -> Assertions.assertThat(
                get("/notifications")
                        .then()
                        .statusCode(200)
                        .extract().body().jsonPath().getList(".", String.class))
                .containsExactlyInAnyOrder(
                        SnapshotStarted.class.getName(),
                        SnapshotInProgress.class.getName(),
                        SnapshotInProgress.class.getName(),
                        SnapshotTableScanCompleted.class.getName(),
                        SnapshotTableScanCompleted.class.getName(),
                        SnapshotCompleted.class.getName()));
    }
}
