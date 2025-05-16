/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.sample.app;

import static io.restassured.RestAssured.get;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import io.debezium.runtime.DebeziumStatus;
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
}
