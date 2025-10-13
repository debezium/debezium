/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.quarkus.sample.app.general.single;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.RestAssured;

@Tag("external-suite-only")
@QuarkusIntegrationTest
public class CapturingSingleEngineIT {

    @Test
    @DisplayName("Debezium should capture events")
    void shouldDebeziumSendCaptureEvents() {
        await().untilAsserted(() -> RestAssured
                .given()
                .redirects().follow(false)
                .when()
                .get("/captured/all")
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
                .get("/captured/products")
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

}
