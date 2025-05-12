/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.sample.app;

import static io.restassured.RestAssured.get;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

import org.junit.jupiter.api.Test;

import io.debezium.runtime.DebeziumManifest;
import io.quarkus.test.junit.QuarkusIntegrationTest;

@QuarkusIntegrationTest
public class SampleNativeApplicationIT {

    @Test
    void smokeTest() {
        await().untilAsserted(() -> assertThat(
                get("/api/debezium/manifest")
                        .then()
                        .statusCode(200)
                        .extract().body().as(DebeziumManifest.class))
                .isEqualTo(new DebeziumManifest(
                        new DebeziumManifest.Connector("io.debezium.connector.postgresql.PostgresConnector"),
                        new DebeziumManifest.Status(DebeziumManifest.Status.State.POLLING))));
    }
}
