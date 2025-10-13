/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.sample.app.events.single;

import static io.restassured.RestAssured.get;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.debezium.runtime.events.DebeziumHeartbeat;
import io.quarkus.test.junit.QuarkusIntegrationTest;

@Tag("external-suite-only")
@QuarkusIntegrationTest
public class HeartbeatEventSingleEngineIT {

    @Test
    @DisplayName("should get an heartbeat")
    void shouldGetHeartbeat() {
        await().untilAsserted(() -> assertThat(
                get("/heartbeat?engine=default")
                        .then()
                        .statusCode(200)
                        .extract()
                        .body()
                        .as(DebeziumHeartbeat.class).connector().name())
                .startsWith("io.debezium.connector"));
    }

}
