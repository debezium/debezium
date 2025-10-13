/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.quarkus.sample.app.general.multi;

import static io.restassured.RestAssured.get;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.quarkus.debezium.notification.SnapshotCompleted;
import io.quarkus.debezium.notification.SnapshotInProgress;
import io.quarkus.debezium.notification.SnapshotStarted;
import io.quarkus.debezium.notification.SnapshotTableScanCompleted;
import io.quarkus.sample.app.conditions.DisableIfSingleEngine;
import io.quarkus.test.junit.QuarkusIntegrationTest;

@Tag("external-suite-only")
@QuarkusIntegrationTest
public class NotificationMultiEngineIT {

    @Test
    @DisplayName("should get snapshot notifications fromAnotherEngine")
    @DisableIfSingleEngine
    void shouldGetSnapshotNotificationsFromAnotherEngine() {
        await().untilAsserted(() -> Assertions.assertThat(
                get("/notifications?engine=alternative")
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
