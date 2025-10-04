package io.quarkus.sample.app;

import static io.restassured.RestAssured.get;
import static org.hamcrest.Matchers.notNullValue;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import io.quarkus.test.junit.QuarkusIntegrationTest;

@QuarkusIntegrationTest
public class RestExtensionsIT {

    @Test
    @DisplayName("E2E check - Debezium status endpoint should respond OK")
    void shouldReturnDebeziumStatus() {
        await().untilAsserted(() -> {
            get("/api/debezium/status")
                    .then()
                    .statusCode(200)
                    .body(notNullValue());
        });
    }
}
