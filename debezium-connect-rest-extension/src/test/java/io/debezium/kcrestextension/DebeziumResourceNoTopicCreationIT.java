/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.kcrestextension;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.is;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests topic creation endpoint which is disabled in Kafka version less than 2.6.0.
 * Debezium Container with 1.2 image is used for the same.
 */
public class DebeziumResourceNoTopicCreationIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(DebeziumResourceNoTopicCreationIT.class);
    private static final String DEBEZIUM_CONTAINER_IMAGE_VERSION = "1.2";

    @BeforeEach
    public void start() {
        TestHelper.stopContainers();
        TestHelper.setupDebeziumContainer(DEBEZIUM_CONTAINER_IMAGE_VERSION);
        TestHelper.startContainers();
    }

    @AfterEach
    public void stop() {
        TestHelper.stopContainers();
    }

    @Test
    public void testTopicCreationEndpoint() {
        given()
                .port(TestHelper.getDebeziumContainer().getFirstMappedPort())
                .when()
                .get(TestHelper.API_PREFIX + TestHelper.TOPIC_CREATION_ENDPOINT)
                .then().log().all()
                .statusCode(200)
                .body(is("false"));
    }
}
