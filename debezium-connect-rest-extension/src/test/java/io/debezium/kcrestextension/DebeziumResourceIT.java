/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.kcrestextension;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;

import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DebeziumResourceIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(DebeziumResourceIT.class);
    private static int EXPOSED_PORT;

    @BeforeEach
    public void setup() {
        TestHelper.startContainers();
        EXPOSED_PORT = TestHelper.getDebeziumContainer().getFirstMappedPort();
    }

    @Test
    public void testTopicCreationEndpoint() {
        given()
                .port(EXPOSED_PORT)
                .when()
                .get(TestHelper.API_PREFIX + TestHelper.TOPIC_CREATION_ENDPOINT)
                .then().log().all()
                .statusCode(200)
                .body(is("true"));
    }

    @Test
    public void testTransformsEndpoint() {
        given()
                .port(EXPOSED_PORT)
                .when().get(TestHelper.API_PREFIX + TestHelper.TRANSFORMS_ENDPOINT)
                .then().log().all()
                .statusCode(200)
                .body("transform.size()", is(33))
                .body("transform",
                        containsInAnyOrder(List.of("io.debezium.connector.mongodb.transforms.ExtractNewDocumentState",
                                "io.debezium.connector.mysql.transforms.ReadToInsertEvent", "io.debezium.transforms.ByLogicalTableRouter",
                                "io.debezium.transforms.ContentBasedRouter", "io.debezium.transforms.ExtractNewRecordState", "io.debezium.transforms.Filter",
                                "io.debezium.transforms.outbox.EventRouter", "io.debezium.transforms.tracing.ActivateTracingSpan",
                                "org.apache.kafka.connect.transforms.predicates.HasHeaderKey", "org.apache.kafka.connect.transforms.predicates.RecordIsTombstone",
                                "org.apache.kafka.connect.transforms.predicates.TopicNameMatches", "org.apache.kafka.connect.transforms.Cast$Key",
                                "org.apache.kafka.connect.transforms.Cast$Value", "org.apache.kafka.connect.transforms.ExtractField$Key",
                                "org.apache.kafka.connect.transforms.ExtractField$Value", "org.apache.kafka.connect.transforms.Filter",
                                "org.apache.kafka.connect.transforms.Flatten$Key", "org.apache.kafka.connect.transforms.Flatten$Value",
                                "org.apache.kafka.connect.transforms.HoistField$Key", "org.apache.kafka.connect.transforms.HoistField$Value",
                                "org.apache.kafka.connect.transforms.InsertField$Key",
                                "org.apache.kafka.connect.transforms.InsertField$Value", "org.apache.kafka.connect.transforms.MaskField$Key",
                                "org.apache.kafka.connect.transforms.MaskField$Value", "org.apache.kafka.connect.transforms.RegexRouter",
                                "org.apache.kafka.connect.transforms.ReplaceField$Key", "org.apache.kafka.connect.transforms.ReplaceField$Value",
                                "org.apache.kafka.connect.transforms.SetSchemaMetadata$Key", "org.apache.kafka.connect.transforms.SetSchemaMetadata$Value",
                                "org.apache.kafka.connect.transforms.TimestampConverter$Key", "org.apache.kafka.connect.transforms.TimestampConverter$Value",
                                "org.apache.kafka.connect.transforms.TimestampRouter", "org.apache.kafka.connect.transforms.ValueToKey").toArray()));
    }
}
