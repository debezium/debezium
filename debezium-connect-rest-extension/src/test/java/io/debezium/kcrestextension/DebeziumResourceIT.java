/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.kcrestextension;

import static io.debezium.testing.testcontainers.testhelper.RestExtensionTestInfrastructure.DATABASE;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;

import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.testing.testcontainers.testhelper.RestExtensionTestInfrastructure;

/**
 * Tests topic creation (which is enabled in Kafka version greater than 2.6.0) and transforms endpoints.
 * Debezium Container with 1.7 image is used for the same.
 */
public class DebeziumResourceIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(DebeziumResourceIT.class);

    private static final List<String> SUPPORTED_TRANSFORMS = List.of(
            "io.debezium.connector.mongodb.transforms.ExtractNewDocumentState",
            "io.debezium.connector.mongodb.transforms.outbox.MongoEventRouter",
            "io.debezium.connector.mysql.transforms.ReadToInsertEvent",
            "io.debezium.connector.postgresql.transforms.timescaledb.TimescaleDb",
            "io.debezium.transforms.ByLogicalTableRouter",
            "io.debezium.transforms.ContentBasedRouter",
            "io.debezium.transforms.ExtractChangedRecordState",
            "io.debezium.transforms.ExtractSchemaToNewRecord",
            "io.debezium.transforms.ExtractNewRecordState",
            "io.debezium.transforms.Filter",
            "io.debezium.transforms.HeaderToValue",
            "io.debezium.transforms.SchemaChangeEventFilter",
            "io.debezium.transforms.TimezoneConverter",
            "io.debezium.transforms.outbox.EventRouter",
            "io.debezium.transforms.partitions.PartitionRouting",
            "io.debezium.transforms.tracing.ActivateTracingSpan",
            "org.apache.kafka.connect.transforms.Cast",
            "org.apache.kafka.connect.transforms.DropHeaders",
            "org.apache.kafka.connect.transforms.ExtractField",
            "org.apache.kafka.connect.transforms.Filter",
            "org.apache.kafka.connect.transforms.Flatten",
            "org.apache.kafka.connect.transforms.HeaderFrom",
            "org.apache.kafka.connect.transforms.HoistField",
            "org.apache.kafka.connect.transforms.InsertField",
            "org.apache.kafka.connect.transforms.InsertHeader",
            "org.apache.kafka.connect.transforms.MaskField",
            "org.apache.kafka.connect.transforms.RegexRouter",
            "org.apache.kafka.connect.transforms.ReplaceField",
            "org.apache.kafka.connect.transforms.SetSchemaMetadata",
            "org.apache.kafka.connect.transforms.TimestampConverter",
            "org.apache.kafka.connect.transforms.TimestampRouter",
            "org.apache.kafka.connect.transforms.ValueToKey");

    private static final List<String> SUPPORTED_PREDICATES = List.of(
            "org.apache.kafka.connect.transforms.predicates.HasHeaderKey",
            "org.apache.kafka.connect.transforms.predicates.RecordIsTombstone",
            "org.apache.kafka.connect.transforms.predicates.TopicNameMatches");

    @BeforeEach
    public void start() {
        RestExtensionTestInfrastructure.setupDebeziumContainer(Module.version(), DebeziumConnectRestExtension.class.getName());
    }

    @AfterEach
    public void stop() {
        RestExtensionTestInfrastructure.stopContainers();
    }

    @Test
    public void testTopicCreationEndpoint() {
        RestExtensionTestInfrastructure.startContainers(DATABASE.NONE);
        given()
                .port(RestExtensionTestInfrastructure.getDebeziumContainer().getFirstMappedPort())
                .when()
                .get(DebeziumResource.BASE_PATH + DebeziumResource.TOPIC_CREATION_ENDPOINT)
                .then().log().all()
                .statusCode(200)
                .body(is("true"));
    }

    @Test
    public void testTopicCreationEndpointWhenExplicitlyDisabled() {
        RestExtensionTestInfrastructure.getDebeziumContainer().withEnv("CONNECT_TOPIC_CREATION_ENABLE", "false");
        RestExtensionTestInfrastructure.startContainers(DATABASE.NONE);
        given()
                .port(RestExtensionTestInfrastructure.getDebeziumContainer().getFirstMappedPort())
                .when()
                .get(DebeziumResource.BASE_PATH + DebeziumResource.TOPIC_CREATION_ENDPOINT)
                .then().log().all()
                .statusCode(200)
                .body(is("false"));
    }

    @Test
    public void testTransformsEndpoint() {
        RestExtensionTestInfrastructure.startContainers(DATABASE.NONE);
        given()
                .port(RestExtensionTestInfrastructure.getDebeziumContainer().getFirstMappedPort())
                .when().get(DebeziumResource.BASE_PATH + DebeziumResource.TRANSFORMS_ENDPOINT)
                .then().log().all()
                .statusCode(200)
                .body("transform.size()", is(SUPPORTED_TRANSFORMS.size()))
                .body("transform", containsInAnyOrder(SUPPORTED_TRANSFORMS.toArray()));
    }

    @Test
    public void testPredicatesEndpoint() {
        RestExtensionTestInfrastructure.startContainers(DATABASE.NONE);
        given()
                .port(RestExtensionTestInfrastructure.getDebeziumContainer().getFirstMappedPort())
                .when().get(DebeziumResource.BASE_PATH + DebeziumResource.PREDICATES_ENDPOINT)
                .then().log().all()
                .statusCode(200)
                .body("predicate.size()", is(SUPPORTED_PREDICATES.size()))
                .body("predicate", containsInAnyOrder(SUPPORTED_PREDICATES.toArray()));
    }

    @Test
    public void testConnectorPluginsEndpoint() {
        RestExtensionTestInfrastructure.startContainers(DATABASE.NONE);
        given()
                .port(RestExtensionTestInfrastructure.getDebeziumContainer().getFirstMappedPort())
                .when().get(DebeziumResource.BASE_PATH + DebeziumResource.CONNECTOR_PLUGINS_ENDPOINT)
                .then().log().all()
                .statusCode(200)
                .body("size()", is(DebeziumResource.SUPPORTED_CONNECTORS.size()))
                .body("className", containsInAnyOrder(DebeziumResource.SUPPORTED_CONNECTORS.toArray()));
    }

}
