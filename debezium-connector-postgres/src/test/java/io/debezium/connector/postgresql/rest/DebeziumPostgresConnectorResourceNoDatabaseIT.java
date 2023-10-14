/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.rest;

import static io.debezium.testing.testcontainers.testhelper.RestExtensionTestInfrastructure.DATABASE;
import static io.restassured.RestAssured.given;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.hasKey;

import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import io.debezium.connector.postgresql.Module;
import io.debezium.connector.postgresql.PostgresConnector;
import io.debezium.testing.testcontainers.testhelper.RestExtensionTestInfrastructure;

@Ignore
public class DebeziumPostgresConnectorResourceNoDatabaseIT {

    @BeforeClass
    public static void checkCondition() {
        Assume.assumeThat("Skipping DebeziumPostgresConnectorResourceIT tests when assembly profile is not active!",
                System.getProperty("isAssemblyProfileActive", "false"),
                is("true"));
    }

    @Before
    public void start() {
        RestExtensionTestInfrastructure.setupDebeziumContainer(Module.version(), DebeziumPostgresConnectRestExtension.class.getName());
        RestExtensionTestInfrastructure.startContainers(DATABASE.NONE);
    }

    @After
    public void stop() {
        RestExtensionTestInfrastructure.stopContainers();
    }

    @Test
    public void testVersionEndpoint() {
        given()
                .port(RestExtensionTestInfrastructure.getDebeziumContainer().getFirstMappedPort())
                .when()
                .get(DebeziumPostgresConnectorResource.BASE_PATH + DebeziumPostgresConnectorResource.VERSION_ENDPOINT)
                .then().log().all()
                .statusCode(200)
                .body(is(Module.version()));
    }

    @Test
    public void testSchemaEndpoint() {
        given()
                .port(RestExtensionTestInfrastructure.getDebeziumContainer().getFirstMappedPort())
                .when()
                .get(DebeziumPostgresConnectorResource.BASE_PATH + DebeziumPostgresConnectorResource.SCHEMA_ENDPOINT)
                .then().log().all()
                .statusCode(200)
                .body("components.schemas.size()", is(1))
                .rootPath("components.schemas.values()[0]")
                .body("title", is("Debezium PostgreSQL Connector"))
                .body("properties.size()", is(81))
                .body("x-connector-id", is("postgres"))
                .body("x-version", is(Module.version()))
                .body("x-className", is(PostgresConnector.class.getName()))
                .body("properties", hasKey("topic.prefix"))
                .body("properties", hasKey("plugin.name"))
                .body("properties", hasKey("slot.name"))
                .body("properties", hasKey("snapshot.mode"));
    }
}
