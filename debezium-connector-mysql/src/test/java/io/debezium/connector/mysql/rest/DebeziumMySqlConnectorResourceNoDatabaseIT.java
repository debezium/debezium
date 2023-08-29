/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.rest;

import static io.debezium.testing.testcontainers.testhelper.RestExtensionTestInfrastructure.DATABASE;
import static io.restassured.RestAssured.given;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.hasKey;

import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import io.debezium.connector.mysql.Module;
import io.debezium.connector.mysql.MySqlConnector;
import io.debezium.testing.testcontainers.testhelper.RestExtensionTestInfrastructure;

public class DebeziumMySqlConnectorResourceNoDatabaseIT {

    @BeforeClass
    public static void checkCondition() {
        Assume.assumeThat("Skipping DebeziumMySqlConnectorResourceIT tests when assembly profile is not active!", System.getProperty("isAssemblyProfileActive", "false"),
                is("true"));
    }

    @Before
    public void start() {
        RestExtensionTestInfrastructure.setupDebeziumContainer(Module.version(), DebeziumMySqlConnectRestExtension.class.getName());
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
                .get(DebeziumMySqlConnectorResource.BASE_PATH + DebeziumMySqlConnectorResource.VERSION_ENDPOINT)
                .then().log().all()
                .statusCode(200)
                .body(is(Module.version()));
    }

    @Test
    public void testSchemaEndpoint() {
        given()
                .port(RestExtensionTestInfrastructure.getDebeziumContainer().getFirstMappedPort())
                .when()
                .get(DebeziumMySqlConnectorResource.BASE_PATH + DebeziumMySqlConnectorResource.SCHEMA_ENDPOINT)
                .then().log().all()
                .statusCode(200)
                .body("components.schemas.size()", is(1))
                .rootPath("components.schemas.values()[0]")
                .body("title", is("Debezium MySQL Connector"))
                .body("properties.size()", is(82))
                .body("x-connector-id", is("mysql"))
                .body("x-version", is(Module.version()))
                .body("x-className", is(MySqlConnector.class.getName()))
                .body("properties", hasKey("topic.prefix"))
                .body("properties", hasKey("database.server.id"))
                .body("properties", hasKey("snapshot.mode"));
    }
}
