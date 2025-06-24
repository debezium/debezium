/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb.rest;

import static io.restassured.RestAssured.given;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.hasKey;

import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import io.debezium.connector.mariadb.MariaDbConnector;
import io.debezium.connector.mariadb.Module;
import io.debezium.testing.testcontainers.testhelper.TestInfrastructureHelper;

/**
 * @author Chris Cranford
 */
@Ignore
public class DebeziumMariaDbConnectorResourceNoDatabaseIT {
    @BeforeClass
    public static void checkCondition() {
        Assume.assumeThat(
                "Skipping DebeziumMariaDbConnectorResourceIT tests when assembly profile is not active!",
                System.getProperty("isAssemblyProfileActive", "false"),
                is("true"));
    }

    @Before
    public void start() {
        TestInfrastructureHelper.setupDebeziumContainer(Module.version(), DebeziumMariaDbConnectRestExtension.class.getName());
        TestInfrastructureHelper.startContainers(TestInfrastructureHelper.DATABASE.NONE);
    }

    @After
    public void stop() {
        TestInfrastructureHelper.stopContainers();
    }

    @Test
    public void testVersionEndpoint() {
        given()
                .port(TestInfrastructureHelper.getDebeziumContainer().getFirstMappedPort())
                .when()
                .get(DebeziumMariaDbConnectorResource.BASE_PATH + DebeziumMariaDbConnectorResource.VERSION_ENDPOINT)
                .then().log().all()
                .statusCode(200)
                .body(is(Module.version()));
    }

    @Test
    public void testSchemaEndpoint() {
        given()
                .port(TestInfrastructureHelper.getDebeziumContainer().getFirstMappedPort())
                .when()
                .get(DebeziumMariaDbConnectorResource.BASE_PATH + DebeziumMariaDbConnectorResource.SCHEMA_ENDPOINT)
                .then().log().all()
                .statusCode(200)
                .body("components.schemas.size()", is(1))
                .rootPath("components.schemas.values()[0]")
                .body("title", is("Debezium MariaDB Connector"))
                .body("properties.isEmpty()", is(false))
                .body("x-connector-id", is("mariadb"))
                .body("x-version", is(Module.version()))
                .body("x-className", is(MariaDbConnector.class.getName()))
                .body("properties", hasKey("topic.prefix"))
                .body("properties", hasKey("database.server.id"))
                .body("properties", hasKey("snapshot.mode"));
    }
}
