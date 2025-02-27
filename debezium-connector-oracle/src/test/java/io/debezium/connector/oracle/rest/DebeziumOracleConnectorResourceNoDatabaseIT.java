/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.rest;

import static io.debezium.testing.testcontainers.testhelper.TestInfrastructureHelper.DATABASE;
import static io.restassured.RestAssured.given;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.hasKey;

import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import io.debezium.connector.oracle.Module;
import io.debezium.connector.oracle.OracleConnector;
import io.debezium.testing.testcontainers.testhelper.TestInfrastructureHelper;

public class DebeziumOracleConnectorResourceNoDatabaseIT {

    @BeforeClass
    public static void checkCondition() {
        Assume.assumeThat("Skipping DebeziumOracleConnectorResourceIT tests when assembly profile is not active!", System.getProperty("isAssemblyProfileActive", "false"),
                is("true"));
    }

    @Before
    public void start() {
        TestInfrastructureHelper.setupDebeziumContainer(Module.version(), DebeziumOracleConnectRestExtension.class.getName());
        TestInfrastructureHelper.startContainers(DATABASE.NONE);
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
                .get(DebeziumOracleConnectorResource.BASE_PATH + DebeziumOracleConnectorResource.VERSION_ENDPOINT)
                .then().log().all()
                .statusCode(200)
                .body(is(Module.version()));
    }

    @Test
    public void testSchemaEndpoint() {
        given()
                .port(TestInfrastructureHelper.getDebeziumContainer().getFirstMappedPort())
                .when()
                .get(DebeziumOracleConnectorResource.BASE_PATH + DebeziumOracleConnectorResource.SCHEMA_ENDPOINT)
                .then().log().all()
                .statusCode(200)
                .body("components.schemas.size()", is(1))
                .rootPath("components.schemas.values()[0]")
                .body("title", is("Debezium Oracle Connector"))
                .body("properties.isEmpty()", is(false))
                .body("x-connector-id", is("oracle"))
                .body("x-version", is(Module.version()))
                .body("x-className", is(OracleConnector.class.getName()))
                .body("properties", hasKey("topic.prefix"))
                .body("properties", hasKey("database.pdb.name"))
                .body("properties", hasKey("database.out.server.name"))
                .body("properties", hasKey("rac.nodes"))
                .body("properties", hasKey("database.connection.adapter"))
                .body("properties", hasKey("log.mining.strategy"))
                .body("properties", hasKey("lob.enabled"))
                .body("properties", hasKey("snapshot.mode"));
    }
}
