/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.rest;

import static io.debezium.testing.testcontainers.testhelper.RestExtensionTestInfrastructure.DATABASE;
import static io.restassured.RestAssured.given;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.startsWith;

import java.util.Locale;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import io.debezium.connector.oracle.Module;
import io.debezium.connector.oracle.OracleConnector;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.testing.testcontainers.Connector;
import io.debezium.testing.testcontainers.ConnectorConfiguration;
import io.debezium.testing.testcontainers.testhelper.RestExtensionTestInfrastructure;
import io.restassured.http.ContentType;

public class DebeziumOracleConnectorResourceIT {

    private static final boolean IS_ASSEMBLY_PROFILE_ACTIVE = System.getProperty("isAssemblyProfileActive", "false").equalsIgnoreCase("true");
    private static String ORACLE_USERNAME;
    private static boolean running = false;

    @BeforeClass
    public static void checkConditionAndStart() {
        Assume.assumeTrue("Skipping DebeziumOracleConnectorResourceIT tests when assembly profile is not active!", IS_ASSEMBLY_PROFILE_ACTIVE);
        RestExtensionTestInfrastructure.setupDebeziumContainer(Module.version(), DebeziumOracleConnectRestExtension.class.getName());
        RestExtensionTestInfrastructure.startContainers(DATABASE.ORACLE);
        TestHelper.loadTestData(TestHelper.getOracleConnectorConfiguration(1), "rest/data.sql");
        ORACLE_USERNAME = RestExtensionTestInfrastructure.getOracleContainer().getUsername();
        running = true;
    }

    @AfterClass
    public static void stop() {
        if (IS_ASSEMBLY_PROFILE_ACTIVE && running) {
            RestExtensionTestInfrastructure.stopContainers();
        }
    }

    @Test
    public void testValidConnection() {
        ConnectorConfiguration config = TestHelper.getOracleConnectorConfiguration(1);

        given()
                .port(RestExtensionTestInfrastructure.getDebeziumContainer().getFirstMappedPort())
                .when().contentType(ContentType.JSON).accept(ContentType.JSON).body(config.toJson())
                .put(DebeziumOracleConnectorResource.BASE_PATH + DebeziumOracleConnectorResource.VALIDATE_CONNECTION_ENDPOINT)
                .then().log().all()
                .statusCode(200)
                .assertThat().body("status", equalTo("VALID"))
                .body("validationResults.size()", is(0));
    }

    @Test
    public void testInvalidHostnameConnection() {
        ConnectorConfiguration config = TestHelper.getOracleConnectorConfiguration(1).with(OracleConnectorConfig.HOSTNAME.name(), "zzzzzzzzzz");

        Locale.setDefault(new Locale("en", "US")); // to enforce errormessages in English
        given()
                .port(RestExtensionTestInfrastructure.getDebeziumContainer().getFirstMappedPort())
                .when().contentType(ContentType.JSON).accept(ContentType.JSON).body(config.toJson())
                .put(DebeziumOracleConnectorResource.BASE_PATH + DebeziumOracleConnectorResource.VALIDATE_CONNECTION_ENDPOINT)
                .then().log().all()
                .statusCode(200)
                .assertThat().body("status", equalTo("INVALID"))
                .body("validationResults.size()", is(1))
                .rootPath("validationResults[0]")
                .body("property", equalTo(OracleConnectorConfig.HOSTNAME.name()))
                .body("message", startsWith("Unable to connect: Failed to resolve Oracle database version"));
    }

    @Test
    public void testInvalidConnection() {
        given()
                .port(RestExtensionTestInfrastructure.getDebeziumContainer().getFirstMappedPort())
                .when().contentType(ContentType.JSON).accept(ContentType.JSON).body("{\"connector.class\": \"" + OracleConnector.class.getName() + "\"}")
                .put(DebeziumOracleConnectorResource.BASE_PATH + DebeziumOracleConnectorResource.VALIDATE_CONNECTION_ENDPOINT)
                .then().log().all()
                .statusCode(200)
                .assertThat().body("status", equalTo("INVALID"))
                .body("validationResults.size()", is(5))
                .body("validationResults",
                        hasItems(
                                Map.of("property", OracleConnectorConfig.DATABASE_NAME.name(), "message", "The 'database.dbname' value is invalid: A value is required"),
                                Map.of("property", OracleConnectorConfig.USER.name(), "message", "The 'database.user' value is invalid: A value is required"),
                                Map.of("property", OracleConnectorConfig.TOPIC_PREFIX.name(), "message", "The 'topic.prefix' value is invalid: A value is required"),
                                Map.of("property", OracleConnectorConfig.URL.name(), "message", "The 'database.url' value is invalid: A value is required"),
                                Map.of("property", OracleConnectorConfig.HOSTNAME.name(), "message", "The 'database.hostname' value is invalid: A value is required")));
    }

    @Test
    public void testFiltersWithEmptyFilters() {
        ConnectorConfiguration config = TestHelper.getOracleConnectorConfiguration(1);

        given()
                .port(RestExtensionTestInfrastructure.getDebeziumContainer().getFirstMappedPort())
                .when().contentType(ContentType.JSON).accept(ContentType.JSON).body(config.toJson())
                .put(DebeziumOracleConnectorResource.BASE_PATH + DebeziumOracleConnectorResource.VALIDATE_FILTERS_ENDPOINT)
                .then().log().all()
                .statusCode(200)
                .assertThat().body("status", equalTo("VALID"))
                .body("validationResults.size()", is(0))
                .body("matchingCollections.size()", is(10))
                .body("matchingCollections",
                        hasItems(
                                Map.of("namespace", ORACLE_USERNAME.toUpperCase(), "name", "ORDERS", "identifier", ORACLE_USERNAME.toUpperCase() + ".ORDERS"),
                                Map.of("namespace", ORACLE_USERNAME.toUpperCase(), "name", "PEOPLE", "identifier", ORACLE_USERNAME.toUpperCase() + ".PEOPLE"),
                                Map.of("namespace", ORACLE_USERNAME.toUpperCase(), "name", "TEST", "identifier", ORACLE_USERNAME.toUpperCase() + ".TEST"),
                                Map.of("namespace", ORACLE_USERNAME.toUpperCase(), "name", "PRODUCTS", "identifier", ORACLE_USERNAME.toUpperCase() + ".PRODUCTS"),
                                Map.of("namespace", ORACLE_USERNAME.toUpperCase(), "name", "CUSTOMERS", "identifier", ORACLE_USERNAME.toUpperCase() + ".CUSTOMERS"),
                                Map.of("namespace", ORACLE_USERNAME.toUpperCase(), "name", "DEBEZIUM_TABLE3", "identifier",
                                        ORACLE_USERNAME.toUpperCase() + ".DEBEZIUM_TABLE3"),
                                Map.of("namespace", ORACLE_USERNAME.toUpperCase(), "name", "DEBEZIUM_TABLE2", "identifier",
                                        ORACLE_USERNAME.toUpperCase() + ".DEBEZIUM_TABLE2"),
                                Map.of("namespace", ORACLE_USERNAME.toUpperCase(), "name", "DEBEZIUM_TABLE1", "identifier",
                                        ORACLE_USERNAME.toUpperCase() + ".DEBEZIUM_TABLE1"),
                                Map.of("namespace", ORACLE_USERNAME.toUpperCase(), "name", "PRODUCTS_ON_HAND", "identifier",
                                        ORACLE_USERNAME.toUpperCase() + ".PRODUCTS_ON_HAND"),
                                Map.of("namespace", ORACLE_USERNAME.toUpperCase(), "name", "DEBEZIUM_TEST", "identifier",
                                        ORACLE_USERNAME.toUpperCase() + ".DEBEZIUM_TEST")));
    }

    @Test
    public void testFiltersWithValidTableIncludeList() {
        ConnectorConfiguration config = TestHelper.getOracleConnectorConfiguration(1)
                .with("table.include.list", ORACLE_USERNAME.toUpperCase() + "\\.DEBEZIUM_TABLE.*");

        given()
                .port(RestExtensionTestInfrastructure.getDebeziumContainer().getFirstMappedPort())
                .when().contentType(ContentType.JSON).accept(ContentType.JSON).body(config.toJson())
                .put(DebeziumOracleConnectorResource.BASE_PATH + DebeziumOracleConnectorResource.VALIDATE_FILTERS_ENDPOINT)
                .then().log().all()
                .statusCode(200)
                .assertThat().body("status", equalTo("VALID"))
                .body("validationResults.size()", is(0))
                .body("matchingCollections.size()", is(3))
                .body("matchingCollections",
                        hasItems(
                                Map.of("namespace", ORACLE_USERNAME.toUpperCase(), "name", "DEBEZIUM_TABLE3", "identifier",
                                        ORACLE_USERNAME.toUpperCase() + ".DEBEZIUM_TABLE3"),
                                Map.of("namespace", ORACLE_USERNAME.toUpperCase(), "name", "DEBEZIUM_TABLE1", "identifier",
                                        ORACLE_USERNAME.toUpperCase() + ".DEBEZIUM_TABLE1"),
                                Map.of("namespace", ORACLE_USERNAME.toUpperCase(), "name", "DEBEZIUM_TABLE2", "identifier",
                                        ORACLE_USERNAME.toUpperCase() + ".DEBEZIUM_TABLE2")));
    }

    @Test
    public void testFiltersWithInvalidTableIncludeList() {
        ConnectorConfiguration config = TestHelper.getOracleConnectorConfiguration(1)
                .with("table.include.list", "+");

        given()
                .port(RestExtensionTestInfrastructure.getDebeziumContainer().getFirstMappedPort())
                .when().contentType(ContentType.JSON).accept(ContentType.JSON).body(config.toJson())
                .put(DebeziumOracleConnectorResource.BASE_PATH + DebeziumOracleConnectorResource.VALIDATE_FILTERS_ENDPOINT)
                .then().log().all()
                .statusCode(200)
                .assertThat().body("status", equalTo("INVALID"))
                .body("matchingCollections.size()", is(0))
                .body("validationResults.size()", is(1))
                .rootPath("validationResults[0]")
                .body("property", equalTo("table.include.list"))
                .body("message", equalTo(
                        "The 'table.include.list' value is invalid: A comma-separated list of valid regular expressions is expected, but Dangling meta character '+' near index 0\n+\n^"));
    }

    @Test
    public void testMetricsEndpoint() {
        ConnectorConfiguration config = TestHelper.getOracleConnectorConfiguration(1);

        var connectorName = "my-oracle-connector";
        RestExtensionTestInfrastructure.getDebeziumContainer().registerConnector(
                connectorName,
                config);

        RestExtensionTestInfrastructure.getDebeziumContainer().ensureConnectorState(connectorName, Connector.State.RUNNING);
        RestExtensionTestInfrastructure.waitForConnectorTaskStatus(connectorName, 0, Connector.State.RUNNING);

        given()
                .port(RestExtensionTestInfrastructure.getDebeziumContainer().getFirstMappedPort())
                .when().contentType(ContentType.JSON).accept(ContentType.JSON).body(config.toJson())
                .get(DebeziumOracleConnectorResource.BASE_PATH + DebeziumOracleConnectorResource.CONNECTOR_METRICS_ENDPOINT, connectorName)
                .then().log().all()
                .statusCode(200)
                .body("name", equalTo(connectorName))
                .body("connector.metrics.Connected", equalTo("true"))
                .body("tasks[0].id", equalTo(0))
                .body("tasks[0].namespaces[0].metrics.MilliSecondsSinceLastEvent", equalTo("-1"))
                .body("tasks[0].namespaces[0].metrics.TotalNumberOfEventsSeen", equalTo("0"));
    }

}
