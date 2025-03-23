/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.rest;

import static io.debezium.testing.testcontainers.testhelper.TestInfrastructureHelper.DATABASE;
import static io.restassured.RestAssured.given;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;

import java.util.Locale;
import java.util.Map;

import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import io.debezium.connector.postgresql.Module;
import io.debezium.connector.postgresql.PostgresConnector;
import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.testing.testcontainers.Connector;
import io.debezium.testing.testcontainers.ConnectorConfiguration;
import io.debezium.testing.testcontainers.testhelper.TestInfrastructureHelper;
import io.restassured.http.ContentType;

@Ignore
public class DebeziumPostgresConnectorResourceIT {

    @BeforeClass
    public static void checkCondition() {
        Assume.assumeThat("Skipping DebeziumPostgresConnectorResourceIT tests when assembly profile is not active!",
                System.getProperty("isAssemblyProfileActive", "false"),
                is("true"));
    }

    @Before
    public void start() {
        TestInfrastructureHelper.setupDebeziumContainer(Module.version(), DebeziumPostgresConnectRestExtension.class.getName());
        TestInfrastructureHelper.startContainers(DATABASE.POSTGRES);
    }

    @After
    public void stop() {
        TestInfrastructureHelper.stopContainers();
    }

    @Test
    public void testValidConnection() {
        ConnectorConfiguration config = getPostgresConnectorConfiguration(1);

        given()
                .port(TestInfrastructureHelper.getDebeziumContainer().getFirstMappedPort())
                .when().contentType(ContentType.JSON).accept(ContentType.JSON).body(config.toJson())
                .put(DebeziumPostgresConnectorResource.BASE_PATH + DebeziumPostgresConnectorResource.VALIDATE_CONNECTION_ENDPOINT)
                .then().log().all()
                .statusCode(200)
                .assertThat().body("status", equalTo("VALID"))
                .body("validationResults.size()", is(0));
    }

    @Test
    public void testInvalidHostnameConnection() {
        ConnectorConfiguration config = getPostgresConnectorConfiguration(1).with(PostgresConnectorConfig.HOSTNAME.name(), "zzzzzzzzzz");

        Locale.setDefault(new Locale("en", "US")); // to enforce errormessages in English
        given()
                .port(TestInfrastructureHelper.getDebeziumContainer().getFirstMappedPort())
                .when().contentType(ContentType.JSON).accept(ContentType.JSON).body(config.toJson())
                .put(DebeziumPostgresConnectorResource.BASE_PATH + DebeziumPostgresConnectorResource.VALIDATE_CONNECTION_ENDPOINT)
                .then().log().all()
                .statusCode(200)
                .assertThat().body("status", equalTo("INVALID"))
                .body("validationResults.size()", is(1))
                .rootPath("validationResults[0]")
                .body("property", is(PostgresConnectorConfig.HOSTNAME.name()))
                .body("message", is("Error while validating connector config: The connection attempt failed."));
    }

    @Test
    public void testInvalidConnection() {
        given()
                .port(TestInfrastructureHelper.getDebeziumContainer().getFirstMappedPort())
                .when().contentType(ContentType.JSON).accept(ContentType.JSON).body("{\"connector.class\": \"" + PostgresConnector.class.getName() + "\"}")
                .put(DebeziumPostgresConnectorResource.BASE_PATH + DebeziumPostgresConnectorResource.VALIDATE_CONNECTION_ENDPOINT)
                .then().log().all()
                .statusCode(200)
                .assertThat().body("status", equalTo("INVALID"))
                .body("validationResults.size()", is(4))
                .body("validationResults",
                        hasItems(
                                Map.of("property", PostgresConnectorConfig.USER.name(), "message", "The 'database.user' value is invalid: A value is required"),
                                Map.of("property", PostgresConnectorConfig.DATABASE_NAME.name(), "message",
                                        "The 'database.dbname' value is invalid: A value is required"),
                                Map.of("property", PostgresConnectorConfig.TOPIC_PREFIX.name(), "message", "The 'topic.prefix' value is invalid: A value is required"),
                                Map.of("property", PostgresConnectorConfig.HOSTNAME.name(), "message", "The 'database.hostname' value is invalid: A value is required")));
    }

    @Test
    public void testFiltersWithEmptyFilters() {
        ConnectorConfiguration config = getPostgresConnectorConfiguration(1);

        given()
                .port(TestInfrastructureHelper.getDebeziumContainer().getFirstMappedPort())
                .when().contentType(ContentType.JSON).accept(ContentType.JSON).body(config.toJson())
                .put(DebeziumPostgresConnectorResource.BASE_PATH + DebeziumPostgresConnectorResource.VALIDATE_FILTERS_ENDPOINT)
                .then().log().all()
                .statusCode(200)
                .assertThat().body("status", equalTo("VALID"))
                .body("validationResults.size()", is(0))
                .body("matchingCollections.size()", is(5))
                .body("matchingCollections",
                        hasItems(
                                Map.of("namespace", "inventory", "name", "geom", "identifier", "inventory.geom"),
                                Map.of("namespace", "inventory", "name", "products_on_hand", "identifier", "inventory.products_on_hand"),
                                Map.of("namespace", "inventory", "name", "customers", "identifier", "inventory.customers"),
                                Map.of("namespace", "inventory", "name", "orders", "identifier", "inventory.orders"),
                                Map.of("namespace", "inventory", "name", "products", "identifier", "inventory.products")));
    }

    @Test
    public void testFiltersWithValidTableIncludeList() {
        ConnectorConfiguration config = getPostgresConnectorConfiguration(1)
                .with("table.include.list", "inventory\\.product.*");

        given()
                .port(TestInfrastructureHelper.getDebeziumContainer().getFirstMappedPort())
                .when().contentType(ContentType.JSON).accept(ContentType.JSON).body(config.toJson())
                .put(DebeziumPostgresConnectorResource.BASE_PATH + DebeziumPostgresConnectorResource.VALIDATE_FILTERS_ENDPOINT)
                .then().log().all()
                .statusCode(200)
                .assertThat().body("status", equalTo("VALID"))
                .body("validationResults.size()", is(0))
                .body("matchingCollections.size()", is(2))
                .body("matchingCollections",
                        hasItems(
                                Map.of("namespace", "inventory", "name", "products_on_hand", "identifier", "inventory.products_on_hand"),
                                Map.of("namespace", "inventory", "name", "products", "identifier", "inventory.products")));
    }

    @Test
    public void testFiltersWithValidSchemaIncludeList() {
        ConnectorConfiguration config = getPostgresConnectorConfiguration(1)
                .with("schema.include.list", "inventory");

        given()
                .port(TestInfrastructureHelper.getDebeziumContainer().getFirstMappedPort())
                .when().contentType(ContentType.JSON).accept(ContentType.JSON).body(config.toJson())
                .put(DebeziumPostgresConnectorResource.BASE_PATH + DebeziumPostgresConnectorResource.VALIDATE_FILTERS_ENDPOINT)
                .then().log().all()
                .statusCode(200)
                .assertThat().body("status", equalTo("VALID"))
                .body("validationResults.size()", is(0))
                .body("matchingCollections.size()", is(5))
                .body("matchingCollections",
                        hasItems(
                                Map.of("namespace", "inventory", "name", "geom", "identifier", "inventory.geom"),
                                Map.of("namespace", "inventory", "name", "products_on_hand", "identifier", "inventory.products_on_hand"),
                                Map.of("namespace", "inventory", "name", "customers", "identifier", "inventory.customers"),
                                Map.of("namespace", "inventory", "name", "orders", "identifier", "inventory.orders"),
                                Map.of("namespace", "inventory", "name", "products", "identifier", "inventory.products")));
    }

    @Test
    public void testFiltersWithInvalidSchemaIncludeListPattern() {
        ConnectorConfiguration config = getPostgresConnectorConfiguration(1)
                .with("schema.include.list", "+");

        given()
                .port(TestInfrastructureHelper.getDebeziumContainer().getFirstMappedPort())
                .when().contentType(ContentType.JSON).accept(ContentType.JSON).body(config.toJson())
                .put(DebeziumPostgresConnectorResource.BASE_PATH + DebeziumPostgresConnectorResource.VALIDATE_FILTERS_ENDPOINT)
                .then().log().all()
                .statusCode(200)
                .assertThat().body("status", equalTo("INVALID"))
                .body("matchingCollections.size()", is(0))
                .body("validationResults.size()", is(1))
                .rootPath("validationResults[0]")
                .body("property", equalTo("schema.include.list"))
                .body("message", equalTo(
                        "The 'schema.include.list' value is invalid: A comma-separated list of valid regular expressions is expected, but Dangling meta character '+' near index 0\n+\n^"));
    }

    @Test
    public void testFiltersWithInvalidSchemaExcludeListPattern() {
        ConnectorConfiguration config = getPostgresConnectorConfiguration(1)
                .with("schema.exclude.list", "+");

        given()
                .port(TestInfrastructureHelper.getDebeziumContainer().getFirstMappedPort())
                .when().contentType(ContentType.JSON).accept(ContentType.JSON).body(config.toJson())
                .put(DebeziumPostgresConnectorResource.BASE_PATH + DebeziumPostgresConnectorResource.VALIDATE_FILTERS_ENDPOINT)
                .then().log().all()
                .statusCode(200)
                .assertThat().body("status", equalTo("INVALID"))
                .body("matchingCollections.size()", is(0))
                .body("validationResults.size()", is(1))
                .rootPath("validationResults[0]")
                .body("property", equalTo("schema.exclude.list"))
                .body("message", equalTo(
                        "The 'schema.exclude.list' value is invalid: A comma-separated list of valid regular expressions is expected, but Dangling meta character '+' near index 0\n+\n^"));
    }

    @Test
    public void testMetricsEndpoint() throws InterruptedException {
        ConnectorConfiguration config = getPostgresConnectorConfiguration(1);

        var connectorName = "my-postgres-connector";
        TestInfrastructureHelper.getDebeziumContainer().registerConnector(
                connectorName,
                config);

        TestInfrastructureHelper.getDebeziumContainer().ensureConnectorState(connectorName, Connector.State.RUNNING);
        TestInfrastructureHelper.waitForConnectorTaskStatus(connectorName, 0, Connector.State.RUNNING);
        TestInfrastructureHelper.getDebeziumContainer().waitForStreamingRunning("postgres", config.asProperties().getProperty("topic.prefix"));

        given()
                .port(TestInfrastructureHelper.getDebeziumContainer().getFirstMappedPort())
                .when().contentType(ContentType.JSON).accept(ContentType.JSON).body(config.toJson())
                .get(DebeziumPostgresConnectorResource.BASE_PATH + DebeziumPostgresConnectorResource.CONNECTOR_METRICS_ENDPOINT, connectorName)
                .then().log().all()
                .statusCode(200)
                .body("name", equalTo(connectorName))
                .body("connector.metrics.Connected", equalTo("true"))
                .body("tasks[0].id", equalTo(0))
                .body("tasks[0].namespaces[0].metrics.MilliSecondsSinceLastEvent", equalTo("-1"))
                .body("tasks[0].namespaces[0].metrics.TotalNumberOfEventsSeen", equalTo("0"));

    }

    private static ConnectorConfiguration getPostgresConnectorConfiguration(int id, String... options) {
        final ConnectorConfiguration config = ConnectorConfiguration.forJdbcContainer(TestInfrastructureHelper.getPostgresContainer())
                .with(PostgresConnectorConfig.SNAPSHOT_MODE.name(), "never") // temporarily disable snapshot mode globally until we can check if connectors inside testcontainers are in SNAPSHOT or STREAMING mode (wait for snapshot finished!)
                .with(PostgresConnectorConfig.TOPIC_PREFIX.name(), "dbserver" + id)
                .with(PostgresConnectorConfig.SLOT_NAME.name(), "debezium_" + id);

        if (options != null && options.length > 0) {
            for (int i = 0; i < options.length; i += 2) {
                config.with(options[i], options[i + 1]);
            }
        }
        return config;
    }

}
