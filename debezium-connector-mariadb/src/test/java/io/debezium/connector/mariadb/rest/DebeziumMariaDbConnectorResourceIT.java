/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb.rest;

import static io.restassured.RestAssured.given;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.Matchers.notNullValue;

import java.util.Locale;
import java.util.Map;

import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import io.debezium.connector.mariadb.MariaDbConnector;
import io.debezium.connector.mariadb.MariaDbConnectorConfig;
import io.debezium.connector.mariadb.Module;
import io.debezium.storage.kafka.history.KafkaSchemaHistory;
import io.debezium.testing.testcontainers.Connector;
import io.debezium.testing.testcontainers.ConnectorConfiguration;
import io.debezium.testing.testcontainers.testhelper.TestInfrastructureHelper;
import io.restassured.http.ContentType;

/**
 * @author Chris Cranford
 */
public class DebeziumMariaDbConnectorResourceIT {
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
        TestInfrastructureHelper.startContainers(TestInfrastructureHelper.DATABASE.MARIADB);
    }

    @After
    public void stop() {
        TestInfrastructureHelper.stopContainers();
    }

    @Test
    public void testValidConnection() {
        ConnectorConfiguration config = getMariaDbConnectorConfiguration(1);

        given()
                .port(TestInfrastructureHelper.getDebeziumContainer().getFirstMappedPort())
                .when().contentType(ContentType.JSON).accept(ContentType.JSON).body(config.toJson())
                .put(DebeziumMariaDbConnectorResource.BASE_PATH + DebeziumMariaDbConnectorResource.VALIDATE_CONNECTION_ENDPOINT)
                .then().log().all()
                .statusCode(200)
                .assertThat().body("status", equalTo("VALID"))
                .body("validationResults.size()", is(0));
    }

    @Test
    public void testInvalidHostnameConnection() {
        ConnectorConfiguration config = getMariaDbConnectorConfiguration(1).with(MariaDbConnectorConfig.HOSTNAME.name(), "zzzzzzzzzz");

        Locale.setDefault(new Locale("en", "US")); // to enforce errormessages in English
        given()
                .port(TestInfrastructureHelper.getDebeziumContainer().getFirstMappedPort())
                .when().contentType(ContentType.JSON).accept(ContentType.JSON).body(config.toJson())
                .put(DebeziumMariaDbConnectorResource.BASE_PATH + DebeziumMariaDbConnectorResource.VALIDATE_CONNECTION_ENDPOINT)
                .then().log().all()
                .statusCode(200)
                .assertThat().body("status", equalTo("INVALID"))
                .body("validationResults.size()", is(1))
                .rootPath("validationResults[0]")
                .body("property", equalTo(MariaDbConnectorConfig.HOSTNAME.name()))
                .body("message", startsWith("Unable to connect: Socket fail to connect"));
    }

    @Test
    public void testInvalidConnection() {
        given()
                .port(TestInfrastructureHelper.getDebeziumContainer().getFirstMappedPort())
                .when().contentType(ContentType.JSON).accept(ContentType.JSON).body("{\"connector.class\": \"" + MariaDbConnector.class.getName() + "\"}")
                .put(DebeziumMariaDbConnectorResource.BASE_PATH + DebeziumMariaDbConnectorResource.VALIDATE_CONNECTION_ENDPOINT)
                .then().log().all()
                .statusCode(200)
                .assertThat().body("status", equalTo("INVALID"))
                .body("validationResults.size()", is(4))
                .body("validationResults",
                        hasItems(
                                Map.of("property", MariaDbConnectorConfig.USER.name(), "message", "The 'database.user' value is invalid: A value is required"),
                                Map.of("property", MariaDbConnectorConfig.TOPIC_PREFIX.name(), "message", "The 'topic.prefix' value is invalid: A value is required"),
                                Map.of("property", MariaDbConnectorConfig.SERVER_ID.name(), "message", "The 'database.server.id' value is invalid: A value is required"),
                                Map.of("property", MariaDbConnectorConfig.HOSTNAME.name(), "message", "The 'database.hostname' value is invalid: A value is required")));
    }

    @Test
    public void testFiltersWithEmptyFilters() {
        ConnectorConfiguration config = getMariaDbConnectorConfiguration(1);

        given()
                .port(TestInfrastructureHelper.getDebeziumContainer().getFirstMappedPort())
                .when().contentType(ContentType.JSON).accept(ContentType.JSON).body(config.toJson())
                .put(DebeziumMariaDbConnectorResource.BASE_PATH + DebeziumMariaDbConnectorResource.VALIDATE_FILTERS_ENDPOINT)
                .then().log().all()
                .statusCode(200)
                .assertThat().body("status", equalTo("VALID"))
                .body("validationResults.size()", is(0))
                .body("matchingCollections.size()", is(6))
                .body("matchingCollections",
                        hasItems(
                                Map.of("namespace", "inventory", "name", "geom", "identifier", "inventory.geom"),
                                Map.of("namespace", "inventory", "name", "products_on_hand", "identifier", "inventory.products_on_hand"),
                                Map.of("namespace", "inventory", "name", "customers", "identifier", "inventory.customers"),
                                Map.of("namespace", "inventory", "name", "addresses", "identifier", "inventory.addresses"),
                                Map.of("namespace", "inventory", "name", "orders", "identifier", "inventory.orders"),
                                Map.of("namespace", "inventory", "name", "products", "identifier", "inventory.products")));
    }

    @Test
    public void testFiltersWithValidTableIncludeList() {
        ConnectorConfiguration config = getMariaDbConnectorConfiguration(1)
                .with("table.include.list", "inventory\\.product.*");

        given()
                .port(TestInfrastructureHelper.getDebeziumContainer().getFirstMappedPort())
                .when().contentType(ContentType.JSON).accept(ContentType.JSON).body(config.toJson())
                .put(DebeziumMariaDbConnectorResource.BASE_PATH + DebeziumMariaDbConnectorResource.VALIDATE_FILTERS_ENDPOINT)
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
    public void testFiltersWithValidDatabaseIncludeList() {
        ConnectorConfiguration config = getMariaDbConnectorConfiguration(1)
                .with("database.include.list", "inventory");

        given()
                .port(TestInfrastructureHelper.getDebeziumContainer().getFirstMappedPort())
                .when().contentType(ContentType.JSON).accept(ContentType.JSON).body(config.toJson())
                .put(DebeziumMariaDbConnectorResource.BASE_PATH + DebeziumMariaDbConnectorResource.VALIDATE_FILTERS_ENDPOINT)
                .then().log().all()
                .statusCode(200)
                .assertThat().body("status", equalTo("VALID"))
                .body("validationResults.size()", is(0))
                .body("matchingCollections.size()", is(6))
                .body("matchingCollections",
                        hasItems(
                                Map.of("namespace", "inventory", "name", "geom", "identifier", "inventory.geom"),
                                Map.of("namespace", "inventory", "name", "products_on_hand", "identifier", "inventory.products_on_hand"),
                                Map.of("namespace", "inventory", "name", "customers", "identifier", "inventory.customers"),
                                Map.of("namespace", "inventory", "name", "addresses", "identifier", "inventory.addresses"),
                                Map.of("namespace", "inventory", "name", "orders", "identifier", "inventory.orders"),
                                Map.of("namespace", "inventory", "name", "products", "identifier", "inventory.products")));
    }

    @Test
    public void testFiltersWithInvalidDatabaseIncludeListPattern() {
        ConnectorConfiguration config = getMariaDbConnectorConfiguration(1)
                .with("database.include.list", "+");

        given()
                .port(TestInfrastructureHelper.getDebeziumContainer().getFirstMappedPort())
                .when().contentType(ContentType.JSON).accept(ContentType.JSON).body(config.toJson())
                .put(DebeziumMariaDbConnectorResource.BASE_PATH + DebeziumMariaDbConnectorResource.VALIDATE_FILTERS_ENDPOINT)
                .then().log().all()
                .statusCode(200)
                .assertThat().body("status", equalTo("INVALID"))
                .body("matchingCollections.size()", is(0))
                .body("validationResults.size()", is(1))
                .rootPath("validationResults[0]")
                .body("property", equalTo("database.include.list"))
                .body("message", equalTo(
                        "The 'database.include.list' value is invalid: A comma-separated list of valid regular expressions is expected, but Dangling meta character '+' near index 0\n+\n^"));
    }

    @Test
    public void testFiltersWithInvalidDatabaseExcludeListPattern() {
        ConnectorConfiguration config = getMariaDbConnectorConfiguration(1)
                .with("database.exclude.list", "+");

        given()
                .port(TestInfrastructureHelper.getDebeziumContainer().getFirstMappedPort())
                .when().contentType(ContentType.JSON).accept(ContentType.JSON).body(config.toJson())
                .put(DebeziumMariaDbConnectorResource.BASE_PATH + DebeziumMariaDbConnectorResource.VALIDATE_FILTERS_ENDPOINT)
                .then().log().all()
                .statusCode(200)
                .assertThat().body("status", equalTo("INVALID"))
                .body("matchingCollections.size()", is(0))
                .body("validationResults.size()", is(1))
                .rootPath("validationResults[0]")
                .body("property", equalTo("database.exclude.list"))
                .body("message", equalTo(
                        "The 'database.exclude.list' value is invalid: A comma-separated list of valid regular expressions is expected, but Dangling meta character '+' near index 0\n+\n^"));
    }

    @Test
    public void testMetricsEndpoint() throws InterruptedException {
        ConnectorConfiguration config = getMariaDbConnectorConfiguration(1);

        var connectorName = "my-mariadb-connector";
        TestInfrastructureHelper.getDebeziumContainer().registerConnector(
                connectorName,
                config);

        TestInfrastructureHelper.getDebeziumContainer().ensureConnectorState(connectorName, Connector.State.RUNNING);
        TestInfrastructureHelper.waitForConnectorTaskStatus(connectorName, 0, Connector.State.RUNNING);
        TestInfrastructureHelper.getDebeziumContainer().waitForStreamingRunning("mariadb", config.asProperties().getProperty("topic.prefix"));

        given()
                .port(TestInfrastructureHelper.getDebeziumContainer().getFirstMappedPort())
                .when().contentType(ContentType.JSON).accept(ContentType.JSON).body(config.toJson())
                .get(DebeziumMariaDbConnectorResource.BASE_PATH + DebeziumMariaDbConnectorResource.CONNECTOR_METRICS_ENDPOINT, connectorName)
                .then().log().all()
                .statusCode(200)
                .body("name", equalTo(connectorName))
                .body("connector.metrics.Connected", equalTo("true"))
                .body("tasks[0].id", equalTo(0))
                .body("tasks[0].namespaces[0].metrics.MilliSecondsSinceLastEvent", equalTo("0"))
                .body("tasks[0].namespaces[0].metrics.TotalNumberOfEventsSeen", is(notNullValue()));
    }

    public static ConnectorConfiguration getMariaDbConnectorConfiguration(int id, String... options) {
        final ConnectorConfiguration config = ConnectorConfiguration.forJdbcContainer(TestInfrastructureHelper.getMariaDbContainer())
                .with(MariaDbConnectorConfig.USER.name(), "debezium")
                .with(MariaDbConnectorConfig.PASSWORD.name(), "dbz")
                .with(MariaDbConnectorConfig.SNAPSHOT_MODE.name(), "never") // temporarily disable snapshot mode globally until we can check if connectors inside testcontainers are in SNAPSHOT or STREAMING mode (wait for snapshot finished!)
                .with(MariaDbConnectorConfig.TOPIC_PREFIX.name(), "dbserver" + id)
                .with(KafkaSchemaHistory.BOOTSTRAP_SERVERS.name(), TestInfrastructureHelper.KAFKA_HOSTNAME + ":9092")
                .with(KafkaSchemaHistory.TOPIC.name(), "dbhistory.inventory")
                .with(MariaDbConnectorConfig.SERVER_ID.name(), Long.valueOf(5555 + id - 1))
                // basic container does not support SSL out of the box
                .with(MariaDbConnectorConfig.SSL_MODE.name(), "disable");

        if (options != null && options.length > 0) {
            for (int i = 0; i < options.length; i += 2) {
                config.with(options[i], options[i + 1]);
            }
        }
        return config;
    }
}
