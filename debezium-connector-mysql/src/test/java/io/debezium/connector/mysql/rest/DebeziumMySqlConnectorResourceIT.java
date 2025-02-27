/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.rest;

import static io.debezium.testing.testcontainers.testhelper.TestInfrastructureHelper.DATABASE;
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

import io.debezium.connector.mysql.Module;
import io.debezium.connector.mysql.MySqlConnector;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.storage.kafka.history.KafkaSchemaHistory;
import io.debezium.testing.testcontainers.Connector;
import io.debezium.testing.testcontainers.ConnectorConfiguration;
import io.debezium.testing.testcontainers.testhelper.TestInfrastructureHelper;
import io.restassured.http.ContentType;

public class DebeziumMySqlConnectorResourceIT {

    @BeforeClass
    public static void checkCondition() {
        Assume.assumeThat("Skipping DebeziumMySqlConnectorResourceIT tests when assembly profile is not active!", System.getProperty("isAssemblyProfileActive", "false"),
                is("true"));
    }

    @Before
    public void start() {
        TestInfrastructureHelper.setupDebeziumContainer(Module.version(), DebeziumMySqlConnectRestExtension.class.getName());
        TestInfrastructureHelper.startContainers(DATABASE.MYSQL);
    }

    @After
    public void stop() {
        TestInfrastructureHelper.stopContainers();
    }

    @Test
    public void testValidConnection() {
        ConnectorConfiguration config = getMySqlConnectorConfiguration(1);

        given()
                .port(TestInfrastructureHelper.getDebeziumContainer().getFirstMappedPort())
                .when().contentType(ContentType.JSON).accept(ContentType.JSON).body(config.toJson())
                .put(DebeziumMySqlConnectorResource.BASE_PATH + DebeziumMySqlConnectorResource.VALIDATE_CONNECTION_ENDPOINT)
                .then().log().all()
                .statusCode(200)
                .assertThat().body("status", equalTo("VALID"))
                .body("validationResults.size()", is(0));
    }

    @Test
    public void testInvalidHostnameConnection() {
        ConnectorConfiguration config = getMySqlConnectorConfiguration(1).with(MySqlConnectorConfig.HOSTNAME.name(), "zzzzzzzzzz");

        Locale.setDefault(new Locale("en", "US")); // to enforce errormessages in English
        given()
                .port(TestInfrastructureHelper.getDebeziumContainer().getFirstMappedPort())
                .when().contentType(ContentType.JSON).accept(ContentType.JSON).body(config.toJson())
                .put(DebeziumMySqlConnectorResource.BASE_PATH + DebeziumMySqlConnectorResource.VALIDATE_CONNECTION_ENDPOINT)
                .then().log().all()
                .statusCode(200)
                .assertThat().body("status", equalTo("INVALID"))
                .body("validationResults.size()", is(1))
                .rootPath("validationResults[0]")
                .body("property", equalTo(MySqlConnectorConfig.HOSTNAME.name()))
                .body("message", startsWith("Unable to connect: Communications link failure"));
    }

    @Test
    public void testInvalidConnection() {
        given()
                .port(TestInfrastructureHelper.getDebeziumContainer().getFirstMappedPort())
                .when().contentType(ContentType.JSON).accept(ContentType.JSON).body("{\"connector.class\": \"" + MySqlConnector.class.getName() + "\"}")
                .put(DebeziumMySqlConnectorResource.BASE_PATH + DebeziumMySqlConnectorResource.VALIDATE_CONNECTION_ENDPOINT)
                .then().log().all()
                .statusCode(200)
                .assertThat().body("status", equalTo("INVALID"))
                .body("validationResults.size()", is(4))
                .body("validationResults",
                        hasItems(
                                Map.of("property", MySqlConnectorConfig.USER.name(), "message", "The 'database.user' value is invalid: A value is required"),
                                Map.of("property", MySqlConnectorConfig.TOPIC_PREFIX.name(), "message", "The 'topic.prefix' value is invalid: A value is required"),
                                Map.of("property", MySqlConnectorConfig.SERVER_ID.name(), "message", "The 'database.server.id' value is invalid: A value is required"),
                                Map.of("property", MySqlConnectorConfig.HOSTNAME.name(), "message", "The 'database.hostname' value is invalid: A value is required")));
    }

    @Test
    public void testFiltersWithEmptyFilters() {
        ConnectorConfiguration config = getMySqlConnectorConfiguration(1);

        given()
                .port(TestInfrastructureHelper.getDebeziumContainer().getFirstMappedPort())
                .when().contentType(ContentType.JSON).accept(ContentType.JSON).body(config.toJson())
                .put(DebeziumMySqlConnectorResource.BASE_PATH + DebeziumMySqlConnectorResource.VALIDATE_FILTERS_ENDPOINT)
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
        ConnectorConfiguration config = getMySqlConnectorConfiguration(1)
                .with("table.include.list", "inventory\\.product.*");

        given()
                .port(TestInfrastructureHelper.getDebeziumContainer().getFirstMappedPort())
                .when().contentType(ContentType.JSON).accept(ContentType.JSON).body(config.toJson())
                .put(DebeziumMySqlConnectorResource.BASE_PATH + DebeziumMySqlConnectorResource.VALIDATE_FILTERS_ENDPOINT)
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
        ConnectorConfiguration config = getMySqlConnectorConfiguration(1)
                .with("database.include.list", "inventory");

        given()
                .port(TestInfrastructureHelper.getDebeziumContainer().getFirstMappedPort())
                .when().contentType(ContentType.JSON).accept(ContentType.JSON).body(config.toJson())
                .put(DebeziumMySqlConnectorResource.BASE_PATH + DebeziumMySqlConnectorResource.VALIDATE_FILTERS_ENDPOINT)
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
        ConnectorConfiguration config = getMySqlConnectorConfiguration(1)
                .with("database.include.list", "+");

        given()
                .port(TestInfrastructureHelper.getDebeziumContainer().getFirstMappedPort())
                .when().contentType(ContentType.JSON).accept(ContentType.JSON).body(config.toJson())
                .put(DebeziumMySqlConnectorResource.BASE_PATH + DebeziumMySqlConnectorResource.VALIDATE_FILTERS_ENDPOINT)
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
        ConnectorConfiguration config = getMySqlConnectorConfiguration(1)
                .with("database.exclude.list", "+");

        given()
                .port(TestInfrastructureHelper.getDebeziumContainer().getFirstMappedPort())
                .when().contentType(ContentType.JSON).accept(ContentType.JSON).body(config.toJson())
                .put(DebeziumMySqlConnectorResource.BASE_PATH + DebeziumMySqlConnectorResource.VALIDATE_FILTERS_ENDPOINT)
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
        ConnectorConfiguration config = getMySqlConnectorConfiguration(1);

        var connectorName = "my-mysql-connector";
        TestInfrastructureHelper.getDebeziumContainer().registerConnector(
                connectorName,
                config);

        TestInfrastructureHelper.getDebeziumContainer().ensureConnectorState(connectorName, Connector.State.RUNNING);
        TestInfrastructureHelper.waitForConnectorTaskStatus(connectorName, 0, Connector.State.RUNNING);
        TestInfrastructureHelper.getDebeziumContainer().waitForStreamingRunning("mysql", config.asProperties().getProperty("topic.prefix"));

        given()
                .port(TestInfrastructureHelper.getDebeziumContainer().getFirstMappedPort())
                .when().contentType(ContentType.JSON).accept(ContentType.JSON).body(config.toJson())
                .get(DebeziumMySqlConnectorResource.BASE_PATH + DebeziumMySqlConnectorResource.CONNECTOR_METRICS_ENDPOINT, connectorName)
                .then().log().all()
                .statusCode(200)
                .body("name", equalTo(connectorName))
                .body("connector.metrics.Connected", equalTo("true"))
                .body("tasks[0].id", equalTo(0))
                .body("tasks[0].namespaces[0].metrics.MilliSecondsSinceLastEvent", equalTo("0"))
                .body("tasks[0].namespaces[0].metrics.TotalNumberOfEventsSeen", is(notNullValue()));
    }

    public static ConnectorConfiguration getMySqlConnectorConfiguration(int id, String... options) {
        final ConnectorConfiguration config = ConnectorConfiguration.forJdbcContainer(TestInfrastructureHelper.getMySqlContainer())
                .with(MySqlConnectorConfig.USER.name(), "debezium")
                .with(MySqlConnectorConfig.PASSWORD.name(), "dbz")
                .with(MySqlConnectorConfig.SNAPSHOT_MODE.name(), "never") // temporarily disable snapshot mode globally until we can check if connectors inside testcontainers are in SNAPSHOT or STREAMING mode (wait for snapshot finished!)
                .with(MySqlConnectorConfig.TOPIC_PREFIX.name(), "dbserver" + id)
                .with(KafkaSchemaHistory.BOOTSTRAP_SERVERS.name(), TestInfrastructureHelper.KAFKA_HOSTNAME + ":9092")
                .with(KafkaSchemaHistory.TOPIC.name(), "dbhistory.inventory")
                .with(MySqlConnectorConfig.SERVER_ID.name(), Long.valueOf(5555 + id - 1));

        if (options != null && options.length > 0) {
            for (int i = 0; i < options.length; i += 2) {
                config.with(options[i], options[i + 1]);
            }
        }
        return config;
    }

}
