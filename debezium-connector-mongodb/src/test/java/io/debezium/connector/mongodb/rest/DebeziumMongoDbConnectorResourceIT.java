/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.rest;

import static io.debezium.testing.testcontainers.testhelper.RestExtensionTestInfrastructure.DATABASE;
import static io.restassured.RestAssured.given;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.startsWith;

import java.util.Locale;
import java.util.Map;

import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testcontainers.utility.MountableFile;

import io.debezium.connector.mongodb.Module;
import io.debezium.connector.mongodb.MongoDbConnector;
import io.debezium.connector.mongodb.MongoDbConnectorConfig;
import io.debezium.testing.testcontainers.Connector;
import io.debezium.testing.testcontainers.ConnectorConfiguration;
import io.debezium.testing.testcontainers.testhelper.RestExtensionTestInfrastructure;
import io.debezium.testing.testcontainers.util.DockerUtils;
import io.restassured.http.ContentType;

public class DebeziumMongoDbConnectorResourceIT {

    public static final MountableFile INIT_SCRIPT_RESOURCE = MountableFile.forClasspathResource("/initialize-mongo-single.js");
    public static final String INIT_SCRIPT_PATH = "/docker-entrypoint-initdb.d/initialize-mongo-single.js";

    @BeforeClass
    public static void checkCondition() {
        Assume.assumeThat("Skipping DebeziumMongoDbConnectorResourceIT tests when assembly profile is not active!",
                System.getProperty("isAssemblyProfileActive", "false"),
                is("true"));
    }

    @Before
    public void start() {
        RestExtensionTestInfrastructure.setupDebeziumContainer(Module.version(), DebeziumMongoDbConnectRestExtension.class.getName());
        DockerUtils.enableFakeDnsIfRequired();
        RestExtensionTestInfrastructure.startContainers(DATABASE.MONGODB);
        RestExtensionTestInfrastructure.getMongoDbContainer().execMongoScript(INIT_SCRIPT_RESOURCE, INIT_SCRIPT_PATH);
    }

    @After
    public void stop() {
        RestExtensionTestInfrastructure.stopContainers();
        DockerUtils.disableFakeDns();
    }

    @Test
    public void testValidConnection() {
        ConnectorConfiguration config = getMongoDbConnectorConfiguration(1);

        given()
                .port(RestExtensionTestInfrastructure.getDebeziumContainer().getFirstMappedPort())
                .when().contentType(ContentType.JSON).accept(ContentType.JSON).body(config.toJson())
                .put(DebeziumMongoDbConnectorResource.BASE_PATH + DebeziumMongoDbConnectorResource.VALIDATE_CONNECTION_ENDPOINT)
                .then().log().all()
                .statusCode(200)
                .assertThat().body("status", equalTo("VALID"))
                .body("validationResults.size()", is(0));
    }

    @Test
    public void testInvalidIpConnection() {
        ConnectorConfiguration config = getMongoDbConnectorConfiguration(1)
                .with(MongoDbConnectorConfig.CONNECTION_STRING.name(), "mongodb://192.168.222.222:27017/?replicaSet=zz666");

        Locale.setDefault(new Locale("en", "US")); // to enforce errormessages in English
        given()
                .port(RestExtensionTestInfrastructure.getDebeziumContainer().getFirstMappedPort())
                .when().contentType(ContentType.JSON).accept(ContentType.JSON).body(config.toJson())
                .put(DebeziumMongoDbConnectorResource.BASE_PATH + DebeziumMongoDbConnectorResource.VALIDATE_CONNECTION_ENDPOINT)
                .then().log().all()
                .statusCode(200)
                .assertThat().body("status", equalTo("INVALID"))
                .body("validationResults.size()", is(1))
                .rootPath("validationResults[0]")
                .body("property", equalTo(MongoDbConnectorConfig.CONNECTION_STRING.name()))
                .body("message", startsWith("Unable to connect: "));
    }

    @Test
    public void testInvalidConnection() {
        given()
                .port(RestExtensionTestInfrastructure.getDebeziumContainer().getFirstMappedPort())
                .when().contentType(ContentType.JSON).accept(ContentType.JSON).body("{\"connector.class\": \"" + MongoDbConnector.class.getName() + "\"}")
                .put(DebeziumMongoDbConnectorResource.BASE_PATH + DebeziumMongoDbConnectorResource.VALIDATE_CONNECTION_ENDPOINT)
                .then().log().all()
                .statusCode(200)
                .assertThat().body("status", equalTo("INVALID"))
                .body("validationResults.size()", is(2))
                .body("validationResults", hasItems(
                        Map.of("property", MongoDbConnectorConfig.CONNECTION_STRING.name(), "message",
                                "The 'mongodb.connection.string' value is invalid: Missing connection string"),
                        Map.of("property", MongoDbConnectorConfig.TOPIC_PREFIX.name(), "message", "The 'topic.prefix' value is invalid: A value is required")));
    }

    @Test
    public void testFiltersWithEmptyFilters() {
        ConnectorConfiguration config = getMongoDbConnectorConfiguration(1);

        given()
                .port(RestExtensionTestInfrastructure.getDebeziumContainer().getFirstMappedPort())
                .when().contentType(ContentType.JSON).accept(ContentType.JSON).body(config.toJson())
                .put(DebeziumMongoDbConnectorResource.BASE_PATH + DebeziumMongoDbConnectorResource.VALIDATE_FILTERS_ENDPOINT)
                .then().log().all()
                .statusCode(200)
                .assertThat().body("status", equalTo("VALID"))
                .body("validationResults.size()", is(0))
                .body("matchingCollections.size()", is(3))
                .body("matchingCollections",
                        hasItems(
                                Map.of("namespace", "inventory", "name", "customers", "identifier", "inventory.customers"),
                                Map.of("namespace", "inventory", "name", "orders", "identifier", "inventory.orders"),
                                Map.of("namespace", "inventory", "name", "products", "identifier", "inventory.products")));
    }

    @Test
    public void testFiltersWithValidCollectionIncludeList() {
        ConnectorConfiguration config = getMongoDbConnectorConfiguration(1)
                .with(MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST.name(), "inventory\\.product.*");

        given()
                .port(RestExtensionTestInfrastructure.getDebeziumContainer().getFirstMappedPort())
                .when().contentType(ContentType.JSON).accept(ContentType.JSON).body(config.toJson())
                .put(DebeziumMongoDbConnectorResource.BASE_PATH + DebeziumMongoDbConnectorResource.VALIDATE_FILTERS_ENDPOINT)
                .then().log().all()
                .statusCode(200)
                .assertThat().body("status", equalTo("VALID"))
                .body("validationResults.size()", is(0))
                .body("matchingCollections.size()", is(1))
                .body("matchingCollections",
                        hasItems(Map.of("namespace", "inventory", "name", "products", "identifier", "inventory.products")));
    }

    @Test
    public void testFiltersWithValidDatabaseIncludeList() {
        ConnectorConfiguration config = getMongoDbConnectorConfiguration(1)
                .with(MongoDbConnectorConfig.DATABASE_INCLUDE_LIST.name(), "inventory");

        given()
                .port(RestExtensionTestInfrastructure.getDebeziumContainer().getFirstMappedPort())
                .when().contentType(ContentType.JSON).accept(ContentType.JSON).body(config.toJson())
                .put(DebeziumMongoDbConnectorResource.BASE_PATH + DebeziumMongoDbConnectorResource.VALIDATE_FILTERS_ENDPOINT)
                .then().log().all()
                .statusCode(200)
                .assertThat().body("status", equalTo("VALID"))
                .body("validationResults.size()", is(0))
                .body("matchingCollections.size()", is(3))
                .body("matchingCollections",
                        hasItems(
                                Map.of("namespace", "inventory", "name", "customers", "identifier", "inventory.customers"),
                                Map.of("namespace", "inventory", "name", "orders", "identifier", "inventory.orders"),
                                Map.of("namespace", "inventory", "name", "products", "identifier", "inventory.products")));
    }

    @Test
    public void testFiltersWithInvalidDatabaseIncludeListPattern() {
        ConnectorConfiguration config = getMongoDbConnectorConfiguration(1)
                .with(MongoDbConnectorConfig.DATABASE_INCLUDE_LIST.name(), "+");

        given()
                .port(RestExtensionTestInfrastructure.getDebeziumContainer().getFirstMappedPort())
                .when().contentType(ContentType.JSON).accept(ContentType.JSON).body(config.toJson())
                .put(DebeziumMongoDbConnectorResource.BASE_PATH + DebeziumMongoDbConnectorResource.VALIDATE_FILTERS_ENDPOINT)
                .then().log().all()
                .statusCode(200)
                .assertThat().body("status", equalTo("INVALID"))
                .body("matchingCollections.size()", is(0))
                .body("validationResults.size()", is(1))
                .rootPath("validationResults[0]")
                .body("property", equalTo(MongoDbConnectorConfig.DATABASE_INCLUDE_LIST.name()))
                .body("message", equalTo(
                        "The 'database.include.list' value is invalid: A comma-separated list of valid regular expressions is expected, but Dangling meta character '+' near index 0\n+\n^"));
    }

    @Test
    public void testFiltersWithInvalidDatabaseExcludeListPattern() {
        ConnectorConfiguration config = getMongoDbConnectorConfiguration(1)
                .with(MongoDbConnectorConfig.DATABASE_EXCLUDE_LIST.name(), "+");

        given()
                .port(RestExtensionTestInfrastructure.getDebeziumContainer().getFirstMappedPort())
                .when().contentType(ContentType.JSON).accept(ContentType.JSON).body(config.toJson())
                .put(DebeziumMongoDbConnectorResource.BASE_PATH + DebeziumMongoDbConnectorResource.VALIDATE_FILTERS_ENDPOINT)
                .then().log().all()
                .statusCode(200)
                .assertThat().body("status", equalTo("INVALID"))
                .body("matchingCollections.size()", is(0))
                .body("validationResults.size()", is(1))
                .rootPath("validationResults[0]")
                .body("property", equalTo(MongoDbConnectorConfig.DATABASE_EXCLUDE_LIST.name()))
                .body("message", equalTo(
                        "The 'database.exclude.list' value is invalid: A comma-separated list of valid regular expressions is expected, but Dangling meta character '+' near index 0\n+\n^"));
    }

    @Test
    public void testMetricsEndpoint() throws InterruptedException {
        ConnectorConfiguration config = getMongoDbConnectorConfiguration(1);

        var connectorName = "my-mongodb-connector";
        RestExtensionTestInfrastructure.getDebeziumContainer().registerConnector(
                connectorName,
                config);

        RestExtensionTestInfrastructure.getDebeziumContainer().ensureConnectorState(connectorName, Connector.State.RUNNING);
        RestExtensionTestInfrastructure.waitForConnectorTaskStatus(connectorName, 0, Connector.State.RUNNING);
        RestExtensionTestInfrastructure.getDebeziumContainer().waitForStreamingRunning("mongodb", config.asProperties().getProperty("topic.prefix"), "streaming",
                String.valueOf(0));

        given()
                .port(RestExtensionTestInfrastructure.getDebeziumContainer().getFirstMappedPort())
                .when().contentType(ContentType.JSON).accept(ContentType.JSON).body(config.toJson())
                .get(DebeziumMongoDbConnectorResource.BASE_PATH + DebeziumMongoDbConnectorResource.CONNECTOR_METRICS_ENDPOINT, connectorName)
                .then().log().all()
                .statusCode(200)
                .body("name", equalTo(connectorName))
                .body("connector.metrics.Connected", equalTo("true"))
                .body("tasks[0].id", equalTo(0))
                .body("tasks[0].namespaces[0].metrics.MilliSecondsSinceLastEvent", equalTo("-1"))
                .body("tasks[0].namespaces[0].metrics.TotalNumberOfEventsSeen", equalTo("0"));

    }

    public static ConnectorConfiguration getMongoDbConnectorConfiguration(int id, String... options) {
        final ConnectorConfiguration config = ConnectorConfiguration.forMongoDbReplicaSet(RestExtensionTestInfrastructure.getMongoDbContainer())
                .with(MongoDbConnectorConfig.SERVER_SELECTION_TIMEOUT_MS.name(), 10000)
                .with(MongoDbConnectorConfig.SNAPSHOT_MODE.name(), "never") // temporarily disable snapshot mode globally until we can check if connectors inside testcontainers are in SNAPSHOT or STREAMING mode (wait for snapshot finished!)
                .with(MongoDbConnectorConfig.USER.name(), "debezium")
                .with(MongoDbConnectorConfig.PASSWORD.name(), "dbz")
                .with(MongoDbConnectorConfig.TOPIC_PREFIX.name(), "mongo" + id);

        if (options != null && options.length > 0) {
            for (int i = 0; i < options.length; i += 2) {
                config.with(options[i], options[i + 1]);
            }
        }
        return config;
    }

}
