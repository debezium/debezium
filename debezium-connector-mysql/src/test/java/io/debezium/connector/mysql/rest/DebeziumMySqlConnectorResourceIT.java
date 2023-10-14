/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.rest;

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
import org.junit.Ignore;
import org.junit.Test;

import io.debezium.connector.mysql.Module;
import io.debezium.connector.mysql.MySqlConnector;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.storage.kafka.history.KafkaSchemaHistory;
import io.debezium.testing.testcontainers.ConnectorConfiguration;
import io.debezium.testing.testcontainers.testhelper.RestExtensionTestInfrastructure;
import io.restassured.http.ContentType;

@Ignore
public class DebeziumMySqlConnectorResourceIT {

    @BeforeClass
    public static void checkCondition() {
        Assume.assumeThat("Skipping DebeziumMySqlConnectorResourceIT tests when assembly profile is not active!", System.getProperty("isAssemblyProfileActive", "false"),
                is("true"));
    }

    @Before
    public void start() {
        RestExtensionTestInfrastructure.setupDebeziumContainer(Module.version(), DebeziumMySqlConnectRestExtension.class.getName());
        RestExtensionTestInfrastructure.startContainers(DATABASE.MYSQL);
    }

    @After
    public void stop() {
        RestExtensionTestInfrastructure.stopContainers();
    }

    @Test
    public void testValidConnection() {
        ConnectorConfiguration config = getMySqlConnectorConfiguration(1);

        given()
                .port(RestExtensionTestInfrastructure.getDebeziumContainer().getFirstMappedPort())
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
                .port(RestExtensionTestInfrastructure.getDebeziumContainer().getFirstMappedPort())
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
                .port(RestExtensionTestInfrastructure.getDebeziumContainer().getFirstMappedPort())
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

    public static ConnectorConfiguration getMySqlConnectorConfiguration(int id, String... options) {
        final ConnectorConfiguration config = ConnectorConfiguration.forJdbcContainer(RestExtensionTestInfrastructure.getMySqlContainer())
                .with(MySqlConnectorConfig.SNAPSHOT_MODE.name(), "never") // temporarily disable snapshot mode globally until we can check if connectors inside testcontainers are in SNAPSHOT or STREAMING mode (wait for snapshot finished!)
                .with(MySqlConnectorConfig.TOPIC_PREFIX.name(), "dbserver" + id)
                .with(KafkaSchemaHistory.BOOTSTRAP_SERVERS.name(), RestExtensionTestInfrastructure.KAFKA_HOSTNAME + ":9092")
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
