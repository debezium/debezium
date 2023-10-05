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

import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import io.debezium.connector.oracle.Module;
import io.debezium.connector.oracle.OracleConnector;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.storage.kafka.history.KafkaSchemaHistory;
import io.debezium.testing.testcontainers.ConnectorConfiguration;
import io.debezium.testing.testcontainers.testhelper.RestExtensionTestInfrastructure;
import io.restassured.http.ContentType;

public class DebeziumOracleConnectorResourceIT {

    @BeforeClass
    public static void checkCondition() {
        Assume.assumeThat("Skipping DebeziumOracleConnectorResourceIT tests when assembly profile is not active!", System.getProperty("isAssemblyProfileActive", "false"),
                is("true"));
    }

    @Before
    public void start() {
        RestExtensionTestInfrastructure.setupDebeziumContainer(Module.version(), DebeziumOracleConnectRestExtension.class.getName());
        RestExtensionTestInfrastructure.startContainers(DATABASE.ORACLE);
    }

    @After
    public void stop() {
        RestExtensionTestInfrastructure.stopContainers();
    }

    @Test
    public void testValidConnection() {
        ConnectorConfiguration config = getOracleConnectorConfiguration(1);

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
        ConnectorConfiguration config = getOracleConnectorConfiguration(1).with(OracleConnectorConfig.HOSTNAME.name(), "zzzzzzzzzz");

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

    public static ConnectorConfiguration getOracleConnectorConfiguration(int id, String... options) {
        final ConnectorConfiguration config = ConnectorConfiguration.forJdbcContainer(RestExtensionTestInfrastructure.getOracleContainer())
                .with(OracleConnectorConfig.PDB_NAME.name(), "ORCLPDB1")
                .with(OracleConnectorConfig.TOPIC_PREFIX.name(), "dbserver" + id)
                .with(KafkaSchemaHistory.BOOTSTRAP_SERVERS.name(), RestExtensionTestInfrastructure.KAFKA_HOSTNAME + ":9092")
                .with(KafkaSchemaHistory.TOPIC.name(), "dbhistory.inventory");

        if (options != null && options.length > 0) {
            for (int i = 0; i < options.length; i += 2) {
                config.with(options[i], options[i + 1]);
            }
        }

        return config;
    }

}
