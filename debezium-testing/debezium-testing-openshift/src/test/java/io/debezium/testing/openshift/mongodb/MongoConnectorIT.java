/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.openshift.mongodb;

import static io.debezium.testing.openshift.assertions.KafkaAssertions.awaitAssert;
import static io.debezium.testing.openshift.tools.ConfigProperties.DATABASE_MONGO_DBZ_DBNAME;
import static io.debezium.testing.openshift.tools.ConfigProperties.DATABASE_MONGO_DBZ_LOGIN_DBNAME;
import static io.debezium.testing.openshift.tools.ConfigProperties.DATABASE_MONGO_DBZ_PASSWORD;
import static io.debezium.testing.openshift.tools.ConfigProperties.DATABASE_MONGO_DBZ_USERNAME;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;

import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import io.debezium.testing.openshift.ConnectorTestBase;
import io.debezium.testing.openshift.tools.ConfigProperties;
import io.debezium.testing.openshift.tools.databases.mongodb.MongoController;
import io.debezium.testing.openshift.tools.databases.mongodb.MongoDatabaseClient;
import io.debezium.testing.openshift.tools.databases.mongodb.OcpMongoDeployer;
import io.debezium.testing.openshift.tools.kafka.ConnectorConfigBuilder;

import okhttp3.Request;
import okhttp3.Response;

/**
 * @author Jakub Cechacek
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@Tag("acceptance")
@Tag("mongo")
public class MongoConnectorIT extends ConnectorTestBase {

    public static final String DB_DEPLOYMENT_PATH = "/database-resources/mongodb/deployment.yaml";
    public static final String DB_SERVICE_PATH_LB = "/database-resources/mongodb/service-lb.yaml";
    public static final String DB_SERVICE_PATH = "/database-resources/mongodb/service.yaml";

    public static final String CONNECTOR_NAME = "inventory-connector-mongo";

    private static MongoController dbController;
    private static ConnectorConfigBuilder connectorConfig;
    private static String connectorName;
    private static String dbServerName;

    @BeforeAll
    public static void setupDatabase() throws IOException, InterruptedException {
        if (!ConfigProperties.DATABASE_MYSQL_HOST.isPresent()) {
            OcpMongoDeployer deployer = new OcpMongoDeployer.Deployer()
                    .withOcpClient(ocp)
                    .withProject(ConfigProperties.OCP_PROJECT_MONGO)
                    .withDeployment(DB_DEPLOYMENT_PATH)
                    .withServices(DB_SERVICE_PATH, DB_SERVICE_PATH_LB)
                    .build();
            dbController = deployer.deploy();
            dbController.initialize();
        }

        connectorName = CONNECTOR_NAME + "-" + testUtils.getUniqueId();
        dbServerName = connectorName.replaceAll("-", "_");
        connectorConfig = connectorFactories.mongo(dbServerName);

        if (ConfigProperties.DEPLOY_SERVICE_REGISTRY) {
            connectorConfig.addApicurioAvroSupport(registryController.getRegistryApiAddress());
        }

        kafkaConnectController.deployConnector(connectorName, connectorConfig);
    }

    @AfterAll
    public static void tearDownDatabase() throws IOException, InterruptedException {
        kafkaConnectController.undeployConnector(connectorName);
        dbController.reload();
    }

    private void insertCustomer(String firstName, String lastName, String email) {
        MongoDatabaseClient client = dbController.getDatabaseClient(
                DATABASE_MONGO_DBZ_USERNAME, DATABASE_MONGO_DBZ_PASSWORD, DATABASE_MONGO_DBZ_LOGIN_DBNAME);
        client.execute(DATABASE_MONGO_DBZ_DBNAME, "customers", col -> {
            Document doc = new Document()
                    .append("first_name", firstName)
                    .append("last_name", lastName)
                    .append("email", email);
            col.insertOne(doc);
        });
    }

    @Test
    @Order(1)
    public void shouldHaveRegisteredConnector() {
        Request r = new Request.Builder()
                .url(kafkaConnectController.getApiURL().resolve("/connectors"))
                .build();
        awaitAssert(() -> {
            try (Response res = httpClient.newCall(r).execute()) {
                assertThat(res.body().string()).contains(connectorName);
            }
        });
    }

    @Test
    @Order(2)
    public void shouldCreateKafkaTopics() {
        assertions.assertTopicsExist(
                dbServerName + ".inventory.customers",
                dbServerName + ".inventory.orders",
                dbServerName + ".inventory.products");
    }

    @Test
    @Order(3)
    public void shouldContainRecordsInCustomersTopic() throws IOException {
        kafkaConnectController.waitForMongoSnapshot(dbServerName);
        awaitAssert(() -> assertions.assertRecordsCount(dbServerName + ".inventory.customers", 4));
    }

    @Test
    @Order(4)
    public void shouldStreamChanges() {
        insertCustomer("Tom", "Tester", "tom@test.com");
        awaitAssert(() -> assertions.assertRecordsCount(dbServerName + ".inventory.customers", 5));
        awaitAssert(() -> assertions.assertRecordsContain(dbServerName + ".inventory.customers", "tom@test.com"));
    }

    @Test
    @Order(5)
    public void shouldBeDown() throws IOException {
        kafkaConnectController.undeployConnector(connectorName);
        insertCustomer("Jerry", "Tester", "jerry@test.com");
        awaitAssert(() -> assertions.assertRecordsCount(dbServerName + ".inventory.customers", 5));
    }

    @Test
    @Order(6)
    public void shouldResumeStreamingAfterRedeployment() throws IOException, InterruptedException {
        kafkaConnectController.deployConnector(connectorName, connectorConfig);
        awaitAssert(() -> assertions.assertRecordsCount(dbServerName + ".inventory.customers", 6));
        awaitAssert(() -> assertions.assertRecordsContain(dbServerName + ".inventory.customers", "jerry@test.com"));
    }

    @Test
    @Order(7)
    public void shouldBeDownAfterCrash() {
        operatorController.disable();
        kafkaConnectController.destroy();
        insertCustomer("Nibbles", "Tester", "nibbles@test.com");
        awaitAssert(() -> assertions.assertRecordsCount(dbServerName + ".inventory.customers", 6));
    }

    @Test
    @Order(8)
    public void shouldResumeStreamingAfterCrash() throws InterruptedException {
        operatorController.enable();
        kafkaConnectController.waitForCluster();
        awaitAssert(() -> assertions.assertMinimalRecordsCount(dbServerName + ".inventory.customers", 7));
        awaitAssert(() -> assertions.assertRecordsContain(dbServerName + ".inventory.customers", "nibbles@test.com"));
    }
}
