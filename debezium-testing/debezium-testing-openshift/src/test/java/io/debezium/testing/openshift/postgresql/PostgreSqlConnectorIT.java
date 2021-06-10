/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.openshift.postgresql;

import static io.debezium.testing.openshift.assertions.KafkaAssertions.awaitAssert;
import static io.debezium.testing.openshift.tools.ConfigProperties.DATABASE_POSTGRESQL_DBZ_DBNAME;
import static io.debezium.testing.openshift.tools.ConfigProperties.DATABASE_POSTGRESQL_PASSWORD;
import static io.debezium.testing.openshift.tools.ConfigProperties.DATABASE_POSTGRESQL_USERNAME;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.sql.SQLException;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import io.debezium.testing.openshift.ConnectorTestBase;
import io.debezium.testing.openshift.tools.ConfigProperties;
import io.debezium.testing.openshift.tools.databases.OcpSqlDatabaseController;
import io.debezium.testing.openshift.tools.databases.SqlDatabaseClient;
import io.debezium.testing.openshift.tools.databases.postgresql.OcpPostgreSqlDeployer;
import io.debezium.testing.openshift.tools.kafka.ConnectorConfigBuilder;

import okhttp3.Request;
import okhttp3.Response;

/**
 * @author Jakub Cechacek
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@Tag("acceptance")
@Tag("postgresql")
public class PostgreSqlConnectorIT extends ConnectorTestBase {

    public static final String DB_DEPLOYMENT_PATH = "/database-resources/postgresql/deployment.yaml";
    public static final String DB_SERVICE_PATH_LB = "/database-resources/postgresql/service-lb.yaml";
    public static final String DB_SERVICE_PATH = "/database-resources/postgresql/service.yaml";

    public static final String CONNECTOR_NAME = "inventory-connector-postgresql";

    private static OcpSqlDatabaseController dbController;
    private static ConnectorConfigBuilder connectorConfig;
    private static String connectorName;
    private static String dbServerName;

    @BeforeAll
    public static void setupDatabase() throws IOException, InterruptedException, ClassNotFoundException {
        Class.forName("org.postgresql.Driver");

        if (!ConfigProperties.DATABASE_MYSQL_HOST.isPresent()) {
            OcpPostgreSqlDeployer deployer = new OcpPostgreSqlDeployer.Deployer()
                    .withOcpClient(ocp)
                    .withProject(ConfigProperties.OCP_PROJECT_POSTGRESQL)
                    .withDeployment(DB_DEPLOYMENT_PATH)
                    .withServices(DB_SERVICE_PATH, DB_SERVICE_PATH_LB)
                    .build();
            dbController = deployer.deploy();
        }

        connectorName = CONNECTOR_NAME + "-" + testUtils.getUniqueId();
        dbServerName = connectorName.replaceAll("-", "_");
        connectorConfig = connectorFactories.postgresql(dbServerName);

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

    private void insertCustomer(String firstName, String lastName, String email) throws SQLException {
        SqlDatabaseClient client = dbController.getDatabaseClient(DATABASE_POSTGRESQL_USERNAME, DATABASE_POSTGRESQL_PASSWORD);
        String sql = "INSERT INTO inventory.customers VALUES  (default, '" + firstName + "', '" + lastName + "', '" + email + "')";
        client.execute(DATABASE_POSTGRESQL_DBZ_DBNAME, sql);
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
                dbServerName + ".inventory.products",
                dbServerName + ".inventory.products_on_hand");
    }

    @Test
    @Order(3)
    public void shouldContainRecordsInCustomersTopic() throws IOException {
        kafkaConnectController.waitForPostgreSqlSnapshot(dbServerName);
        awaitAssert(() -> assertions.assertRecordsCount(dbServerName + ".inventory.customers", 4));
    }

    @Test
    @Order(4)
    public void shouldStreamChanges() throws SQLException {
        insertCustomer("Tom", "Tester", "tom@test.com");
        awaitAssert(() -> assertions.assertRecordsCount(dbServerName + ".inventory.customers", 5));
        awaitAssert(() -> assertions.assertRecordsContain(dbServerName + ".inventory.customers", "tom@test.com"));
    }

    @Test
    @Order(5)
    public void shouldBeDown() throws SQLException, IOException {
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
    public void shouldBeDownAfterCrash() throws SQLException {
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
