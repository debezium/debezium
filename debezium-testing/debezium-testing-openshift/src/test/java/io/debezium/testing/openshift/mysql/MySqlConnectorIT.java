/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.openshift.mysql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import io.debezium.testing.openshift.ConnectorTestBase;
import io.debezium.testing.openshift.resources.ConfigProperties;
import io.debezium.testing.openshift.resources.ConnectorFactories;
import io.debezium.testing.openshift.tools.databases.MySqlDeployer;
import io.debezium.testing.openshift.tools.kafka.ConnectorConfigBuilder;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

/**
 * @author Jakub Cechacek
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@Tag("acceptance")
@Tag("mysql")
public class MySqlConnectorIT extends ConnectorTestBase {

    public static final String DB_DEPLOYMENT_PATH = "/database-resources/mysql/deployment.yaml";
    public static final String DB_SERVICE_PATH_LB = "/database-resources/mysql/service-lb.yaml";
    public static final String DB_SERVICE_PATH = "/database-resources/mysql/service.yaml";

    public static final String CONNECTOR_NAME = "inventory-connector-mysql";

    private static MySqlDeployer dbDeployer;
    private static OkHttpClient httpClient = new OkHttpClient();
    private static ConnectorFactories connectorFactories = new ConnectorFactories();
    private static String connectorName;

    @BeforeAll
    public static void setupDatabase() throws IOException, InterruptedException {
        if (!ConfigProperties.DATABASE_MYSQL_HOST.isPresent()) {
            dbDeployer = new MySqlDeployer(ocp)
                    .withProject(ConfigProperties.OCP_PROJECT_MYSQL)
                    .withDeployment(DB_DEPLOYMENT_PATH)
                    .withServices(DB_SERVICE_PATH_LB, DB_SERVICE_PATH);
            dbDeployer.deploy();
        }

        connectorName = CONNECTOR_NAME + "-" + testUtils.getUniqueId();
        ConnectorConfigBuilder connectorConfig = connectorFactories.mysql().put("database.server.name", connectorName);
        kafkaConnectController.deployConnector(connectorName, connectorConfig);
    }

    @AfterAll
    public static void tearDownDatabase() throws IOException {
        kafkaConnectController.undeployConnector(connectorName);
    }

    @Test
    @Order(1)
    public void shouldHaveRegisteredConnector() {
        Request r = new Request.Builder()
                .url(kafkaConnectController.getApiURL().resolve("/connectors"))
                .build();

        await().atMost(1, TimeUnit.MINUTES).untilAsserted(() -> {
            try (Response res = httpClient.newCall(r).execute()) {
                assertThat(res.body().string()).contains(connectorName);
            }
        });
    }

    @Test
    @Order(2)
    public void shouldCreateKafkaTopics() {
        assertTopicsExist(
                connectorName + ".inventory.addresses",
                connectorName + ".inventory.customers",
                connectorName + ".inventory.geom",
                connectorName + ".inventory.orders",
                connectorName + ".inventory.products",
                connectorName + ".inventory.products_on_hand");
    }

    @Test
    @Order(3)
    public void shouldContainRecordsInCustomersTopic() throws IOException {
        kafkaConnectController.waitForMySqlSnapshot(connectorName);
        assertRecordsCount(connectorName + ".inventory.customers", 4);
    }

}
