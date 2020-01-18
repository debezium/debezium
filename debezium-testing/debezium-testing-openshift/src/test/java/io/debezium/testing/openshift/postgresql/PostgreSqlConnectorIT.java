/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.openshift.postgresql;

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
import io.debezium.testing.openshift.tools.databases.PostgreSqlDeployer;
import io.debezium.testing.openshift.tools.kafka.ConnectorConfigBuilder;

import okhttp3.OkHttpClient;
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

    private static PostgreSqlDeployer dbDeployer;
    private static OkHttpClient httpClient = new OkHttpClient();
    private static ConnectorFactories connectorFactories = new ConnectorFactories();
    private static String connectorName;

    @BeforeAll
    public static void setupDatabase() throws IOException, InterruptedException {
        if (!ConfigProperties.DATABASE_MYSQL_HOST.isPresent()) {
            dbDeployer = new PostgreSqlDeployer(ocp)
                    .withProject(ConfigProperties.OCP_PROJECT_POSTGRESQL)
                    .withDeployment(DB_DEPLOYMENT_PATH)
                    .withServices(DB_SERVICE_PATH_LB, DB_SERVICE_PATH);
            dbDeployer.deploy();
        }

        String id = testUtils.getUniqueId();
        connectorName = CONNECTOR_NAME + "-" + id;
        ConnectorConfigBuilder connectorConfig = connectorFactories.postgresql()
                .put("database.server.name", connectorName)
                .put("slot.name", "debezium_" + id)
                .put("slot.drop.on.stop", true);
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
                connectorName + ".inventory.customers",
                connectorName + ".inventory.orders",
                connectorName + ".inventory.products",
                connectorName + ".inventory.products_on_hand");
    }

    @Test
    @Order(3)
    public void shouldContainRecordsInCustomersTopic() throws IOException {
        kafkaConnectController.waitForPostgreSqlSnapshot(connectorName);
        assertRecordsCount(connectorName + ".inventory.customers", 4);
    }

}
