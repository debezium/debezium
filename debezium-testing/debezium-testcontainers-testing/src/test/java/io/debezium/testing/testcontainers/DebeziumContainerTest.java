/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.testcontainers;

import static org.fest.assertions.Assertions.assertThat;

import java.io.IOException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

public class DebeziumContainerTest {

    private static PostgreSQLContainer postgreContainer = new PostgreSQLContainer<>("debezium/postgres:11");
    private static KafkaContainer kafkaContainer = new KafkaContainer("5.3.1");
    private static DebeziumContainer debeziumContainer = new DebeziumContainer("1.0.0.Final");

    @BeforeClass
    public static void initializeInfrastructure() {
        kafkaContainer.start();

        postgreContainer.withNetwork(kafkaContainer.getNetwork()).start();
        debeziumContainer.withKafka(kafkaContainer).start();
    }

    @AfterClass
    public static void stopInfrastructure() {
        debeziumContainer.stop();
        postgreContainer.stop();
        kafkaContainer.stop();
    }

    @Test
    public void shouldRegisterPostgreSQLConnector() throws IOException {
        final Configuration configuration = JdbcConfiguration.fromJdbcContainer(postgreContainer);
        configuration.with("tasks.max", 1);
        configuration.with("database.server.name", "dbserver1");
        configuration.with("table.whitelist", "public.outbox");

        final Connector connector = Connector.from("my-connector", configuration);

        debeziumContainer.registerConnector(connector);

        final String connectorInfo = readConnector(debeziumContainer.getConnectorTarget("my-connector"));

        final ObjectMapper mapper = new ObjectMapper();
        final ObjectNode connectorInfoNode = mapper.readValue(connectorInfo, ObjectNode.class);

        final ArrayNode tasks = (ArrayNode) connectorInfoNode.get("tasks");
        assertThat(tasks.size()).isEqualTo(1);

        final JsonNode expectedTask = tasks.get(0);
        assertThat(expectedTask.get("connector").asText()).isEqualTo("my-connector");
    }

    private String readConnector(String url) throws IOException {
        final OkHttpClient client = new OkHttpClient();
        final Request request = new Request.Builder().url("http://" + url).build();

        try (Response response = client.newCall(request).execute()) {
            return response.body().string();
        }
    }

}
