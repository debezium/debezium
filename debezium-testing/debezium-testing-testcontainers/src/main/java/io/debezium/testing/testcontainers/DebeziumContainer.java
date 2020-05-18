/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.testcontainers;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;

import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

/**
 * Debezium Container main class.
 */
public class DebeziumContainer extends GenericContainer<DebeziumContainer> {

    private static final int KAFKA_CONNECT_PORT = 8083;
    private final OkHttpClient client = new OkHttpClient();
    public static final MediaType JSON = MediaType.get("application/json; charset=utf-8");

    public DebeziumContainer(final String containerImageName) {
        super(containerImageName);

        setWaitStrategy(
                Wait.forHttp("/connectors")
                        .forPort(KAFKA_CONNECT_PORT)
                        .forStatusCode(200));

        withEnv("GROUP_ID", "1");
        withEnv("CONFIG_STORAGE_TOPIC", "debezium_connect_config");
        withEnv("OFFSET_STORAGE_TOPIC", "debezium_connect_offsets");
        withEnv("STATUS_STORAGE_TOPIC", "debezium_connect_status");
        withEnv("CONNECT_KEY_CONVERTER_SCHEMAS_ENABLE", "false");
        withEnv("CONNECT_VALUE_CONVERTER_SCHEMAS_ENABLE", "false");

        withExposedPorts(8083);
    }

    public DebeziumContainer withKafka(final KafkaContainer kafkaContainer) {
        return withKafka(kafkaContainer.getNetwork(), kafkaContainer.getNetworkAliases().get(0) + ":9092");
    }

    public DebeziumContainer withKafka(final Network network, final String bootstrapServers) {
        withNetwork(network);
        withEnv("BOOTSTRAP_SERVERS", bootstrapServers);
        return self();
    }

    public void registerConnector(String name, ConnectorConfiguration configuration) throws IOException {
        Connector connector = Connector.from(name, configuration);

        registerConnectorToDebezium(connector.toJson(), getConnectors());

        // To avoid a 409 error code meanwhile connector is being configured.
        // This is just a guard, probably in most of use cases you won't need that as preparation time of the test might be enough to configure connector.
        Awaitility.await()
                .atMost(10, TimeUnit.SECONDS)
                .until(() -> isConnectorConfigured(connector.getName()));
    }

    private void registerConnectorToDebezium(final String payload, final String fullUrl) throws IOException {
        final RequestBody body = RequestBody.create(payload, JSON);
        final Request request = new Request.Builder().url(fullUrl).post(body).build();

        try (Response response = client.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                throw new IOException("Unexpected code " + response + "Message: " + response.body().string());
            }
        }
    }

    private boolean isConnectorConfigured(String connectorName) throws IOException {
        final Request request = new Request.Builder()
                .url(getConnector(connectorName))
                .build();

        try (Response response = client.newCall(request).execute()) {
            return response.isSuccessful();
        }
    }

    /**
     * Returns the "/connectors" endpoint.
     */
    public String getConnectors() {
        return getTarget() + "/connectors/";
    }

    /**
     * Returns the "/connectors/<connector>" endpoint.
     */
    public String getConnector(String connectorName) {
        return getConnectors() + connectorName;
    }

    /**
     * Returns the "/connectors/<connector>/status" endpoint.
     */
    public String getConnectorStatus(String connectorName) {
        return getConnectors() + connectorName + "/status";
    }

    public String getTarget() {
        return "http://" + getContainerIpAddress() + ":" + getMappedPort(8083);
    }
}
