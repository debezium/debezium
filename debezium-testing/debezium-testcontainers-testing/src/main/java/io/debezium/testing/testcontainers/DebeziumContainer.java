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

    private final OkHttpClient client = new OkHttpClient();
    public static final MediaType JSON = MediaType.get("application/json; charset=utf-8");

    public DebeziumContainer(final String version) {
        super("debezium/connect:" + version);

        setWaitStrategy(
                Wait.forHttp("/connectors")
                        .forPort(8083)
                        .forStatusCode(200));

        withEnv("GROUP_ID", "1");
        withEnv("CONFIG_STORAGE_TOPIC", "debezium_connect_config");
        withEnv("OFFSET_STORAGE_TOPIC", "debezium_connect_offsets");
        withEnv("STATUS_STORAGE_TOPIC", "debezium_connect_status");
        withEnv("CONNECT_KEY_CONVERTER_SCHEMAS_ENABLE", "false");
        withEnv("CONNECT_VALUE_CONVERTER_SCHEMAS_ENABLE", "false");

        withExposedPorts(8083);
    }

    public DebeziumContainer(final String version, final KafkaContainer kafkaContainer) {
        this(version);
        withKafka(kafkaContainer);
    }

    public DebeziumContainer withKafka(final KafkaContainer kafkaContainer) {
        return withKafka(kafkaContainer.getNetwork(), kafkaContainer.getNetworkAliases().get(0) + ":9092");
    }

    public DebeziumContainer withKafka(final Network network, final String bootstrapServers) {
        withNetwork(network);
        withEnv("BOOTSTRAP_SERVERS", bootstrapServers);
        return self();
    }

    public void registerConnector(final Connector connector) throws IOException {
        this.registerConnector(connector.getName(), connector.toJson());
    }

    public void registerConnector(final String connectorName, final String payload) throws IOException {
        final String fullUrl = "http://" + getTarget() + "/connectors/";

        registerConnectorToDebezium(payload, fullUrl);
        // To avoid a 409 error code meanwhile connector is being configured.
        // This is just a guard, probably in most of use cases you won't need that as preparation time of the test might be enough to configure connector.
        Awaitility.await()
                .atMost(5, TimeUnit.SECONDS)
                .until(() -> isConnectorConfigured(connectorName));
    }

    private void registerConnectorToDebezium(final String payload, final String fullUrl) throws IOException {
        final RequestBody body = RequestBody.create(payload, JSON);
        final Request request = new Request.Builder().url(fullUrl).post(body).build();

        try (Response response = client.newCall(request).execute()) {
            if (!response.isSuccessful())
                throw new IOException("Unexpected code " + response + "Message: " + response.body().string());
        }
    }

    private boolean isConnectorConfigured(String connectorName) throws IOException {
        final Request request = new Request.Builder()
                .url("http://" + getConnectorTarget(connectorName))
                .build();

        try (Response response = client.newCall(request).execute()) {
            return response.isSuccessful();
        }
    }

    public String getConnectors() {
        return getTarget() + "/connectors/";
    }

    public String getConnectorTarget(String connectorName) {
        return getConnectors() + connectorName;
    }

    public String getTarget() {
        return getContainerIpAddress() + ":" + getMappedPort(8083);
    }

}
