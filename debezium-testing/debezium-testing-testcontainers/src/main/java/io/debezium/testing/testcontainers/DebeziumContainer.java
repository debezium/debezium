/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.testcontainers;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;

/**
 * Debezium Container main class.
 */
public class DebeziumContainer extends GenericContainer<DebeziumContainer> {

    private static final int KAFKA_CONNECT_PORT = 8083;
    public static final MediaType JSON = MediaType.get("application/json; charset=utf-8");
    protected static final ObjectMapper MAPPER = new ObjectMapper();
    protected static final OkHttpClient CLIENT = new OkHttpClient();

    public DebeziumContainer(final String containerImageName) {
        super(containerImageName);

        defaultConfig();
    }

    public DebeziumContainer(final Future<String> image) {
        super(image);

        defaultConfig();
    }

    private void defaultConfig() {
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

    public String getTarget() {
        return "http://" + getContainerIpAddress() + ":" + getMappedPort(8083);
    }

    /**
     * Returns the "/connectors/<connector>" endpoint.
     */
    public String getConnectorsURI() {
        return getTarget() + "/connectors/";
    }

    /**
     * Returns the "/connectors/<connector>" endpoint.
     */
    public String getConnectorURI(String connectorName) {
        return getConnectorsURI() + connectorName;
    }

    /**
     * Returns the "/connectors/<connector>/pause" endpoint.
     */
    public String getPauseConnectorURI(String connectorName) {
        return getConnectorURI(connectorName) + "/pause";
    }

    /**
     * Returns the "/connectors/<connector>/status" endpoint.
     */
    public String getConnectorStatusURI(String connectorName) {
        return getConnectorURI(connectorName) + "/status";
    }

    public void registerConnector(String name, ConnectorConfiguration configuration) throws IOException {
        Connector connector = Connector.from(name, configuration);

        registerConnectorToDebezium(connector.toJson(), getConnectorsURI());

        // To avoid a 409 error code meanwhile connector is being configured.
        // This is just a guard, probably in most of use cases you won't need that as preparation time of the test might be enough to configure connector.
        Awaitility.await()
                .atMost(10, TimeUnit.SECONDS)
                .until(() -> isConnectorConfigured(connector.getName()));
    }

    private void registerConnectorToDebezium(final String payload, final String fullUrl) throws IOException {
        final RequestBody body = RequestBody.create(payload, JSON);
        final Request request = new Request.Builder().url(fullUrl).post(body).build();

        try (Response response = CLIENT.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                throw new IOException("Unexpected code " + response + "Message: " + Objects.requireNonNull(response.body()).string());
            }
        }
    }

    protected static Response executeRequest(Request request) throws IOException {
        return CLIENT.newCall(request).execute();
    }

    protected static Response executeRequestSuccessful(Request request) throws IOException {
        Response response = executeRequest(request);
        String responseBodyContent = "{empty response body}";
        if (!response.isSuccessful()) {
            ResponseBody responseBody = response.body();
            if (null != responseBody) {
                responseBodyContent = responseBody.string();
                responseBody.close();
            }
            throw new IOException("Unexpected response: " + response + " Response Body: " + responseBodyContent);
        }
        return response;
    }

    public boolean connectorIsNotRegistered(String connectorName) throws IOException {
        try (Response response = executeRequest(new Request.Builder().url(getConnectorURI(connectorName)).get().build())) {
            boolean connectorIsNotRegistered = response.code() == 404;
            response.close();
            return connectorIsNotRegistered;
        }
    }

    protected void deleteDebeziumConnector(String connectorName) throws IOException {
        executeRequestSuccessful(new Request.Builder().url(getConnectorURI(connectorName)).delete().build()).close();
    }

    public void deleteConnector(String connectorName) throws IOException {
        deleteDebeziumConnector(connectorName);

        Awaitility.await()
                .atMost(10, TimeUnit.SECONDS)
                .until(() -> connectorIsNotRegistered(connectorName));
    }

    public List<String> getRegisteredConnectors() throws IOException {
        Request request = new Request.Builder().url(getConnectorsURI()).get().build();
        try (ResponseBody responseBody = executeRequestSuccessful(request).body()) {
            if (null != responseBody) {
                String string = responseBody.string();
                responseBody.close();
                return MAPPER.readValue(string, new TypeReference<List<String>>() {
                });
            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        return Collections.emptyList();
    }

    public boolean isConnectorConfigured(String connectorName) throws IOException {
        Request request = new Request.Builder().url(getConnectorURI(connectorName)).get().build();
        try (Response response = executeRequest(request)) {
            boolean isConnectorConfigured = response.isSuccessful();
            response.close();
            return isConnectorConfigured;
        }
    }

    public void ensureConnectorRegistered(String connectorName) {
        Awaitility.await()
                .atMost(10, TimeUnit.SECONDS)
                .until(() -> isConnectorConfigured(connectorName));
    }

    public void deleteAllConnectors() throws IOException {
        List<String> connectorNames = getRegisteredConnectors();

        for (String connectorName : connectorNames) {
            deleteDebeziumConnector(connectorName);
        }

        Awaitility.await()
                .atMost(10, TimeUnit.SECONDS)
                .until(() -> getRegisteredConnectors().size() == 0);
    }

    public Connector.State getConnectorState(String connectorName) throws IOException {
        final Request request = new Request.Builder().url(getConnectorStatusURI(connectorName)).get().build();
        ResponseBody responseBody = executeRequestSuccessful(request).body();
        if (null != responseBody) {
            ObjectNode parsedObject = (ObjectNode) MAPPER.readTree(responseBody.string());
            responseBody.close();
            return Connector.State.valueOf(parsedObject.get("connector").get("state").asText());
        }
        return null;
    }

    public Connector.State getConnectorTaskState(String connectorName, int taskNumber) throws IOException {
        final Request request = new Request.Builder().url(getConnectorStatusURI(connectorName)).get().build();
        ResponseBody responseBody = executeRequestSuccessful(request).body();
        if (null != responseBody) {
            ObjectNode parsedObject = (ObjectNode) MAPPER.readTree(responseBody.string());
            responseBody.close();
            return Connector.State.valueOf(parsedObject.get("tasks").get(taskNumber).get("state").asText());
        }
        return null;
    }

    public void pauseConnector(String connectorName) throws IOException {
        final Request request = new Request.Builder()
                .url(getPauseConnectorURI(connectorName))
                .put(RequestBody.create("", JSON))
                .build();
        executeRequestSuccessful(request).close();

        Awaitility.await()
                .atMost(10, TimeUnit.SECONDS)
                .until(() -> getConnectorState(connectorName) == Connector.State.PAUSED);
    }

    public void ensureConnectorState(String connectorName, Connector.State status) throws IOException {
        Awaitility.await()
                .atMost(10, TimeUnit.SECONDS)
                .until(() -> getConnectorState(connectorName) == status);
    }

    public void ensureConnectorTaskState(String connectorName, int taskNumber, Connector.State status) throws IOException {
        Awaitility.await()
                .atMost(10, TimeUnit.SECONDS)
                .until(() -> getConnectorTaskState(connectorName, taskNumber) == status);
    }
}
