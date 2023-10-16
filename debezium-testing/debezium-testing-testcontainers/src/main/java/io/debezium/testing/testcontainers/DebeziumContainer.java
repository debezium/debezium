/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.testcontainers;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.utility.DockerImageName;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.debezium.testing.testcontainers.util.ContainerImageVersions;

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

    private static final String DEBEZIUM_CONTAINER = "quay.io/debezium/connect";
    private static final String DEBEZIUM_NIGHTLY_TAG = "nightly";

    private static final int KAFKA_CONNECT_PORT = 8083;
    private static final Duration DEBEZIUM_CONTAINER_STARTUP_TIMEOUT = Duration.ofSeconds(waitTimeForRecords() * 30);
    private static final String TEST_PROPERTY_PREFIX = "debezium.test.";
    public static final MediaType JSON = MediaType.get("application/json; charset=utf-8");
    protected static final ObjectMapper MAPPER = new ObjectMapper();
    protected static final OkHttpClient CLIENT = new OkHttpClient();

    public DebeziumContainer(final DockerImageName containerImage) {
        super(containerImage);
        defaultConfig();
    }

    public DebeziumContainer(final Future<String> image) {
        super(image);
        defaultConfig();
    }

    public DebeziumContainer(final String containerImageName) {
        super(DockerImageName.parse(containerImageName));
        defaultConfig();
    }

    public static DebeziumContainer latestStable() {

        return new DebeziumContainer(String.format("%s:%s", DEBEZIUM_CONTAINER, lazilyRetrieveAndCacheLatestStable()));
    }

    private static String debeziumLatestStable;

    private static String lazilyRetrieveAndCacheLatestStable() {
        if (debeziumLatestStable == null) {
            debeziumLatestStable = ContainerImageVersions.getStableVersion("quay.io/debezium/connect");
        }
        return debeziumLatestStable;
    }


    public static DebeziumContainer nightly() {
        return new DebeziumContainer(String.format("%s:%s", DEBEZIUM_CONTAINER, DEBEZIUM_NIGHTLY_TAG));
    }

    private void defaultConfig() {
        setWaitStrategy(
                new HttpWaitStrategy()
                        .forPath("/connectors")
                        .forPort(KAFKA_CONNECT_PORT)
                        .withStartupTimeout(DEBEZIUM_CONTAINER_STARTUP_TIMEOUT));
        withEnv("GROUP_ID", "1");
        withEnv("CONFIG_STORAGE_TOPIC", "debezium_connect_config");
        withEnv("OFFSET_STORAGE_TOPIC", "debezium_connect_offsets");
        withEnv("STATUS_STORAGE_TOPIC", "debezium_connect_status");
        withEnv("CONNECT_KEY_CONVERTER_SCHEMAS_ENABLE", "false");
        withEnv("CONNECT_VALUE_CONVERTER_SCHEMAS_ENABLE", "false");

        withExposedPorts(KAFKA_CONNECT_PORT);
    }

    public DebeziumContainer withKafka(final KafkaContainer kafkaContainer) {
        return withKafka(kafkaContainer.getNetwork(), kafkaContainer.getNetworkAliases().get(0) + ":9092");
    }

    public DebeziumContainer withKafka(final Network network, final String bootstrapServers) {
        withNetwork(network);
        withEnv("BOOTSTRAP_SERVERS", bootstrapServers);
        return self();
    }

    public DebeziumContainer enableApicurioConverters() {
        withEnv("ENABLE_APICURIO_CONVERTERS", "true");
        return self();
    }

    public DebeziumContainer enableJolokia() {
        withEnv("ENABLE_JOLOKIA", "true");
        withExposedPorts(8778);
        return self();
    }

    public static int waitTimeForRecords() {
        return Integer.parseInt(System.getProperty(TEST_PROPERTY_PREFIX + "records.waittime", "2"));
    }

    public String getTarget() {
        return "http://" + getContainerIpAddress() + ":" + getMappedPort(KAFKA_CONNECT_PORT);
    }

    /**
     * Returns the "/connectors/<connector>" endpoint.
     */
    public String getConnectorsUri() {
        return getTarget() + "/connectors/";
    }

    /**
     * Returns the "/connectors/<connector>" endpoint.
     */
    public String getConnectorUri(String connectorName) {
        return getConnectorsUri() + connectorName;
    }

    /**
     * Returns the "/connectors/<connector>/pause" endpoint.
     */
    public String getPauseConnectorUri(String connectorName) {
        return getConnectorUri(connectorName) + "/pause";
    }

    /**
     * Returns the "/connectors/<connector>/pause" endpoint.
     */
    public String getResumeConnectorUri(String connectorName) {
        return getConnectorUri(connectorName) + "/resume";
    }

    /**
     * Returns the "/connectors/<connector>/status" endpoint.
     */
    public String getConnectorStatusUri(String connectorName) {
        return getConnectorUri(connectorName) + "/status";
    }

    /**
     * Returns the "/connectors/<connector>/config" endpoint.
     */
    public String getConnectorConfigUri(String connectorName) {
        return getConnectorUri(connectorName) + "/config";
    }

    public void registerConnector(String name, ConnectorConfiguration configuration) {
        final Connector connector = Connector.from(name, configuration);

        executePOSTRequestSuccessfully(connector.toJson(), getConnectorsUri());

        // To avoid a 409 error code meanwhile connector is being configured.
        // This is just a guard, probably in most of use cases you won't need that as preparation time of the test might be enough to configure connector.
        Awaitility.await()
                .atMost(waitTimeForRecords() * 5L, TimeUnit.SECONDS)
                .until(() -> isConnectorConfigured(connector.getName()));
    }

    public void updateOrCreateConnector(String name, ConnectorConfiguration newConfiguration) {
        executePUTRequestSuccessfully(newConfiguration.getConfiguration().toString(), getConnectorConfigUri(name));

        // To avoid a 409 error code meanwhile connector is being configured.
        // This is just a guard, probably in most of use cases you won't need that as preparation time of the test might be enough to configure connector.
        Awaitility.await()
                .atMost(waitTimeForRecords() * 5L, TimeUnit.SECONDS)
                .until(() -> isConnectorConfigured(name));
    }

    private static void handleFailedResponse(Response response) {
        String responseBodyContent = "{empty response body}";
        try (ResponseBody responseBody = response.body()) {
            if (null != responseBody) {
                responseBodyContent = responseBody.string();
            }
            throw new IllegalStateException("Unexpected response: " + response + " ; Response Body: " + responseBodyContent);
        }
        catch (IOException e) {
            throw new RuntimeException("Error connecting to Debezium container", e);
        }
    }

    private void executePOSTRequestSuccessfully(final String payload, final String fullUrl) {
        final RequestBody body = RequestBody.create(payload, JSON);
        final Request request = new Request.Builder().url(fullUrl).post(body).build();

        try (Response response = CLIENT.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                handleFailedResponse(response);
            }
        }
        catch (IOException e) {
            throw new RuntimeException("Error connecting to Debezium container", e);
        }
    }

    private void executePUTRequestSuccessfully(final String payload, final String fullUrl) {
        final RequestBody body = RequestBody.create(payload, JSON);
        final Request request = new Request.Builder().url(fullUrl).put(body).build();

        try (Response response = CLIENT.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                handleFailedResponse(response);
            }
        }
        catch (IOException e) {
            throw new RuntimeException("Error connecting to Debezium container", e);
        }
    }

    protected static Response executeGETRequest(Request request) {
        try {
            return CLIENT.newCall(request).execute();
        }
        catch (IOException e) {
            throw new RuntimeException("Error connecting to Debezium container", e);
        }
    }

    protected static Response executeGETRequestSuccessfully(Request request) {
        final Response response = executeGETRequest(request);
        if (!response.isSuccessful()) {
            handleFailedResponse(response);
        }
        return response;
    }

    public boolean connectorIsNotRegistered(String connectorName) {
        final Request request = new Request.Builder().url(getConnectorUri(connectorName)).build();
        try (Response response = executeGETRequest(request)) {
            return response.code() == 404;
        }
    }

    protected void deleteDebeziumConnector(String connectorName) {
        final Request request = new Request.Builder().url(getConnectorUri(connectorName)).delete().build();
        executeGETRequestSuccessfully(request).close();
    }

    public void deleteConnector(String connectorName) {
        deleteDebeziumConnector(connectorName);

        Awaitility.await()
                .atMost(waitTimeForRecords() * 5L, TimeUnit.SECONDS)
                .until(() -> connectorIsNotRegistered(connectorName));
    }

    public List<String> getRegisteredConnectors() {
        final Request request = new Request.Builder().url(getConnectorsUri()).build();
        try (ResponseBody responseBody = executeGETRequestSuccessfully(request).body()) {
            if (null != responseBody) {
                return MAPPER.readValue(responseBody.string(), new TypeReference<List<String>>() {
                });
            }
        }
        catch (IOException e) {
            throw new IllegalStateException("Error fetching list of registered connectors", e);
        }
        return Collections.emptyList();
    }

    public boolean isConnectorConfigured(String connectorName) {
        final Request request = new Request.Builder().url(getConnectorUri(connectorName)).build();
        try (Response response = executeGETRequest(request)) {
            return response.isSuccessful();
        }
    }

    public void ensureConnectorRegistered(String connectorName) {
        Awaitility.await()
                .atMost(waitTimeForRecords() * 5L, TimeUnit.SECONDS)
                .until(() -> isConnectorConfigured(connectorName));
    }

    public void deleteAllConnectors() {
        final List<String> connectorNames = getRegisteredConnectors();

        for (String connectorName : connectorNames) {
            deleteDebeziumConnector(connectorName);
        }

        Awaitility.await()
                .atMost(waitTimeForRecords() * 5L, TimeUnit.SECONDS)
                .until(() -> getRegisteredConnectors().size() == 0);
    }

    public Connector.State getConnectorState(String connectorName) {
        final Request request = new Request.Builder().url(getConnectorStatusUri(connectorName)).build();
        try (ResponseBody responseBody = executeGETRequestSuccessfully(request).body()) {
            if (null != responseBody) {
                final ObjectNode parsedObject = (ObjectNode) MAPPER.readTree(responseBody.string());
                return Connector.State.valueOf(parsedObject.get("connector").get("state").asText());
            }
            return null;
        } catch (IOException e) {
            throw new IllegalStateException("Error fetching connector state for connector: " + connectorName, e);
        }
    }

    public Connector.State getConnectorTaskState(String connectorName, int taskNumber) {
        final Request request = new Request.Builder().url(getConnectorStatusUri(connectorName)).get().build();
        try (ResponseBody responseBody = executeGETRequestSuccessfully(request).body()) {
            if (null != responseBody) {
                final ObjectNode parsedObject = (ObjectNode) MAPPER.readTree(responseBody.string());
                final JsonNode tasksNode = parsedObject.get("tasks").get(taskNumber);
                // Task array can return null if the array is empty or the task number is not within bounds
                if (tasksNode == null) {
                    return null;
                }
                return Connector.State.valueOf(tasksNode.get("state").asText());
            }
            return null;
        }
        catch (IOException e) {
            throw new IllegalStateException("Error fetching connector task state for connector task: "
                    + connectorName + "#" + taskNumber, e);
        }
    }

    public String getConnectorConfigProperty(String connectorName, String configPropertyName) {
        final Request request = new Request.Builder().url(getConnectorConfigUri(connectorName)).get().build();

        try (ResponseBody responseBody = executeGETRequestSuccessfully(request).body()) {
            if (null != responseBody) {
                final ObjectNode parsedObject = (ObjectNode) MAPPER.readTree(responseBody.string());
                return parsedObject.get(configPropertyName).asText();
            }
            return null;
        }
        catch (IOException e) {
            throw new IllegalStateException("Error fetching connector config property for connector: " + connectorName, e);
        }
    }

    public void pauseConnector(String connectorName) {
        final Request request = new Request.Builder()
                .url(getPauseConnectorUri(connectorName))
                .put(RequestBody.create("", JSON))
                .build();
        executeGETRequestSuccessfully(request).close();

        Awaitility.await()
                .atMost(waitTimeForRecords() * 5L, TimeUnit.SECONDS)
                .until(() -> getConnectorState(connectorName) == Connector.State.PAUSED);
    }

    public void resumeConnector(String connectorName) {
        final Request request = new Request.Builder()
                .url(getResumeConnectorUri(connectorName))
                .put(RequestBody.create("", JSON))
                .build();
        executeGETRequestSuccessfully(request).close();

        Awaitility.await()
                .atMost(waitTimeForRecords() * 5L, TimeUnit.SECONDS)
                .until(() -> getConnectorState(connectorName) == Connector.State.RUNNING);
    }

    public void ensureConnectorState(String connectorName, Connector.State status) {
        Awaitility.await()
                .atMost(waitTimeForRecords() * 5L, TimeUnit.SECONDS)
                .until(() -> getConnectorState(connectorName) == status);
    }

    public void ensureConnectorTaskState(String connectorName, int taskNumber, Connector.State status) {
        Awaitility.await()
                .atMost(waitTimeForRecords() * 5L, TimeUnit.SECONDS)
                .until(() -> getConnectorTaskState(connectorName, taskNumber) == status);
    }

    public void ensureConnectorConfigProperty(String connectorName, String propertyName, String expectedValue) {
        Awaitility.await()
                .atMost(waitTimeForRecords() * 5L, TimeUnit.SECONDS)
                .until(() -> Objects.equals(expectedValue, getConnectorConfigProperty(connectorName, propertyName)));
    }

    public static ConnectorConfiguration getPostgresConnectorConfiguration(PostgreSQLContainer<?> postgresContainer, int id, String... options) {
        final ConnectorConfiguration config = ConnectorConfiguration.forJdbcContainer(postgresContainer)
                .with("topic.prefix", "dbserver" + id)
                .with("slot.name", "debezium_" + id);

        if (options != null && options.length > 0) {
            for (int i = 0; i < options.length; i += 2) {
                config.with(options[i], options[i + 1]);
            }
        }
        return config;
    }
}
