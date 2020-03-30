/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.openshift.tools.kafka;

import static io.strimzi.api.kafka.Crds.kafkaConnectOperation;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.testing.openshift.tools.HttpUtils;
import io.debezium.testing.openshift.tools.OpenShiftUtils;
import io.debezium.testing.openshift.tools.WaitConditions;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.networking.NetworkPolicy;
import io.fabric8.kubernetes.api.model.networking.NetworkPolicyPort;
import io.fabric8.kubernetes.api.model.networking.NetworkPolicyPortBuilder;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.openshift.api.model.Route;
import io.fabric8.openshift.client.OpenShiftClient;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.KafkaConnectorList;
import io.strimzi.api.kafka.model.DoneableKafkaConnector;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaConnector;

import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

/**
 * This class provides control over Kafka Connect instance deployed in OpenShift
 * @author Jakub Cechacek
 */
public class KafkaConnectController {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConnectController.class);

    private final OpenShiftClient ocp;
    private final OkHttpClient http;
    private final String project;
    private final OpenShiftUtils ocpUtils;
    private final HttpUtils httpUtils;
    private final boolean useConnectorResources;
    private final String name;

    private KafkaConnect kafkaConnect;
    private Route apiRoute;
    private Route metricsRoute;

    public KafkaConnectController(KafkaConnect kafkaConnect, OpenShiftClient ocp, OkHttpClient http, boolean useConnectorResources) {
        this.kafkaConnect = kafkaConnect;
        this.name = kafkaConnect.getMetadata().getName();
        this.ocp = ocp;
        this.http = http;
        this.useConnectorResources = useConnectorResources;
        this.project = kafkaConnect.getMetadata().getNamespace();
        this.ocpUtils = new OpenShiftUtils(ocp);
        this.httpUtils = new HttpUtils(http);
    }

    /**
     * Disables Kafka Connect by scaling it to ZERO
     *
     * NOTICE: cluster operator needs to be disabled first!
     */
    public void disable() {
        LOGGER.info("Disabling KafkaConnect deployment (scaling to ZERO).");
        ocp.apps().deployments().inNamespace(project).withName(this.kafkaConnect.getMetadata().getName() + "-connect")
                .edit().editSpec().withReplicas(0).endSpec().done();
        await()
                .atMost(30, SECONDS)
                .pollDelay(5, SECONDS)
                .pollInterval(3, SECONDS)
                .until(() -> ocp.pods().inNamespace(project).withLabel("strimzi.io/kind", "KafkaConnect").list().getItems().isEmpty());
    }

    /**
     * Crashes Kafka Connect by force deleting all pods. Then it immediately scales its deployment to ZERO by calling {@link #disable()}
     *
     * NOTICE: cluster operator needs to be disabled first!
     */
    public void destroy() {
        LOGGER.info("Force deleting all KafkaConnect pods.");
        ocp.pods().inNamespace(project).withLabel("strimzi.io/kind", "KafkaConnect").withGracePeriod(0).delete();
        disable();
    }

    /**
     * Wait until KafkaConnect instance is back and ready
     * @return {@link KafkaConnect} resource instance
     * @throws InterruptedException
     */
    public KafkaConnect waitForConnectCluster() throws InterruptedException {
        kafkaConnect = Crds.kafkaConnectOperation(ocp).inNamespace(project).withName(name).waitUntilCondition(WaitConditions::kafkaReadyCondition, 5, MINUTES);
        return kafkaConnect;
    }

    /**
     * Creates network policy allowing access to ports exposed by Kafka Connect
     * @return
     */
    public NetworkPolicy allowServiceAccess() {
        LOGGER.info("Creating NetworkPolicy allowing public access to " + kafkaConnect.getMetadata().getName() + "'s services");

        Map<String, String> labels = new HashMap<>();
        labels.put("strimzi.io/cluster", kafkaConnect.getMetadata().getName());
        labels.put("strimzi.io/kind", "KafkaConnect");
        labels.put("strimzi.io/name", kafkaConnect.getMetadata().getName() + "-connect");

        List<NetworkPolicyPort> ports = Stream.of(8083, 8404)
                .map(IntOrString::new)
                .map(p -> new NetworkPolicyPortBuilder().withProtocol("TCP").withPort(p).build())
                .collect(Collectors.toList());

        NetworkPolicy policy = ocpUtils.createNetworkPolicy(project, kafkaConnect.getMetadata().getName() + "-allowed", labels, ports);
        return policy;
    }

    /**
     * Exposes a route for kafka connect API associated with given KafkaConnect resource
     * @return {@link Route} object
     */
    public Route exposeApi() {
        LOGGER.info("Exposing KafkaConnect API");
        String name = kafkaConnect.getMetadata().getName() + "-connect-api";
        Service service = ocp.services().inNamespace(project).withName(name).get();

        apiRoute = ocpUtils.createRoute(project, name, name, "rest-api", service.getMetadata().getLabels());
        httpUtils.awaitApi(getApiURL());
        return apiRoute;
    }

    /**
     * Exposes a route for prometheus metrics for kafka connect associated with given KafkaConnect resource
     * @return {@link Route} object
     */
    public Route exposeMetrics() {
        LOGGER.info("Exposing KafkaConnect metrics");
        String name = kafkaConnect.getMetadata().getName() + "-connect-metrics";
        String nameSvc = kafkaConnect.getMetadata().getName() + "-connect-api";
        Service service = ocp.services().inNamespace(project).withName(nameSvc).get();

        metricsRoute = ocpUtils
                .createRoute(project, name, nameSvc, "prometheus", service.getMetadata().getLabels());
        httpUtils.awaitApi(getMetricsURL());

        return metricsRoute;
    }

    /**
     * Deploys Kafka connector with given name and configuration via REST
     * @param name connector name
     * @param config connector config
     * @throws IOException or request error
     */
    public void deployConnector(String name, ConnectorConfigBuilder config) throws IOException, InterruptedException {
        if (useConnectorResources) {
            deployConnectorCr(name, config);
        }
        else {
            deployConnectorJson(name, config);
        }
    }

    private void deployConnectorJson(String name, ConnectorConfigBuilder config) throws IOException {
        if (apiRoute == null) {
            throw new IllegalStateException("KafkaConnect API was not exposed");
        }

        HttpUrl url = getApiURL().resolve("/connectors/" + name + "/config");
        Request r = new Request.Builder()
                .url(url)
                .put(RequestBody.create(config.getJsonString(), MediaType.parse("application/json")))
                .build();

        try (Response res = http.newCall(r).execute()) {
            if (!res.isSuccessful()) {
                LOGGER.error(res.request().url().toString());
                throw new RuntimeException("Connector registration request returned status code '" + res.code() + "'");
            }
            LOGGER.info("Registered kafka connector '" + name + "'");
        }
    }

    private void deployConnectorCr(String name, ConnectorConfigBuilder config) throws InterruptedException {
        LOGGER.info("Deploying connector CR");
        KafkaConnector connector = config.getCustomResource();
        connector.getMetadata().setName(name);
        connector.getMetadata().getLabels().put("strimzi.io/cluster", kafkaConnect.getMetadata().getName());

        kafkaConnectorOperation().createOrReplace(connector);
        waitForKafkaConnector(connector.getMetadata().getName());
    }

    /**
     * Waits until connector is properly deployed.
     * Note: works only for CR deployment
     * @param name name of the connector
     * @throws InterruptedException on wait error
     * @throws IllegalArgumentException when deployment doesn't use custom resources
     */
    public KafkaConnector waitForKafkaConnector(String name) throws InterruptedException {
        if (!useConnectorResources) {
            throw new IllegalStateException("Unable to wait for connector, deployment doesn't use custom resources.");
        }
        return kafkaConnectorOperation().withName(name).waitUntilCondition(WaitConditions::kafkaReadyCondition, 5, MINUTES);
    }

    private NonNamespaceOperation<KafkaConnector, KafkaConnectorList, DoneableKafkaConnector, Resource<KafkaConnector, DoneableKafkaConnector>> kafkaConnectorOperation() {
        return Crds.kafkaConnectorOperation(ocp).inNamespace(project);
    }

    /**
     * Deletes Kafka connector with given name
     * @param name connector name
     * @throws IOException on request error
     */
    public void undeployConnector(String name) throws IOException {
        LOGGER.info("Undeploying kafka connector " + name);
        if (useConnectorResources) {
            undeployConnectorCr(name);
        }
        else {
            undeployConnectorJson(name);
        }
    }

    private void undeployConnectorJson(String name) throws IOException {
        if (apiRoute == null) {
            throw new IllegalStateException("KafkaConnect API was not exposed");
        }

        HttpUrl url = getApiURL().resolve("/connectors/" + name);
        Request r = new Request.Builder().url(url).delete().build();

        try (Response res = http.newCall(r).execute()) {
            if (!res.isSuccessful()) {
                LOGGER.error(res.request().url().toString());
                throw new RuntimeException("Connector deletion request returned status code '" + res.code() + "'");
            }
            LOGGER.info("Deleted kafka connector '" + name + "'");
        }
    }

    private void undeployConnectorCr(String name) {
        kafkaConnectorOperation().withName(name).delete();
        await()
                .atMost(1, MINUTES)
                .pollInterval(5, SECONDS)
                .until(() -> kafkaConnectorOperation().withName(name).get() == null);
    }

    public List<String> getConnectMetrics() throws IOException {
        OkHttpClient httpClient = new OkHttpClient();
        Request r = new Request.Builder().url(getMetricsURL()).get().build();

        try (Response res = httpClient.newCall(r).execute()) {
            String metrics = res.body().string();
            return Stream.of(metrics.split("\\r?\\n")).collect(Collectors.toList());
        }
    }

    /**
     * Waits until Snapshot phase of given connector completes
     * @param connectorName name of the connect
     * @param metricName name of the metric used to determine the state
     * @throws IOException on metric request error
     */
    public void waitForSnapshot(String connectorName, String metricName) throws IOException {
        List<String> metrics = getConnectMetrics();
        await()
                .atMost(5, TimeUnit.MINUTES)
                .pollInterval(10, TimeUnit.SECONDS)
                .until(() -> metrics.stream().anyMatch(s -> s.contains(metricName) && s.contains(connectorName)));
    }

    /**
     * Waits until snapshot phase of given MySQL connector completes
     * @param connectorName connector name
     * @throws IOException on metric request error
     */
    public void waitForMySqlSnapshot(String connectorName) throws IOException {
        LOGGER.info("Waiting for connector '" + connectorName + "' to finish snapshot");
        waitForSnapshot(connectorName, "debezium_mysql_connector_metrics_snapshotcompleted");
    }

    /**
     * Waits until snapshot phase of given PostgreSQL connector completes
     * @param connectorName connector name
     * @throws IOException on metric request error
     */
    public void waitForPostgreSqlSnapshot(String connectorName) throws IOException {
        waitForSnapshot(connectorName, "debezium_postgres_connector_metrics_snapshotcompleted");
    }

    /**
     * @return URL of Connect API endpoint
     */
    public HttpUrl getApiURL() {
        return new HttpUrl.Builder()
                .scheme("http")
                .host(apiRoute.getSpec().getHost())
                .build();
    }

    /**
     * @return URL of metrics endpoint
     */
    public HttpUrl getMetricsURL() {
        return new HttpUrl.Builder()
                .scheme("http")
                .host(metricsRoute.getSpec().getHost())
                .build();
    }

    /**
     * Undeploy this Kafka Connect cluster by deleted related KafkaConnect CR
     * @return true if the CR was found and deleted
     */
    public boolean undeployCluster() {
        return kafkaConnectOperation(ocp).delete(kafkaConnect);
    }
}
