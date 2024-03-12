/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.kafka;

import static io.debezium.testing.system.tools.WaitConditions.scaled;
import static io.strimzi.api.kafka.Crds.kafkaConnectOperation;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.testing.system.tools.HttpUtils;
import io.debezium.testing.system.tools.OpenShiftUtils;
import io.debezium.testing.system.tools.WaitConditions;
import io.debezium.testing.system.tools.kafka.connectors.ConnectorDeployer;
import io.debezium.testing.system.tools.kafka.connectors.ConnectorMetricsReader;
import io.debezium.testing.system.tools.kafka.connectors.CustomResourceConnectorDeployer;
import io.debezium.testing.system.tools.kafka.connectors.JsonConnectorDeployer;
import io.debezium.testing.system.tools.kafka.connectors.RestPrometheusMetricReader;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicy;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyPort;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyPortBuilder;
import io.fabric8.openshift.api.model.Route;
import io.fabric8.openshift.client.OpenShiftClient;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.KafkaConnect;

import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;

/**
 * This class provides control over Kafka Connect instance deployed in OpenShift
 *
 * @author Jakub Cechacek
 */
public class OcpKafkaConnectController implements KafkaConnectController {
    private static final Logger LOGGER = LoggerFactory.getLogger(OcpKafkaConnectController.class);
    private static final int METRICS_PORT = 9404;

    private final OpenShiftClient ocp;
    private final OkHttpClient http;
    private final String project;
    private final StrimziOperatorController operatorController;
    private final OpenShiftUtils ocpUtils;
    private final HttpUtils httpUtils;
    private final String name;

    private KafkaConnect kafkaConnect;
    private Route apiRoute;
    private Route metricsRoute;
    private Service metricsService;

    public OcpKafkaConnectController(
                                     KafkaConnect kafkaConnect,
                                     StrimziOperatorController operatorController,
                                     OpenShiftClient ocp,
                                     OkHttpClient http) {
        this.kafkaConnect = kafkaConnect;
        this.name = kafkaConnect.getMetadata().getName();
        this.operatorController = operatorController;
        this.ocp = ocp;
        this.http = http;
        this.project = kafkaConnect.getMetadata().getNamespace();
        this.ocpUtils = new OpenShiftUtils(ocp);
        this.httpUtils = new HttpUtils(http);
    }

    /**
     * Disables Kafka Connect by scaling it to ZERO
     * <p>
     * NOTICE: cluster operator needs to be disabled first!
     */
    @Override
    public void disable() {
        LOGGER.info("Disabling KafkaConnect deployment (scaling to ZERO).");

        ocp.apps().deployments().inNamespace(project)
                .withName(name + "-connect")
                .scale(0);
        await()
                .atMost(scaled(30), SECONDS)
                .pollDelay(5, SECONDS)
                .pollInterval(3, SECONDS)
                .until(() -> ocp.pods().inNamespace(project).withLabel("strimzi.io/kind", "KafkaConnect").list().getItems().isEmpty());
    }

    /**
     * TODO: rewrite
     * Crashes Kafka Connect by force deleting all pods. Then it immediately scales its deployment to ZERO by calling {@link #disable()}
     * <p>
     * NOTICE: cluster operator is disabled once this method is called!
     */
    @Override
    public void destroy() {
        LOGGER.info("Force deleting all KafkaConnect pods.");

        KafkaConnect kafkaConnect = Crds.kafkaConnectOperation(ocp).inNamespace(project).withName(name).get();
        kafkaConnect.getSpec().setReplicas(0);
        Crds.kafkaConnectOperation(ocp).inNamespace(project).createOrReplace(kafkaConnect);

        await()
                .atMost(scaled(30), SECONDS)
                .pollDelay(5, SECONDS)
                .pollInterval(3, SECONDS)
                .until(() -> ocp.pods().inNamespace(project).withLabel("strimzi.io/kind", "KafkaConnect").list().getItems().isEmpty());
    }

    @Override
    public void restore() throws InterruptedException {
        KafkaConnect kafkaConnect = Crds.kafkaConnectOperation(ocp).inNamespace(project).withName(name).get();
        kafkaConnect.getSpec().setReplicas(1);
        Crds.kafkaConnectOperation(ocp).inNamespace(project).createOrReplace(kafkaConnect);
        waitForCluster();
    }

    /**
     * Wait until KafkaConnect instance is back and ready
     *
     * @throws InterruptedException
     */
    @Override
    public void waitForCluster() throws InterruptedException {
        LOGGER.info("Waiting for Kafka Connect cluster '" + name + "'");
        kafkaConnect = Crds.kafkaConnectOperation(ocp).inNamespace(project)
                .withName(name)
                .waitUntilCondition(WaitConditions::kafkaReadyCondition, scaled(5), MINUTES);
    }

    /**
     * Creates network policy allowing access to ports exposed by Kafka Connect
     *
     * @return
     */
    public NetworkPolicy allowServiceAccess() {
        LOGGER.info("Creating NetworkPolicy allowing public access to " + kafkaConnect.getMetadata().getName() + "'s services");

        Map<String, String> labels = new HashMap<>();
        labels.put("strimzi.io/cluster", kafkaConnect.getMetadata().getName());
        labels.put("strimzi.io/kind", "KafkaConnect");
        labels.put("strimzi.io/name", kafkaConnect.getMetadata().getName() + "-connect");

        List<NetworkPolicyPort> ports = Stream.of(8083, 8404, 9404)
                .map(IntOrString::new)
                .map(p -> new NetworkPolicyPortBuilder().withProtocol("TCP").withPort(p).build())
                .collect(Collectors.toList());

        return ocpUtils.createNetworkPolicy(project, kafkaConnect.getMetadata().getName() + "-allowed", labels, ports);
    }

    /**
     * Exposes a route for kafka connect API associated with given KafkaConnect resource
     *
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
     *
     * @return {@link Route} object
     */
    public Route exposeMetrics() {
        LOGGER.info("Exposing KafkaConnect metrics");

        String namePort = "tcp-prometheus";
        String name = kafkaConnect.getMetadata().getName() + "-connect-metrics";
        String nameApiSvc = kafkaConnect.getMetadata().getName() + "-connect-api";
        Service apiService = ocp.services().inNamespace(project).withName(nameApiSvc).get();
        Map<String, String> selector = apiService.getSpec().getSelector();
        Map<String, String> labels = apiService.getMetadata().getLabels();

        metricsService = ocpUtils.createService(project, name, namePort, METRICS_PORT, selector, labels);
        metricsRoute = ocpUtils.createRoute(project, name, name, namePort, labels);
        httpUtils.awaitApi(getMetricsURL());

        return metricsRoute;
    }

    /**
     * Deploys Kafka connector with given name and configuration via REST
     *
     * @param config connector config
     * @throws IOException or request error
     */
    @Override
    public void deployConnector(ConnectorConfigBuilder config) throws IOException, InterruptedException {
        LOGGER.info("Deploying connector " + config.getConnectorName());
        getConnectorDeployer().deploy(config);

    }

    private boolean hasConnectorResourcesEnabled() {
        Map<String, String> annotations = kafkaConnect.getMetadata().getAnnotations();
        return "true".equals(annotations.get("strimzi.io/use-connector-resources"));
    }

    private ConnectorDeployer getConnectorDeployer() {
        if (hasConnectorResourcesEnabled()) {
            return new CustomResourceConnectorDeployer(kafkaConnect, ocp);
        }
        else {
            return new JsonConnectorDeployer(getApiURL(), http);
        }
    }

    /**
     * Deletes Kafka connector with given name
     *
     * @param name connector name
     * @throws IOException on request error
     */
    @Override
    public void undeployConnector(String name) throws IOException {
        LOGGER.info("Undeploying kafka connector " + name);
        getConnectorDeployer().undeploy(name);
    }

    /**
     * @return URL of Connect API endpoint
     */
    @Override
    public HttpUrl getApiURL() {
        if (apiRoute == null) {
            throw new IllegalStateException("KafkaConnect API was not exposed");
        }

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
     *
     * @return true if the CR was found and deleted
     */
    @Override
    public boolean undeploy() {
        try {
            kafkaConnectOperation(ocp).resource(kafkaConnect).delete();
            kafkaConnectOperation(ocp)
                    .resource(kafkaConnect)
                    .waitUntilCondition(WaitConditions::resourceDeleted, scaled(1), MINUTES);
        }
        catch (Exception exception) {
            LOGGER.error("Kafka connect cluster was not deleted");
            return false;
        }
        return true;
    }

    @Override
    public ConnectorMetricsReader getMetricsReader() {
        return new RestPrometheusMetricReader(getMetricsURL());
    }
}
