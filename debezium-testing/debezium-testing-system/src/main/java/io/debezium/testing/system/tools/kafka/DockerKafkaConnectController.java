/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.kafka;

import static io.debezium.testing.system.tools.WaitConditions.scaled;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.awaitility.Awaitility.await;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.dockerjava.api.command.InspectContainerResponse.ContainerState;

import io.debezium.testing.system.tools.kafka.connectors.ConnectorDeployer;
import io.debezium.testing.system.tools.kafka.connectors.ConnectorMetricsReader;
import io.debezium.testing.system.tools.kafka.connectors.JsonConnectorDeployer;
import io.debezium.testing.system.tools.kafka.connectors.RestPrometheusMetricReader;
import io.debezium.testing.system.tools.kafka.docker.KafkaConnectConainer;

import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;

/**
 * This class provides control over Kafka Connect instance deployed in OpenShift
 *
 * @author Jakub Cechacek
 */
public class DockerKafkaConnectController implements KafkaConnectController {

    private static final Logger LOGGER = LoggerFactory.getLogger(DockerKafkaConnectController.class);
    private final KafkaConnectConainer container;
    private final OkHttpClient http;

    public DockerKafkaConnectController(KafkaConnectConainer container, OkHttpClient http) {
        this.container = container;
        this.http = http;
    }

    public KafkaConnectConainer getContainer() {
        return container;
    }

    @Override
    public void disable() {
        container.getDockerClient().pauseContainerCmd(container.getContainerId()).exec();
    }

    @Override
    public void destroy() {
        // DockerClientFactory.instance().client().killContainerCmd(container.getContainerId()).exec();
        container.stop();
    }

    @Override
    public void restore() {
        if (container.getContainerId() == null) {
            container.start();
        }
        else {
            ContainerState state = container.getCurrentContainerInfo().getState();
            if (Boolean.TRUE.equals(state.getPaused())) {
                container.getDockerClient().unpauseContainerCmd(container.getContainerId()).exec();
            }
        }

    }

    @Override
    public void waitForCluster() {
        await()
                .atMost(scaled(5), MINUTES)
                .until(container::isRunning);
    }

    @Override
    public void deployConnector(ConnectorConfigBuilder config) throws IOException, InterruptedException {
        getConnectorDeployer().deploy(config);
    }

    private ConnectorDeployer getConnectorDeployer() {
        return new JsonConnectorDeployer(getApiURL(), http);
    }

    @Override
    public void undeployConnector(String name) throws IOException {
        getConnectorDeployer().undeploy(name);
    }

    @Override
    public HttpUrl getApiURL() {
        return new HttpUrl.Builder()
                .scheme("http")
                .host(container.getHost())
                .port(container.getMappedPort(KafkaConnectConainer.KAFKA_CONNECT_API_PORT))
                .build();
    }

    public HttpUrl getMetricsURL() {
        return new HttpUrl.Builder()
                .scheme("http")
                .host(container.getHost())
                .port(container.getMappedPort(KafkaConnectConainer.PROMETHEUS_METRICS_PORT))
                .addPathSegment("metrics")
                .build();
    }

    @Override
    public boolean undeploy() {
        container.stop();
        return !container.isRunning();
    }

    @Override
    public ConnectorMetricsReader getMetricsReader() {
        return new RestPrometheusMetricReader(getMetricsURL());
    }
}
