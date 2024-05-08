/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.kafka.connectors;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.testing.system.tools.WaitConditions;
import io.debezium.testing.system.tools.kafka.ConnectorConfigBuilder;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.openshift.client.OpenShiftClient;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.connect.KafkaConnect;
import io.strimzi.api.kafka.model.connector.KafkaConnector;
import io.strimzi.api.kafka.model.connector.KafkaConnectorList;

public class CustomResourceConnectorDeployer implements ConnectorDeployer {
    private static final Logger LOGGER = LoggerFactory.getLogger(CustomResourceConnectorDeployer.class);

    private final KafkaConnect kafkaConnect;
    private final String project;
    private final OpenShiftClient ocp;

    public CustomResourceConnectorDeployer(KafkaConnect kafkaConnect, OpenShiftClient ocp) {
        this.kafkaConnect = kafkaConnect;
        this.project = kafkaConnect.getMetadata().getNamespace();
        this.ocp = ocp;
    }

    @Override
    public void deploy(ConnectorConfigBuilder config) {
        LOGGER.info("Deploying connector CR for connector " + config.getConnectorName());
        KafkaConnector connector = config.getCustomResource();
        connector.getMetadata().getLabels().put("strimzi.io/cluster", kafkaConnect.getMetadata().getName());

        kafkaConnectorOperation().createOrReplace(connector);
        waitForKafkaConnector(config);
    }

    @Override
    public void undeploy(String name) {
        LOGGER.info("Undeploying kafka connector " + name);
        kafkaConnectorOperation().withName(name).delete();
        await()
                .atMost(1, MINUTES)
                .pollInterval(5, SECONDS)
                .until(() -> kafkaConnectorOperation().withName(name).get() == null);
    }

    private NonNamespaceOperation<KafkaConnector, KafkaConnectorList, Resource<KafkaConnector>> kafkaConnectorOperation() {
        return Crds.kafkaConnectorOperation(ocp).inNamespace(project);
    }

    /**
     * Waits until connector is properly deployed.
     *
     * @param config config of the connector
     */
    private void waitForKafkaConnector(ConnectorConfigBuilder config) {
        waitForKafkaConnector(config.getConnectorName());
    }

    /**
     * Waits until connector is properly deployed.
     *
     * @param name name of the connector
     */
    private void waitForKafkaConnector(String name) {
        LOGGER.info("Waiting for connector '" + name + "' to become ready.");
        kafkaConnectorOperation()
                .withName(name)
                .waitUntilCondition(WaitConditions::kafkaReadyCondition, 5, MINUTES);
        LOGGER.info("Connector '" + name + "' is ready.");
    }
}
