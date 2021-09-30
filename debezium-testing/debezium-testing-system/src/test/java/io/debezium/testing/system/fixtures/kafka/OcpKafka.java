/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.fixtures.kafka;

import static io.debezium.testing.system.tools.ConfigProperties.STRIMZI_CRD_VERSION;

import java.util.Arrays;
import java.util.stream.Collectors;

import io.debezium.testing.system.fixtures.OcpClient;
import io.debezium.testing.system.tools.ConfigProperties;
import io.debezium.testing.system.tools.kafka.KafkaConnectController;
import io.debezium.testing.system.tools.kafka.KafkaController;
import io.debezium.testing.system.tools.kafka.OcpKafkaConnectController;
import io.debezium.testing.system.tools.kafka.OcpKafkaConnectDeployer;
import io.debezium.testing.system.tools.kafka.OcpKafkaDeployer;
import io.debezium.testing.system.tools.kafka.StrimziOperatorController;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.openshift.client.OpenShiftClient;

import okhttp3.OkHttpClient;

public interface OcpKafka extends KafkaSetupFixture, KafkaRuntimeFixture, OcpClient {
    // Kafka resources
    String KAFKA = "/kafka-resources/" + STRIMZI_CRD_VERSION + "/010-kafka.yaml";
    String KAFKA_CONNECT_S2I_LOGGING = "/kafka-resources/" + STRIMZI_CRD_VERSION + "/020-kafka-connect-cfg.yaml";
    String KAFKA_CONNECT_S2I = "/kafka-resources/" + STRIMZI_CRD_VERSION + "/021-kafka-connect.yaml";

    @Override
    default void setupKafka() throws Exception {
        OpenShiftClient ocp = getOcpClient();
        KafkaController controller = deployKafkaCluster(ocp);
        KafkaConnectController connectController = deployKafkaConnectCluster(ocp);

        setKafkaController(controller);
        setKafkaConnectController(connectController);
    }

    @Override
    default void teardownKafka() throws Exception {
        // no-op
        // kafka is reused across tests
    }

    default KafkaController deployKafkaCluster(OpenShiftClient ocp) throws Exception {
        updateKafkaOperator(ConfigProperties.OCP_PROJECT_DBZ, ocp);

        OcpKafkaDeployer kafkaDeployer = new OcpKafkaDeployer.Builder()
                .withOcpClient(ocp)
                .withHttpClient(new OkHttpClient())
                .withProject(ConfigProperties.OCP_PROJECT_DBZ)
                .withYamlPath(KAFKA)
                .build();

        return kafkaDeployer.deploy();
    }

    default KafkaConnectController deployKafkaConnectCluster(OpenShiftClient ocp) throws InterruptedException {
        OcpKafkaConnectDeployer connectDeployer = new OcpKafkaConnectDeployer.Builder()
                .withOcpClient(ocp)
                .withHttpClient(new OkHttpClient())
                .withProject(ConfigProperties.OCP_PROJECT_DBZ)
                .withYamlPath(KAFKA_CONNECT_S2I)
                .withCfgYamlPath(KAFKA_CONNECT_S2I_LOGGING)
                .withConnectorResources(ConfigProperties.STRIMZI_OPERATOR_CONNECTORS)
                .build();

        OcpKafkaConnectController controller = connectDeployer.deploy();
        controller.allowServiceAccess();
        controller.exposeApi();
        controller.exposeMetrics();

        return controller;
    }

    default void updateKafkaOperator(String project, OpenShiftClient ocp) {
        StrimziOperatorController operatorController = StrimziOperatorController.forProject(project, ocp);

        operatorController.setLogLevel("DEBUG");
        operatorController.setAlwaysPullPolicy();
        operatorController.setOperandAlwaysPullPolicy();
        operatorController.setSingleReplica();

        ConfigProperties.OCP_PULL_SECRET_PATHS.ifPresent(paths -> {
            String secrets = Arrays.stream(paths.split(","))
                    .map(operatorController::deployPullSecret)
                    .map(Secret::getMetadata)
                    .map(ObjectMeta::getName)
                    .peek(operatorController::setImagePullSecret)
                    .collect(Collectors.joining(","));

            operatorController.setOperandImagePullSecrets(secrets);
        });

        operatorController.updateOperator();
    }
}
