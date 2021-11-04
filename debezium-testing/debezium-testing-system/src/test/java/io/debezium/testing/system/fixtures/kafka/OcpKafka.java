/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.fixtures.kafka;

import static io.debezium.testing.system.tools.ConfigProperties.STRIMZI_CRD_VERSION;

import io.debezium.testing.system.fixtures.OcpClient;
import io.debezium.testing.system.tools.ConfigProperties;
import io.debezium.testing.system.tools.artifacts.OcpArtifactServerController;
import io.debezium.testing.system.tools.artifacts.OcpArtifactServerDeployer;
import io.debezium.testing.system.tools.kafka.KafkaConnectController;
import io.debezium.testing.system.tools.kafka.KafkaController;
import io.debezium.testing.system.tools.kafka.OcpKafkaConnectController;
import io.debezium.testing.system.tools.kafka.OcpKafkaConnectDeployer;
import io.debezium.testing.system.tools.kafka.OcpKafkaDeployer;
import io.debezium.testing.system.tools.kafka.StrimziOperatorController;
import io.fabric8.openshift.client.OpenShiftClient;

import okhttp3.OkHttpClient;

public interface OcpKafka extends KafkaSetupFixture, KafkaRuntimeFixture, OcpClient {
    // Kafka resources
    String KAFKA = "/kafka-resources/" + STRIMZI_CRD_VERSION + "/010-kafka.yaml";
    String KAFKA_CONNECT_LOGGING = "/kafka-resources/" + STRIMZI_CRD_VERSION + "/020-kafka-connect-cfg.yaml";
    String KAFKA_CONNECT = "/kafka-resources/" + STRIMZI_CRD_VERSION + "/021-kafka-connect.yaml";
    String KAFKA_CONNECT_BUILD = "/kafka-resources/" + STRIMZI_CRD_VERSION + "/121-kafka-connect-build.yaml";

    String ARTIFACT_SERVER_DEPLOYMENT = "/artifact-server/010-deployment.yaml";
    String ARTIFACT_SERVER_SERVICE = "/artifact-server/020-service.yaml";

    @Override
    default void setupKafka() throws Exception {
        OpenShiftClient ocp = getOcpClient();
        StrimziOperatorController operatorController = updateKafkaOperator(ConfigProperties.OCP_PROJECT_DBZ, ocp);
        KafkaController controller = deployKafkaCluster(ocp, operatorController);
        KafkaConnectController connectController = deployKafkaConnectCluster(ocp, operatorController);

        setKafkaController(controller);
        setKafkaConnectController(connectController);
    }

    @Override
    default void teardownKafka() throws Exception {
        // no-op
        // kafka is reused across tests
    }

    default KafkaController deployKafkaCluster(OpenShiftClient ocp,
                                               StrimziOperatorController operatorController)
            throws Exception {
        OcpKafkaDeployer kafkaDeployer = new OcpKafkaDeployer.Builder()
                .withOcpClient(ocp)
                .withHttpClient(new OkHttpClient())
                .withProject(ConfigProperties.OCP_PROJECT_DBZ)
                .withYamlPath(KAFKA)
                .withOperatorController(operatorController)
                .build();

        return kafkaDeployer.deploy();
    }

    default KafkaConnectController deployKafkaConnectCluster(OpenShiftClient ocp,
                                                             StrimziOperatorController operatorController)
            throws InterruptedException {
        String yamlDescriptor = KAFKA_CONNECT;

        if (ConfigProperties.STRIMZI_KC_BUILD) {
            yamlDescriptor = KAFKA_CONNECT_BUILD;
            deployArtifactServer(ocp);
        }

        OcpKafkaConnectDeployer connectDeployer = new OcpKafkaConnectDeployer.Builder()
                .withOcpClient(ocp)
                .withHttpClient(new OkHttpClient())
                .withProject(ConfigProperties.OCP_PROJECT_DBZ)
                .withYamlPath(yamlDescriptor)
                .withCfgYamlPath(KAFKA_CONNECT_LOGGING)
                .withConnectorResources(ConfigProperties.STRIMZI_OPERATOR_CONNECTORS)
                .withOperatorController(operatorController)
                .build();

        OcpKafkaConnectController controller = connectDeployer.deploy();
        controller.allowServiceAccess();
        controller.exposeApi();
        controller.exposeMetrics();

        return controller;
    }

    default void deployArtifactServer(OpenShiftClient ocp) throws InterruptedException {
        OcpArtifactServerDeployer deployer = new OcpArtifactServerDeployer.Builder()
                .withOcpClient(ocp)
                .withHttpClient(new OkHttpClient())
                .withProject(ConfigProperties.OCP_PROJECT_DBZ)
                .withDeployment(ARTIFACT_SERVER_DEPLOYMENT)
                .withService(ARTIFACT_SERVER_SERVICE)
                .build();

        OcpArtifactServerController controller = deployer.deploy();
    }

    default StrimziOperatorController updateKafkaOperator(String project, OpenShiftClient ocp) {
        StrimziOperatorController operatorController = StrimziOperatorController.forProject(project, ocp);

        operatorController.setLogLevel("DEBUG");
        operatorController.setAlwaysPullPolicy();
        operatorController.setOperandAlwaysPullPolicy();
        operatorController.setSingleReplica();

        ConfigProperties.OCP_PULL_SECRET_PATH.ifPresent(operatorController::deployPullSecret);

        operatorController.updateOperator();

        return operatorController;
    }
}
