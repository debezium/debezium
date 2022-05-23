/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.fixtures.kafka;

import static io.debezium.testing.system.tools.ConfigProperties.STRIMZI_KC_BUILD;
import static io.debezium.testing.system.tools.ConfigProperties.STRIMZI_KC_IMAGE;
import static io.debezium.testing.system.tools.ConfigProperties.STRIMZI_OPERATOR_CONNECTORS;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.extension.ExtensionContext;

import io.debezium.testing.system.assertions.KafkaAssertions;
import io.debezium.testing.system.assertions.PlainKafkaAssertions;
import io.debezium.testing.system.tools.ConfigProperties;
import io.debezium.testing.system.tools.YAML;
import io.debezium.testing.system.tools.artifacts.OcpArtifactServerController;
import io.debezium.testing.system.tools.artifacts.OcpArtifactServerDeployer;
import io.debezium.testing.system.tools.kafka.KafkaConnectController;
import io.debezium.testing.system.tools.kafka.KafkaController;
import io.debezium.testing.system.tools.kafka.OcpKafkaConnectController;
import io.debezium.testing.system.tools.kafka.OcpKafkaConnectDeployer;
import io.debezium.testing.system.tools.kafka.OcpKafkaController;
import io.debezium.testing.system.tools.kafka.OcpKafkaDeployer;
import io.debezium.testing.system.tools.kafka.StrimziOperatorController;
import io.debezium.testing.system.tools.kafka.builders.FabricKafkaBuilder;
import io.debezium.testing.system.tools.kafka.builders.FabricKafkaConnectBuilder;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.openshift.client.OpenShiftClient;

import fixture5.TestFixture;
import fixture5.annotations.FixtureContext;
import okhttp3.OkHttpClient;

@FixtureContext(requires = { OpenShiftClient.class }, provides = { KafkaController.class, KafkaConnectController.class, KafkaAssertions.class })
public class OcpKafka extends TestFixture {

    private final OpenShiftClient ocp;
    private final String project;

    // Kafka resources
    String KAFKA_CONNECT_LOGGING = "/kafka-resources/020-kafka-connect-cfg.yaml";
    // Artifact Server resources
    String ARTIFACT_SERVER_DEPLOYMENT = "/artifact-server/010-deployment.yaml";
    String ARTIFACT_SERVER_SERVICE = "/artifact-server/020-service.yaml";

    public OcpKafka(@NotNull ExtensionContext.Store store) {
        super(store);
        this.ocp = retrieve(OpenShiftClient.class);
        this.project = ConfigProperties.OCP_PROJECT_DBZ;
    }

    @Override
    public void setup() throws Exception {
        StrimziOperatorController operatorController = updateKafkaOperator();

        OcpKafkaController kafkaController = deployKafkaCluster(operatorController);
        deployConnectCluster(operatorController, kafkaController);
    }

    @Override
    public void teardown() {
        // no-op: kafka is reused across tests
    }

    private OcpKafkaController deployKafkaCluster(StrimziOperatorController operatorController) throws Exception {
        FabricKafkaBuilder builder = FabricKafkaBuilder
                .base()
                .withPullSecret(operatorController.getPullSecret());

        OcpKafkaDeployer kafkaDeployer = new OcpKafkaDeployer(
                project, builder, operatorController, ocp, new OkHttpClient());

        OcpKafkaController controller = kafkaDeployer.deploy();
        store(KafkaController.class, controller);
        store(KafkaAssertions.class, new PlainKafkaAssertions(controller.getDefaultConsumerProperties()));

        return controller;
    }

    private void deployConnectCluster(StrimziOperatorController operatorController, OcpKafkaController kafkaController) throws Exception {
        ConfigMap configMap = YAML.fromResource(KAFKA_CONNECT_LOGGING, ConfigMap.class);

        FabricKafkaConnectBuilder builder = FabricKafkaConnectBuilder
                .base(kafkaController.getLocalBootstrapAddress())
                .withLoggingFromConfigMap(configMap)
                .withMetricsFromConfigMap(configMap)
                .withConnectorResources(STRIMZI_OPERATOR_CONNECTORS);

        if (STRIMZI_KC_BUILD) {
            OcpArtifactServerController artifactServerController = deployArtifactServer();
            builder.withBuild(artifactServerController);
        }
        else {
            builder.withImage(STRIMZI_KC_IMAGE);
        }

        builder.withPullSecret(operatorController.getPullSecret());

        OcpKafkaConnectDeployer connectDeployer = new OcpKafkaConnectDeployer(
                project, builder, configMap, operatorController, ocp, new OkHttpClient());

        OcpKafkaConnectController controller = connectDeployer.deploy();
        controller.allowServiceAccess();
        controller.exposeApi();
        controller.exposeMetrics();

        store(KafkaConnectController.class, controller);
    }

    private OcpArtifactServerController deployArtifactServer() throws Exception {
        OcpArtifactServerDeployer deployer = new OcpArtifactServerDeployer.Builder()
                .withOcpClient(ocp)
                .withHttpClient(new OkHttpClient())
                .withProject(project)
                .withDeployment(ARTIFACT_SERVER_DEPLOYMENT)
                .withService(ARTIFACT_SERVER_SERVICE)
                .build();

        return deployer.deploy();
    }

    private StrimziOperatorController updateKafkaOperator() {
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
