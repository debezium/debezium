/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.fixtures.kafka;

import static io.debezium.testing.system.tools.ConfigProperties.STRIMZI_CRD_VERSION;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.extension.ExtensionContext;

import io.debezium.testing.system.assertions.KafkaAssertions;
import io.debezium.testing.system.assertions.PlainKafkaAssertions;
import io.debezium.testing.system.tools.ConfigProperties;
import io.debezium.testing.system.tools.artifacts.OcpArtifactServerDeployer;
import io.debezium.testing.system.tools.kafka.KafkaConnectController;
import io.debezium.testing.system.tools.kafka.KafkaController;
import io.debezium.testing.system.tools.kafka.OcpKafkaConnectController;
import io.debezium.testing.system.tools.kafka.OcpKafkaConnectDeployer;
import io.debezium.testing.system.tools.kafka.OcpKafkaController;
import io.debezium.testing.system.tools.kafka.OcpKafkaDeployer;
import io.debezium.testing.system.tools.kafka.StrimziOperatorController;
import io.debezium.testing.system.tools.kafka.builders.kafkaconnect.OcpKafkaConnectBuilder;
import io.fabric8.openshift.client.OpenShiftClient;

import fixture5.TestFixture;
import fixture5.annotations.FixtureContext;
import okhttp3.OkHttpClient;

@FixtureContext(requires = { OpenShiftClient.class }, provides = { KafkaController.class, KafkaConnectController.class, KafkaAssertions.class })
public class OcpKafka extends TestFixture {

    private final OpenShiftClient ocp;
    private final String project;
    // Kafka resources
    String KAFKA_CONNECT_LOGGING = "/kafka-resources/" + STRIMZI_CRD_VERSION + "/020-kafka-connect-cfg.yaml";
    // Artifact Server resources
    String ARTIFACT_SERVER_DEPLOYMENT = "/artifact-server/010-deployment.yaml";
    String ARTIFACT_SERVER_SERVICE = "/artifact-server/020-service.yaml";

    public OcpKafka(@NotNull ExtensionContext.Store store) {
        super(store);
        this.ocp = retrieve(OpenShiftClient.class);
        this.project = ConfigProperties.OCP_PROJECT_DBZ;
    }

    @Override
    public void setup() {
        try {
            StrimziOperatorController operatorController = updateKafkaOperator();

            deployKafkaCluster(operatorController);
            deployConnectCluster(operatorController);
        }
        catch (Exception e) {
            throw new IllegalStateException("Error while setting up Kafka", e);
        }
    }

    @Override
    public void teardown() {
        // no-op: kafka is reused across tests
    }

    private void deployKafkaCluster(StrimziOperatorController operatorController) throws Exception {
        OcpKafkaDeployer kafkaDeployer = new OcpKafkaDeployer.Builder()
                .withOcpClient(ocp)
                .withHttpClient(new OkHttpClient())
                .withProject(project)
                .withOperatorController(operatorController)
                .build();

        OcpKafkaController controller = kafkaDeployer.deploy();
        store(KafkaController.class, controller);
        store(KafkaAssertions.class, new PlainKafkaAssertions(controller.getDefaultConsumerProperties()));
    }

    private void deployConnectCluster(StrimziOperatorController operatorController) throws InterruptedException {
        OcpKafkaConnectBuilder kafkaConnectBuilder = new OcpKafkaConnectBuilder();

        if (ConfigProperties.STRIMZI_KC_BUILD) {
            kafkaConnectBuilder = kafkaConnectBuilder.withKcBuildSetup();
            deployArtifactServer();
        }
        else {
            kafkaConnectBuilder = kafkaConnectBuilder.withNonKcBuildSetup();
        }

        OcpKafkaConnectDeployer connectDeployer = new OcpKafkaConnectDeployer.Builder()
                .withOcpClient(ocp)
                .withHttpClient(new OkHttpClient())
                .withProject(project)
                .withKafkaConnectBuilder(kafkaConnectBuilder)
                .withCfgYamlPath(KAFKA_CONNECT_LOGGING)
                .withConnectorResources(ConfigProperties.STRIMZI_OPERATOR_CONNECTORS)
                .withOperatorController(operatorController)
                .build();

        OcpKafkaConnectController controller = connectDeployer.deploy();
        controller.allowServiceAccess();
        controller.exposeApi();
        controller.exposeMetrics();

        store(KafkaConnectController.class, controller);
    }

    private void deployArtifactServer() throws InterruptedException {
        OcpArtifactServerDeployer deployer = new OcpArtifactServerDeployer.Builder()
                .withOcpClient(ocp)
                .withHttpClient(new OkHttpClient())
                .withProject(project)
                .withDeployment(ARTIFACT_SERVER_DEPLOYMENT)
                .withService(ARTIFACT_SERVER_SERVICE)
                .build();

        deployer.deploy();
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
