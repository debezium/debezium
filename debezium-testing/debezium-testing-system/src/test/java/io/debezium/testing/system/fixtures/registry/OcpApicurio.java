/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.fixtures.registry;

import static io.debezium.testing.system.tools.ConfigProperties.APICURIO_CRD_VERSION;
import static io.debezium.testing.system.tools.ConfigProperties.STRIMZI_CRD_VERSION;

import io.debezium.testing.system.fixtures.OcpClient;
import io.debezium.testing.system.fixtures.TestSetupFixture;
import io.debezium.testing.system.fixtures.kafka.KafkaRuntimeFixture;
import io.debezium.testing.system.tools.ConfigProperties;
import io.debezium.testing.system.tools.registry.ApicurioOperatorController;
import io.debezium.testing.system.tools.registry.OcpApicurioV1Controller;
import io.debezium.testing.system.tools.registry.OcpApicurioV1Deployer;
import io.debezium.testing.system.tools.registry.OcpApicurioV2Controller;
import io.debezium.testing.system.tools.registry.OcpApicurioV2Deployer;
import io.fabric8.openshift.client.OpenShiftClient;

import okhttp3.OkHttpClient;

public interface OcpApicurio
        extends TestSetupFixture, RegistrySetupFixture, RegistryRuntimeFixture,
        KafkaRuntimeFixture, OcpClient {

    String REGISTRY_V1_DEPLOYMENT_PATH = "/registry-resources/v1alpha1/030-registry-streams.yaml";
    String REGISTRY_V2_DEPLOYMENT_PATH = "/registry-resources/v1/010-registry-kafkasql.yaml";
    String REGISTRY_STORAGE_TOPIC_PATH = "/registry-resources/" + STRIMZI_CRD_VERSION + "/010-storage-topic.yaml";
    String REGISTRY_ID_TOPIC_PATH = "/registry-resources/" + STRIMZI_CRD_VERSION + "/020-global-id-topic.yaml";

    @Override
    default void setupRegistry() throws Exception {
        updateApicurioOperator(ConfigProperties.OCP_PROJECT_REGISTRY, getOcpClient());
        switch (APICURIO_CRD_VERSION) {
            case "v1":
                setupApicurioV2();
                break;
            case ("v1alpha1"):
                setupApicurioV1();
                break;
            default:
                throw new IllegalStateException("Unsupported Apicurio CR version " + APICURIO_CRD_VERSION);
        }
    }

    default void updateApicurioOperator(String project, OpenShiftClient ocp) {
        ApicurioOperatorController operatorController = ApicurioOperatorController.forProject(project, ocp);

        ConfigProperties.OCP_PULL_SECRET_PATH.ifPresent(operatorController::deployPullSecret);

        operatorController.updateOperator();
    }

    @Override
    default void teardownRegistry() {
        // no-op
        // Registry is reused across tests
    }

    default void setupApicurioV1() throws InterruptedException {
        OcpApicurioV1Deployer deployer = new OcpApicurioV1Deployer.Builder()
                .withOcpClient(getOcpClient())
                .withHttpClient(new OkHttpClient())
                .withProject(ConfigProperties.OCP_PROJECT_REGISTRY)
                .withYamlPath(REGISTRY_V1_DEPLOYMENT_PATH)
                .withTopicsYamlPath(REGISTRY_STORAGE_TOPIC_PATH, REGISTRY_ID_TOPIC_PATH)
                .withKafkaController(getKafkaController())
                .build();

        OcpApicurioV1Controller controller = deployer.deploy();
        setRegistryController(controller);
    }

    default void setupApicurioV2() throws InterruptedException {
        OcpApicurioV2Deployer deployer = new OcpApicurioV2Deployer.Builder()
                .withOcpClient(getOcpClient())
                .withHttpClient(new OkHttpClient())
                .withProject(ConfigProperties.OCP_PROJECT_REGISTRY)
                .withYamlPath(REGISTRY_V2_DEPLOYMENT_PATH)
                .build();

        OcpApicurioV2Controller controller = deployer.deploy();
        setRegistryController(controller);
    }
}
