/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.fixtures.registry;

import io.debezium.testing.system.fixtures.OcpClient;
import io.debezium.testing.system.fixtures.TestSetupFixture;
import io.debezium.testing.system.fixtures.kafka.KafkaRuntimeFixture;
import io.debezium.testing.system.tools.ConfigProperties;
import io.debezium.testing.system.tools.registry.ApicurioOperatorController;
import io.debezium.testing.system.tools.registry.OcpApicurioV2Controller;
import io.debezium.testing.system.tools.registry.OcpApicurioV2Deployer;
import io.fabric8.openshift.client.OpenShiftClient;

import okhttp3.OkHttpClient;

public interface OcpApicurio
        extends TestSetupFixture, RegistrySetupFixture, RegistryRuntimeFixture,
        KafkaRuntimeFixture, OcpClient {

    String REGISTRY_V2_DEPLOYMENT_PATH = "/registry-resources/v1/010-registry-kafkasql.yaml";

    @Override
    default void setupRegistry() throws Exception {
        updateApicurioOperator(ConfigProperties.OCP_PROJECT_REGISTRY, getOcpClient());
        setupApicurioV2();
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
