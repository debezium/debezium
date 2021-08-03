/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.fixtures.registry;

import io.debezium.testing.system.fixtures.OcpClient;
import io.debezium.testing.system.fixtures.TestSetupFixture;
import io.debezium.testing.system.fixtures.connectors.ConnectorDecoratorFixture;
import io.debezium.testing.system.fixtures.kafka.KafkaRuntimeFixture;
import io.debezium.testing.system.tools.ConfigProperties;
import io.debezium.testing.system.tools.kafka.ConnectorConfigBuilder;
import io.debezium.testing.system.tools.registry.OcpApicurioV1Controller;
import io.debezium.testing.system.tools.registry.OcpApicurioV1Deployer;
import io.debezium.testing.system.tools.registry.RegistryController;
import okhttp3.OkHttpClient;

import static io.debezium.testing.system.tools.ConfigProperties.STRIMZI_CRD_VERSION;

public interface OcpApicurio
        extends TestSetupFixture, RegistrySetupFixture, RegistryRuntimeFixture,
        KafkaRuntimeFixture, OcpClient, ConnectorDecoratorFixture {

    String REGISTRY_V1_DEPLOYMENT_PATH = "/registry-resources/v1alpha1/030-registry-streams.yaml";
    String REGISTRY_STORAGE_TOPIC_PATH = "/registry-resources/" + STRIMZI_CRD_VERSION + "/010-storage-topic.yaml";
    String REGISTRY_ID_TOPIC_PATH = "/registry-resources/" + STRIMZI_CRD_VERSION + "/020-global-id-topic.yaml";

    @Override
    default void setupRegistry() throws Exception {
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

    @Override
    default void teardownRegistry() {
        getRegistryController().ifPresent(RegistryController::undeploy);
    }

    @Override
    default void decorateConnectorConfig(ConnectorConfigBuilder config) {
        config.addApicurioAvroSupport(
                getRegistryController()
                        .orElseThrow(() -> new IllegalStateException("No registry controller"))
                        .getRegistryApiAddress());
    }
}
