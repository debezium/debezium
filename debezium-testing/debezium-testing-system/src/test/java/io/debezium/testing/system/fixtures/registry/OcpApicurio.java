/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.fixtures.registry;

import static io.debezium.testing.system.tools.ConfigProperties.OCP_PROJECT_REGISTRY;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.extension.ExtensionContext;

import io.debezium.testing.system.assertions.AvroKafkaAssertions;
import io.debezium.testing.system.assertions.KafkaAssertions;
import io.debezium.testing.system.tools.ConfigProperties;
import io.debezium.testing.system.tools.kafka.KafkaController;
import io.debezium.testing.system.tools.registry.ApicurioOperatorController;
import io.debezium.testing.system.tools.registry.OcpApicurioController;
import io.debezium.testing.system.tools.registry.OcpApicurioDeployer;
import io.debezium.testing.system.tools.registry.RegistryController;
import io.debezium.testing.system.tools.registry.builders.FabricApicurioBuilder;
import io.fabric8.openshift.client.OpenShiftClient;

import fixture5.TestFixture;
import fixture5.annotations.FixtureContext;
import okhttp3.OkHttpClient;

@FixtureContext(requires = { OpenShiftClient.class, KafkaController.class }, provides = { RegistryController.class }, overrides = { KafkaAssertions.class })
public class OcpApicurio extends TestFixture {

    private final OpenShiftClient ocp;
    private final KafkaController kafkaController;
    private final String project;

    public OcpApicurio(@NotNull ExtensionContext.Store store) {
        super(store);
        this.ocp = retrieve(OpenShiftClient.class);
        this.kafkaController = retrieve(KafkaController.class);
        this.project = OCP_PROJECT_REGISTRY;
    }

    @Override
    public void setup() throws Exception {
        updateApicurioOperator();

        FabricApicurioBuilder fabricBuilder = FabricApicurioBuilder
                .baseKafkaSql(kafkaController.getBootstrapAddress());

        OcpApicurioDeployer deployer = new OcpApicurioDeployer(OCP_PROJECT_REGISTRY, fabricBuilder, ocp, new OkHttpClient());

        OcpApicurioController controller = deployer.deploy();
        store(RegistryController.class, controller);
        store(KafkaAssertions.class, new AvroKafkaAssertions(kafkaController.getDefaultConsumerProperties()));
    }

    @Override
    public void teardown() {

    }

    private void updateApicurioOperator() {
        ApicurioOperatorController operatorController = ApicurioOperatorController.forProject(project, ocp);

        ConfigProperties.OCP_PULL_SECRET_PATH.ifPresent(operatorController::deployPullSecret);

        operatorController.updateOperator();
    }
}
