/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.fixtures.registry;

import static io.debezium.testing.system.tools.ConfigProperties.OCP_PROJECT_REGISTRY;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.testing.system.assertions.AvroKafkaAssertions;
import io.debezium.testing.system.assertions.KafkaAssertions;
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

@FixtureContext(requires = { OpenShiftClient.class, KafkaController.class, ApicurioOperatorController.class }, provides = { RegistryController.class }, overrides = {
        KafkaAssertions.class })
public class OcpApicurio extends TestFixture {

    private final OpenShiftClient ocp;
    private final KafkaController kafkaController;
    private static final Logger LOGGER = LoggerFactory.getLogger(OcpApicurio.class);

    public OcpApicurio(@NotNull ExtensionContext.Store store) {
        super(store);
        this.ocp = retrieve(OpenShiftClient.class);
        this.kafkaController = retrieve(KafkaController.class);
    }

    @Override
    public void setup() throws Exception {
        FabricApicurioBuilder fabricBuilder = FabricApicurioBuilder
                .baseKafkaSql(kafkaController.getBootstrapAddress());

        OcpApicurioDeployer deployer = new OcpApicurioDeployer(OCP_PROJECT_REGISTRY, fabricBuilder, ocp, new OkHttpClient());

        OcpApicurioController controller = deployer.deploy();
        store(RegistryController.class, controller);
        store(KafkaAssertions.class, new AvroKafkaAssertions(kafkaController.getDefaultConsumerProperties()));
    }

    @Override
    public void teardown() {
        // no-op: apicurio is reused across tests
        LOGGER.debug("Skipping apicurio tear down");
    }
}
