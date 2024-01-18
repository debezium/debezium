/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.fixtures.registry;

import static io.debezium.testing.system.tools.ConfigProperties.APICURIO_TLS_ENABLED;
import static io.debezium.testing.system.tools.ConfigProperties.OCP_PROJECT_DBZ;
import static io.debezium.testing.system.tools.ConfigProperties.OCP_PROJECT_REGISTRY;
import static io.debezium.testing.system.tools.kafka.builders.FabricKafkaConnectBuilder.KAFKA_CERT_SECRET;
import static io.debezium.testing.system.tools.kafka.builders.FabricKafkaConnectBuilder.KAFKA_CLIENT_CERT_SECRET;

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
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
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
        String kafkaAddress = kafkaController.getBootstrapAddress();

        if (APICURIO_TLS_ENABLED) {
            LOGGER.info("Apicurio TLS enabled");
            // Copy kafka certificate secrets created with strimzi to apicurio registry namespace
            prepareCertificateSecrets();

            kafkaAddress = kafkaController.getTlsBootstrapAddress();
        }

        FabricApicurioBuilder fabricBuilder = FabricApicurioBuilder
                .baseKafkaSql(kafkaAddress);

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

    private void prepareCertificateSecrets() {
        LOGGER.debug("Copying Kafka certificate secrets to Apicurio namespace");
        Secret kafkaSecret = ocp.secrets().inNamespace(OCP_PROJECT_DBZ).withName(KAFKA_CERT_SECRET).get();
        var kafkaClientSecretData = ocp.secrets().inNamespace(OCP_PROJECT_DBZ).withName(KAFKA_CLIENT_CERT_SECRET).get().getData();

        Secret secretNewMetadata = new SecretBuilder(kafkaSecret).withNewMetadata().withNamespace(OCP_PROJECT_REGISTRY).withName(KAFKA_CERT_SECRET).endMetadata()
                .build();
        Secret clientSecretRenamedCerts = new SecretBuilder().withNewMetadata().withNamespace(OCP_PROJECT_REGISTRY).withName(KAFKA_CLIENT_CERT_SECRET)
                .endMetadata().addToData("user.p12", kafkaClientSecretData.get("ca.p12")).addToData("user.password", kafkaClientSecretData.get("ca.password"))
                .build();
        ocp.secrets().inNamespace(OCP_PROJECT_REGISTRY).createOrReplace(secretNewMetadata);
        ocp.secrets().inNamespace(OCP_PROJECT_REGISTRY).createOrReplace(clientSecretRenamedCerts);
    }
}
