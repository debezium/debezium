/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.registry.builders;

import static io.debezium.testing.system.tools.ConfigProperties.APICURIO_TLS_ENABLED;
import static io.debezium.testing.system.tools.kafka.builders.FabricKafkaConnectBuilder.KAFKA_CERT_SECRET;
import static io.debezium.testing.system.tools.kafka.builders.FabricKafkaConnectBuilder.KAFKA_CLIENT_CERT_SECRET;

import io.apicurio.registry.operator.api.v1.model.ApicurioRegistry;
import io.apicurio.registry.operator.api.v1.model.ApicurioRegistryBuilder;
import io.apicurio.registry.operator.api.v1.model.ApicurioRegistrySpecConfigurationKafkaSecurity;
import io.apicurio.registry.operator.api.v1.model.ApicurioRegistrySpecConfigurationKafkaSecurityBuilder;
import io.apicurio.registry.operator.api.v1.model.ApicurioRegistrySpecConfigurationKafkaSecurityTlsBuilder;
import io.debezium.testing.system.tools.ConfigProperties;
import io.debezium.testing.system.tools.fabric8.FabricBuilderWrapper;

public class FabricApicurioBuilder
        extends FabricBuilderWrapper<FabricApicurioBuilder, ApicurioRegistryBuilder, ApicurioRegistry> {

    private static final String DEFAULT_PERSISTENCE_TYPE = "kafkasql";

    protected FabricApicurioBuilder(ApicurioRegistryBuilder builder) {
        super(builder);
    }

    @Override
    public ApicurioRegistry build() {
        return builder.build();
    }

    private static FabricApicurioBuilder base() {
        ApicurioRegistryBuilder builder = new ApicurioRegistryBuilder()
                .withNewMetadata()
                .withName("debezium-registry")
                .endMetadata()
                .withNewSpec()
                .endSpec();

        return new FabricApicurioBuilder(builder);
    }

    public static FabricApicurioBuilder baseKafkaSql(String bootstrap) {
        return base().withKafkaSqlConfiguration(bootstrap);
    }

    public FabricApicurioBuilder withKafkaSqlConfiguration(String bootstrap) {
        builder.editSpec()
                .withNewConfiguration()
                .withLogLevel(ConfigProperties.APICURIO_LOG_LEVEL)
                .withPersistence(DEFAULT_PERSISTENCE_TYPE)
                .withNewKafkasql()
                .withBootstrapServers(bootstrap)
                .endKafkasql()
                .endConfiguration()
                .endSpec();

        if (APICURIO_TLS_ENABLED) {
            withTls();
        }

        return self();
    }

    public FabricApicurioBuilder withTls() {
        builder.editSpec()
                .editConfiguration()
                .editKafkasql()
                .withSecurity(getTlsSpec())
                .endKafkasql()
                .endConfiguration()
                .endSpec();
        return self();
    }

    private ApicurioRegistrySpecConfigurationKafkaSecurity getTlsSpec() {
        return new ApicurioRegistrySpecConfigurationKafkaSecurityBuilder()
                .withTls(
                        new ApicurioRegistrySpecConfigurationKafkaSecurityTlsBuilder()
                                .withKeystoreSecretName(KAFKA_CLIENT_CERT_SECRET)
                                .withTruststoreSecretName(KAFKA_CERT_SECRET)
                                .build())
                .build();
    }
}
