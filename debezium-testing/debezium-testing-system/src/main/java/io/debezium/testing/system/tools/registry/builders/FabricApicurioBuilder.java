/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.registry.builders;

import static io.debezium.testing.system.tools.ConfigProperties.APICURIO_TLS_ENABLED;
import static io.debezium.testing.system.tools.kafka.builders.FabricKafkaConnectBuilder.KAFKA_CERT_SECRET;
import static io.debezium.testing.system.tools.kafka.builders.FabricKafkaConnectBuilder.KAFKA_CLIENT_CERT_SECRET;

import io.apicurio.registry.operator.api.v1.ApicurioRegistry3;
import io.apicurio.registry.operator.api.v1.spec.KafkaSqlTLSSpec;
import io.apicurio.registry.operator.api.v1.spec.SecretKeyRef;
import io.apicurio.registry.operator.api.v1.spec.StorageType;
import io.fabric8.kubernetes.api.model.ObjectMeta;

public class FabricApicurioBuilder {

    private final ApicurioRegistry3 registry;

    protected FabricApicurioBuilder(ApicurioRegistry3 registry) {
        this.registry = registry;
    }

    public ApicurioRegistry3 build() {
        return registry;
    }

    private static FabricApicurioBuilder base() {
        ApicurioRegistry3 registry = new ApicurioRegistry3();
        ObjectMeta metadata = new ObjectMeta();
        metadata.setName("debezium-registry");
        registry.setMetadata(metadata);
        return new FabricApicurioBuilder(registry);
    }

    public static FabricApicurioBuilder baseKafkaSql(String bootstrap) {
        return base().withKafkaSqlConfiguration(bootstrap);
    }

    public FabricApicurioBuilder withKafkaSqlConfiguration(String bootstrap) {
        registry.withSpec().withApp().withStorage().setType(StorageType.KAFKASQL);
        registry.withSpec().withApp().withStorage().withKafkasql().setBootstrapServers(bootstrap);

        if (APICURIO_TLS_ENABLED) {
            withTls();
        }

        return this;
    }

    public FabricApicurioBuilder withTls() {
        KafkaSqlTLSSpec tlsSpec = KafkaSqlTLSSpec.builder()
                .keystoreSecretRef(SecretKeyRef.builder()
                        .name(KAFKA_CLIENT_CERT_SECRET)
                        .build())
                .truststoreSecretRef(SecretKeyRef.builder()
                        .name(KAFKA_CERT_SECRET)
                        .build())
                .build();

        registry.withSpec().withApp().withStorage().withKafkasql().setTls(tlsSpec);

        return this;
    }
}