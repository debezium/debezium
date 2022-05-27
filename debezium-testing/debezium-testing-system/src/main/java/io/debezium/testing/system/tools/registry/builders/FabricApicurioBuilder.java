/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.registry.builders;

import io.apicurio.registry.operator.api.model.ApicurioRegistry;
import io.apicurio.registry.operator.api.model.ApicurioRegistryBuilder;
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
        builder
                .editSpec()
                .withNewConfiguration()
                .withLogLevel(ConfigProperties.APICURIO_LOG_LEVEL)
                .withPersistence(DEFAULT_PERSISTENCE_TYPE)
                .withNewKafkasql()
                .withBootstrapServers(bootstrap)
                .endKafkasql()
                .endConfiguration()
                .endSpec();

        return self();
    }
}
