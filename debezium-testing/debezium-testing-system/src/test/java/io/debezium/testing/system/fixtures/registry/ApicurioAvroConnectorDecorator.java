/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.fixtures.registry;

import static io.debezium.testing.system.tools.ConfigProperties.APICURIO_CRD_VERSION;

import io.debezium.testing.system.fixtures.TestSetupFixture;
import io.debezium.testing.system.fixtures.connectors.ConnectorDecoratorFixture;
import io.debezium.testing.system.tools.kafka.ConnectorConfigBuilder;

public interface ApicurioAvroConnectorDecorator
        extends TestSetupFixture, RegistryRuntimeFixture, ConnectorDecoratorFixture {

    @Override
    default void decorateConnectorConfig(ConnectorConfigBuilder config) {
        switch (APICURIO_CRD_VERSION) {
            case "v1":
                config.addApicurioV2AvroSupport(
                        getRegistryController()
                                .orElseThrow(() -> new IllegalStateException("No registry controller"))
                                .getRegistryApiAddress());
                break;
            case ("v1alpha1"):
                config.addApicurioV1AvroSupport(
                        getRegistryController()
                                .orElseThrow(() -> new IllegalStateException("No registry controller"))
                                .getRegistryApiAddress());
                break;
            default:
                throw new IllegalStateException("Unsupported Apicurio CR version " + APICURIO_CRD_VERSION);
        }
    }
}
