/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.fixtures.registry;

import io.debezium.testing.system.fixtures.TestSetupFixture;
import io.debezium.testing.system.fixtures.connectors.ConnectorDecoratorFixture;
import io.debezium.testing.system.tools.kafka.ConnectorConfigBuilder;

public interface ApicurioAvroConnectorDecorator
        extends TestSetupFixture, RegistryRuntimeFixture, ConnectorDecoratorFixture {

    @Override
    default void decorateConnectorConfig(ConnectorConfigBuilder config) {
        String registryApiAddress = getRegistryController()
                .orElseThrow(() -> new IllegalStateException("No registry controller"))
                .getRegistryApiAddress();
        config.addApicurioV2AvroSupport(registryApiAddress);
    }
}
