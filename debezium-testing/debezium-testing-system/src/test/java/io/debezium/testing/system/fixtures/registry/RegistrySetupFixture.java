/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.fixtures.registry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface RegistrySetupFixture {
    Logger LOGGER = LoggerFactory.getLogger(RegistrySetupFixture.class);

    default void setupRegistry() throws Exception {
        LOGGER.info("Skipping registry deployment");
    }

    default void teardownRegistry() throws Exception {
        LOGGER.info("No registry to tear down");
    }
}
