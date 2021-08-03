/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.fixtures.registry;

import java.util.Optional;

import io.debezium.testing.system.fixtures.TestSetupFixture;
import io.debezium.testing.system.tools.registry.RegistryController;

public interface RegistryRuntimeFixture extends TestSetupFixture {
    default Optional<RegistryController> getRegistryController() {
        return Optional.empty();
    }

    default void setRegistryController(RegistryController controller) {
        // no-op
    }
}
