/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.fixtures;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.containers.Network;

import fixture5.TestFixture;
import fixture5.annotations.FixtureContext;

@FixtureContext(provides = { Network.class })
public class DockerNetwork extends TestFixture {
    public DockerNetwork(@NotNull ExtensionContext.Store store) {
        super(store);
    }

    @Override
    public void setup() {
        store(Network.class, Network.newNetwork());
    }

    @Override
    public void teardown() {

    }
}
