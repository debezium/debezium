/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.fixtures.databases.docker;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.containers.Network;

import io.debezium.testing.system.fixtures.databases.DatabaseFixture;
import io.debezium.testing.system.tools.databases.DatabaseController;

public abstract class DockerDatabaseFixture<T extends DatabaseController<?>> extends DatabaseFixture<T> {

    protected final Network network;

    public DockerDatabaseFixture(Class<T> controllerType, ExtensionContext.Store store) {
        super(controllerType, store);
        this.network = retrieve(Network.class);
    }
}
