/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.junit.jupiter;

import org.testcontainers.containers.Network;

import io.debezium.connector.jdbc.junit.TestHelper;
import io.debezium.testing.testcontainers.ImageNames;

/**
 * An implementation of {@link AbstractSinkDatabaseContextProvider} for SingleStore.
 */
public class SingleStoreSinkDatabaseContextProvider extends AbstractSinkDatabaseContextProvider {

    @SuppressWarnings("resource")
    public SingleStoreSinkDatabaseContextProvider() {
        super(SinkType.SINGLESTORE, createContainer());
    }

    private static SingleStoreContainer<?> createContainer() {
        final SingleStoreContainer<?> container = new SingleStoreContainer<>(ImageNames.SINGLESTORE_DOCKER_IMAGE_NAME)
                .withNetwork(Network.SHARED)
                .withUsername("root")
                .withPassword("root")
                .withEnv("TZ", TestHelper.getSinkTimeZone());
        if (TestHelper.isConnectionTimeZoneUsed()) {
            container.withUrlParam("connectionTimeZone", TestHelper.getSinkTimeZone());
        }
        return container;
    }
}
