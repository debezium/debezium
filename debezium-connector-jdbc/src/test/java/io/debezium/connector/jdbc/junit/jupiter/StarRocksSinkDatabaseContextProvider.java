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
 * An implementation of {@link AbstractSinkDatabaseContextProvider} for StarRocks.
 */
public class StarRocksSinkDatabaseContextProvider extends AbstractSinkDatabaseContextProvider {

    @SuppressWarnings("resource")
    public StarRocksSinkDatabaseContextProvider() {
        super(SinkType.STARROCKS, createContainer());
    }

    private static StarRocksContainer<?> createContainer() {
        final StarRocksContainer<?> container = new StarRocksContainer<>(ImageNames.STARROCKS_DOCKER_IMAGE_NAME)
                .withNetwork(Network.SHARED)
                .withEnv("TZ", TestHelper.getSinkTimeZone());
        if (TestHelper.isConnectionTimeZoneUsed()) {
            container.withUrlParam("connectionTimeZone", TestHelper.getSinkTimeZone());
        }
        return container;
    }
}
