/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.junit.jupiter;

import org.testcontainers.cockroachdb.CockroachContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;

import io.debezium.connector.jdbc.junit.TestHelper;
import io.debezium.testing.testcontainers.ImageNames;

/**
 * An implementation of {@link AbstractSinkDatabaseContextProvider} for CockroachDB.
 *
 * @author Virag Tripathi
 */
public class CockroachDbSinkDatabaseContextProvider extends AbstractSinkDatabaseContextProvider {

    // Registry-qualified so the image name is not rewritten by the configured image name substitutor.
    private static final DockerImageName IMAGE_NAME = ImageNames.COCKROACHDB_25_4_12_IMAGE_NAME;

    @SuppressWarnings("resource")
    public CockroachDbSinkDatabaseContextProvider() {
        super(SinkType.COCKROACHDB,
                new CockroachContainer(IMAGE_NAME)
                        .withDatabaseName("test")
                        .withNetwork(Network.SHARED)
                        .withEnv("TZ", TestHelper.getSinkTimeZone()));
    }

}
