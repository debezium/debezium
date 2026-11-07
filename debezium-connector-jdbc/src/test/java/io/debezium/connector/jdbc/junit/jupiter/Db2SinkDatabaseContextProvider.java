/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.junit.jupiter;

import org.testcontainers.containers.Db2Container;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;

import io.debezium.connector.jdbc.junit.TestHelper;
import io.debezium.testing.testcontainers.ImageNames;

/**
 * An implementation of {@link AbstractSinkDatabaseContextProvider} for Db2.
 *
 * @author Chris Cranford
 */
public class Db2SinkDatabaseContextProvider extends AbstractSinkDatabaseContextProvider {

    private static final DockerImageName IMAGE_NAME = ImageNames.DB2_11_5_9_IMAGE_NAME;

    @SuppressWarnings("resource")
    public Db2SinkDatabaseContextProvider() {
        super(SinkType.DB2, new Db2Container(IMAGE_NAME)
                .acceptLicense()
                .withNetwork(Network.SHARED)
                .withEnv("TZ", TestHelper.getSinkTimeZone()));
    }

}
