/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.testcontainers;

import org.testcontainers.utility.DockerImageName;

public final class ImageNames {

    private static final String POSTGRES_IMAGE = "quay.io/debezium/postgres:15";

    private static final String TIMESCALE_DB_IMAGE = "timescale/timescaledb:latest-pg15";

    public static final DockerImageName POSTGRES_DOCKER_IMAGE_NAME = DockerImageName.parse(POSTGRES_IMAGE)
            .asCompatibleSubstituteFor("postgres");

    public static final DockerImageName TIMESCALE_DB_IMAGE_NAME = DockerImageName.parse(TIMESCALE_DB_IMAGE)
            .asCompatibleSubstituteFor("postgres");
}
