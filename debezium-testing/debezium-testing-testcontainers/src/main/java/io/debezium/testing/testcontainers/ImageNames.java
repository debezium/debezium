/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.testcontainers;

import org.testcontainers.utility.DockerImageName;

public final class ImageNames {

    private static final String POSTGRES_IMAGE = "quay.io/debezium/postgres:15";

    private static final String TIMESCALE_DB_IMAGE = "quay.io/debezium/timescale-timescaledb:latest-pg15";

    private static final String SINGLESTORE_IMAGE = "ghcr.io/singlestore-labs/singlestoredb-dev:0.2.77";

    // The explicit registry prevents the image name from being rewritten by the
    // hub.image.name.prefix substitution; the image is only published on Docker Hub.
    private static final String STARROCKS_IMAGE = "docker.io/starrocks/allin1-ubuntu:4.1.1";

    public static final String OFFICIAL_MONGODB_IMAGE = "quay.io/debezium/official-mongo:8.0";

    public static final DockerImageName POSTGRES_DOCKER_IMAGE_NAME = DockerImageName.parse(POSTGRES_IMAGE)
            .asCompatibleSubstituteFor("postgres");

    public static final DockerImageName TIMESCALE_DB_IMAGE_NAME = DockerImageName.parse(TIMESCALE_DB_IMAGE)
            .asCompatibleSubstituteFor("postgres");

    public static final DockerImageName SINGLESTORE_DOCKER_IMAGE_NAME = DockerImageName.parse(SINGLESTORE_IMAGE);

    public static final DockerImageName STARROCKS_DOCKER_IMAGE_NAME = DockerImageName.parse(STARROCKS_IMAGE);

    public static final DockerImageName OFFICIAL_DOCKER_IMAGE_NAME = DockerImageName.parse(OFFICIAL_MONGODB_IMAGE)
            .asCompatibleSubstituteFor("mongodb");
}
