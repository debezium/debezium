/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cube;

import org.arquillian.cube.docker.impl.docker.DockerClientExecutor;
import org.arquillian.cube.spi.Cube;

/**
 * Creates a {@link DatabaseCube} implementation for every supported database. This interface should be implemented
 * in each connector.
 *
 * @author Jiri Pechanec
 *
 */
public interface DatabaseCubeFactory {
    public DatabaseCube createDatabaseCube(final Cube<?> cube, final DockerClientExecutor docker);
}
