/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cube;

import java.util.Map;
import java.util.stream.Collectors;

import org.arquillian.cube.docker.impl.client.config.CubeContainer;
import org.arquillian.cube.docker.impl.docker.DockerClientExecutor;
import org.arquillian.cube.spi.Cube;

/**
 * A skeleton implementation of {@link DatabaseCube}. Database-specific Cubes will typically inherit
 * form this class.
 * 
 * @author Jiri Pechanec
 *
 */
public abstract class AbstractDatabaseCube implements DatabaseCube {
    private final DockerClientExecutor docker;
    private final Cube<?> cube;

    public AbstractDatabaseCube(final Cube<?> cube, final DockerClientExecutor docker) {
        super();
        this.docker = docker;
        this.cube = cube;
    }

    @Override
    @SuppressWarnings("deprecation")
    public String getHost() {
        return docker.inspectContainer(cube.getId()).getNetworkSettings().getIpAddress();
    }

    @Override
    public Cube<?> getCube() {
        return cube;
    }

    @SuppressWarnings("unchecked")
    protected Map<String, String> getEnvVars() {
        return ((Cube<CubeContainer>) getCube()).configuration().getEnv().stream().map(x -> x.split("="))
                .collect(Collectors.toMap(x -> x[0], x -> x[1]));
    }
}
