/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cube;

import org.arquillian.cube.spi.Cube;

import io.debezium.config.Configuration.Builder;

/**
 * This interface encapsulates a Cube that represents an instance of databse
 * against which the DBZ tests are running.
 *
 * @author Jiri Pechanec
 *
 */
public interface DatabaseCube {
    /**
     * Provides a {@link io.debezium.config.Configuration.Builder} to an instance of DBZ test database and
     * pre-configures connection parameters extracted from Cube configuration.
     * <ul>
     * <li>Host</li>
     * <li>port</li>
     * <li>username</li>
     * <li>password</li>
     * </ul>
     *
     * @return pre-configured configuration builder
     */
    public Builder configuration();

    /**
     * Network interface on which the database cube is listening
     *
     * @return IP address in string representation
     */
    public String getHost();

    /**
     * Port on which the database cube is listening
     *
     * @return port number
     */
    public int getPort();

    /**
     * Cube representing the database
     *
     * @return cube
     */
    public Cube<?> getCube();
}
