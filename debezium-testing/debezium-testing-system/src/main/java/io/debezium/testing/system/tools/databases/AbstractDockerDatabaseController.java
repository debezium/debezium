/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.databases;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;

/**
 * Base for controllers of databases deployed as docker container
 * @author Jakub Cechacek
 */
public abstract class AbstractDockerDatabaseController<T extends GenericContainer<?>, C extends DatabaseClient<?, ?>>
        implements DatabaseController<C> {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractDockerDatabaseController.class);

    protected final T container;

    public AbstractDockerDatabaseController(T container) {
        this.container = container;
    }

    @Override
    public String getPublicDatabaseHostname() {
        return container.getHost();
    }

    @Override
    public int getPublicDatabasePort() {
        return container.getMappedPort(getDatabasePort());
    }

    @Override
    public String getDatabaseHostname() {
        return container.getNetworkAliases().get(0);
    }

    @Override
    public void reload() {
        container.stop();
        container.start();
    }
}
