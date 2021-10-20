/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.databases;

import org.testcontainers.containers.JdbcDatabaseContainer;

public abstract class AbstractDockerSqlDatabaseController<C extends JdbcDatabaseContainer<?>>
        extends AbstractDockerDatabaseController<C, SqlDatabaseClient>
        implements SqlDatabaseController {

    protected final C container;

    public AbstractDockerSqlDatabaseController(C container) {
        super(container);
        this.container = container;
    }

    @Override
    public String getPublicDatabaseUrl() {
        return container.getJdbcUrl();
    }

    @Override
    public SqlDatabaseClient getDatabaseClient(String username, String password) {
        return new SqlDatabaseClient(getPublicDatabaseUrl(), username, password);
    }

    @Override
    public void reload() {
        container.stop();
        container.start();
    }

}
