/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.databases.postgresql;

import org.testcontainers.containers.PostgreSQLContainer;

import io.debezium.testing.system.tools.databases.AbstractDockerSqlDatabaseController;
import io.debezium.testing.system.tools.databases.docker.DBZPostgreSQLContainer;

public class DockerPostgreSqlController extends AbstractDockerSqlDatabaseController<DBZPostgreSQLContainer<?>> {

    DockerPostgreSqlController(DBZPostgreSQLContainer<?> container) {
        super(container);
    }

    @Override
    public int getDatabasePort() {
        return PostgreSQLContainer.POSTGRESQL_PORT;
    }
}
