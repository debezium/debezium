/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.databases.mysql;

import org.testcontainers.containers.MySQLContainer;

import io.debezium.testing.system.tools.databases.AbstractDockerSqlDatabaseController;
import io.debezium.testing.system.tools.databases.docker.DBZMySQLContainer;

public class DockerMysqlController extends AbstractDockerSqlDatabaseController<DBZMySQLContainer<?>> implements MySqlMasterController {

    DockerMysqlController(DBZMySQLContainer<?> container) {
        super(container);
    }

    @Override
    public int getDatabasePort() {
        return MySQLContainer.MYSQL_PORT;
    }
}
