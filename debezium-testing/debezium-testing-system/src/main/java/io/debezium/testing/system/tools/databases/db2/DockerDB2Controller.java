/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.databases.db2;

import static io.debezium.testing.system.tools.ConfigProperties.DATABASE_DB2_DBZ_PASSWORD;
import static io.debezium.testing.system.tools.ConfigProperties.DATABASE_DB2_DBZ_USERNAME;

import java.sql.Connection;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Db2Container;

import io.debezium.testing.system.tools.databases.AbstractDockerSqlDatabaseController;
import io.debezium.testing.system.tools.databases.SqlDatabaseClient;

public class DockerDB2Controller extends AbstractDockerSqlDatabaseController<Db2Container> {

    private static final Logger LOGGER = LoggerFactory.getLogger(OcpDB2Controller.class);

    public DockerDB2Controller(Db2Container container) {
        super(container);
    }

    @Override
    public int getDatabasePort() {
        return Db2Container.DB2_PORT;
    }

    @Override
    public void initialize() throws InterruptedException {
        LOGGER.info("Waiting until DB2 instance is ready");
        SqlDatabaseClient client = getDatabaseClient(DATABASE_DB2_DBZ_USERNAME, DATABASE_DB2_DBZ_PASSWORD);
        try (Connection connection = client.connect()) {
            LOGGER.info("Database connection established successfully!");
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
