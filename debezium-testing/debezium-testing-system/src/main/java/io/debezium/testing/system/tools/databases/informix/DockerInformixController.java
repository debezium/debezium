/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.databases.informix;

import java.sql.Connection;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.testing.system.tools.ConfigProperties;
import io.debezium.testing.system.tools.databases.AbstractDockerSqlDatabaseController;
import io.debezium.testing.system.tools.databases.SqlDatabaseClient;
import io.debezium.testing.testcontainers.InformixContainer;

public class DockerInformixController extends AbstractDockerSqlDatabaseController<InformixContainer> {

    private static final Logger LOGGER = LoggerFactory.getLogger(DockerInformixController.class);

    public DockerInformixController(InformixContainer container) {
        super(container);
    }

    @Override
    public int getDatabasePort() {
        return InformixContainer.INFORMIX_PORT;
    }

    @Override
    public void initialize() throws InterruptedException {
        LOGGER.info("Waiting until Informix instance is ready");
        SqlDatabaseClient client = getDatabaseClient(ConfigProperties.DATABASE_INFORMIX_DBZ_USERNAME, ConfigProperties.DATABASE_INFORMIX_DBZ_PASSWORD);
        try (Connection connection = client.connect()) {
            LOGGER.info("Database connection established successfully!");
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
