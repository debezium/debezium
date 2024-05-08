/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.databases.oracle;

import static io.debezium.testing.system.tools.ConfigProperties.DATABASE_ORACLE_PASSWORD;
import static io.debezium.testing.system.tools.ConfigProperties.DATABASE_ORACLE_PDBNAME;
import static io.debezium.testing.system.tools.ConfigProperties.DATABASE_ORACLE_USERNAME;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.OracleContainer;
import org.testcontainers.images.builder.Transferable;

import io.debezium.testing.system.tools.databases.AbstractDockerSqlDatabaseController;

public class DockerOracleController extends AbstractDockerSqlDatabaseController<OracleContainer> {

    private static final String DB_INIT_SCRIPT_PATH = "/database-resources/oracle/inventory.sql";
    private static final String DB_INIT_SCRIPT_PATH_CONTAINER = "/home/oracle/inventory.sql";
    private static final Logger LOGGER = LoggerFactory.getLogger(DockerOracleController.class);

    private final Path initScript;

    public DockerOracleController(OracleContainer container) {
        super(container);
        try {
            initScript = Paths.get(getClass().getResource(DB_INIT_SCRIPT_PATH).toURI());
        }
        catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int getDatabasePort() {
        return 1521;
    }

    @Override
    public void initialize() throws InterruptedException {
        try {
            container.copyFileToContainer(Transferable.of(Files.readAllBytes(initScript)), DB_INIT_SCRIPT_PATH_CONTAINER);
            container.execInContainer(
                    "sqlplus", "-S",
                    DATABASE_ORACLE_USERNAME + "/" + DATABASE_ORACLE_PASSWORD + "@//localhost:1521/" + DATABASE_ORACLE_PDBNAME,
                    "@" + DB_INIT_SCRIPT_PATH_CONTAINER);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
