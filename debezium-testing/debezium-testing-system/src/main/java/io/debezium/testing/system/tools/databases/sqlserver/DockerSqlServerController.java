/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.databases.sqlserver;

import static io.debezium.testing.system.tools.ConfigProperties.DATABASE_SQLSERVER_SA_PASSWORD;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.images.builder.Transferable;

import io.debezium.testing.system.tools.databases.AbstractDockerSqlDatabaseController;

public class DockerSqlServerController extends AbstractDockerSqlDatabaseController<MSSQLServerContainer<?>> {

    private static final String DB_INIT_SCRIPT_PATH = "/database-resources/sqlserver/inventory.sql";
    private static final String DB_INIT_SCRIPT_PATH_CONTAINER = "/opt/inventory.sql";
    private final Path initScript;

    public DockerSqlServerController(MSSQLServerContainer<?> container) {
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
        return MSSQLServerContainer.MS_SQL_SERVER_PORT;
    }

    @Override
    public void initialize() throws InterruptedException {
        try {
            container.copyFileToContainer(Transferable.of(Files.readAllBytes(initScript)), DB_INIT_SCRIPT_PATH_CONTAINER);
            container.execInContainer(
                    "/opt/mssql-tools18/bin/sqlcmd", "-U", "sa", "-P", DATABASE_SQLSERVER_SA_PASSWORD, "-i", DB_INIT_SCRIPT_PATH_CONTAINER, "-C", "-N", "o");
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
