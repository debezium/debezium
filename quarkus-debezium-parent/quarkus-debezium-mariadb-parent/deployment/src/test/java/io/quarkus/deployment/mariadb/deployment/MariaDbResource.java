/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.deployment.mariadb.deployment;

import org.testcontainers.containers.MariaDBContainer;
import org.testcontainers.ext.ScriptUtils;
import org.testcontainers.jdbc.JdbcDatabaseDelegate;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

public class MariaDbResource {

    private static final MariaDBContainer<?> MARIADB_CONTAINER = new MariaDBContainer<>(
            DockerImageName.parse("mirror.gcr.io/library/mariadb:11.4.3").asCompatibleSubstituteFor("mariadb"))
            .withUsername("mariadbuser")
            .withPassword("mariadbpw")
            .withCopyFileToContainer(MountableFile.forClasspathResource("mariadb.cnf"), "/etc/mysql/conf.d/")
            .withEnv("MARIADB_ROOT_PASSWORD", "mariadbpw");

    public void start() throws Exception {
        MARIADB_CONTAINER.start();

        MARIADB_CONTAINER.execInContainer("mariadb",
                "-uroot",
                "-p" + MARIADB_CONTAINER.getPassword(),
                "-e",
                """
                        CREATE DATABASE inventory;
                        GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT  ON *.* TO 'mariadbuser';
                        GRANT ALL PRIVILEGES ON inventory.* TO 'mariadbuser'@'%';
                        """);

        ScriptUtils.runInitScript(new JdbcDatabaseDelegate(MARIADB_CONTAINER, ""), "initialize-mariadb-database.sql");

        System.setProperty("MARIADB_JDBC", MARIADB_CONTAINER.getJdbcUrl());
        System.setProperty("MARIADB_PASSWORD", MARIADB_CONTAINER.getPassword());
        System.setProperty("MARIADB_USERNAME", MARIADB_CONTAINER.getUsername());
    }

    public void stop() {
        MARIADB_CONTAINER.stop();
    }
}
