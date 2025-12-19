/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.quarkus.debezium.mysql.deployment;

import static io.debezium.testing.testcontainers.testhelper.TestInfrastructureHelper.CI_CONTAINER_STARTUP_TIME;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.startupcheck.MinimumDurationRunningStartupCheckStrategy;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.ext.ScriptUtils;
import org.testcontainers.jdbc.JdbcDatabaseDelegate;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

public class MySqlResource {

    private static final MySQLContainer<?> MYSQL_CONTAINER = new MySQLContainer<>(
            DockerImageName.parse("container-registry.oracle.com/mysql/community-server:9.1")
                    .asCompatibleSubstituteFor("mysql"))
            .withNetworkAliases("mysql")
            .withUsername("mysqluser")
            .withPassword("mysqlpwd")
            .waitingFor(new LogMessageWaitStrategy()
                    .withRegEx(".*(MySQL) init process done. Ready for start up.(?s)(.*)(mysqld): ready for connections..*")
                    .withTimes(1)
                    .withStartupTimeout(Duration.of(CI_CONTAINER_STARTUP_TIME * 3, ChronoUnit.SECONDS)))
            .withStartupCheckStrategy(new MinimumDurationRunningStartupCheckStrategy(Duration.ofSeconds(10)))
            .withConnectTimeoutSeconds(300)
            .withCopyFileToContainer(MountableFile.forClasspathResource("mysql.cnf"), "/etc/mysql/conf.d/");

    public void start() throws Exception {
        MYSQL_CONTAINER.start();

        // With container started, give the 'mysqluser' all the needed grants
        MYSQL_CONTAINER.execInContainer("mysql",
                "-uroot",
                "-p" + MYSQL_CONTAINER.getPassword(),
                "-e",
                """
                            CREATE DATABASE inventory;
                            GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'mysqluser'@'%';
                            GRANT ALL PRIVILEGES ON inventory.* TO 'mysqluser'@'%';
                        """);

        // With all grants assigned, execute the init script.
        ScriptUtils.runInitScript(new JdbcDatabaseDelegate(MYSQL_CONTAINER, ""), "initialize-mysql-database.sql");

        System.setProperty("MYSQL_JDBC", MYSQL_CONTAINER.getJdbcUrl());
        System.setProperty("MYSQL_PASSWORD", MYSQL_CONTAINER.getPassword());
        System.setProperty("MYSQL_USERNAME", MYSQL_CONTAINER.getUsername());
    }

    public void stop() {
        MYSQL_CONTAINER.stop();
    }
}
