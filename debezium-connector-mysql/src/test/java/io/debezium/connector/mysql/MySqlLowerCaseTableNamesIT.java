/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.time.Duration;
import java.util.List;

import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

import io.debezium.config.Configuration;
import io.debezium.connector.binlog.BinlogLowerCaseTableNamesIT;

public class MySqlLowerCaseTableNamesIT extends BinlogLowerCaseTableNamesIT<MySqlConnector> implements MySqlCommon {

    private static final String IMAGE = System.getProperty("mysql.server.image", "container-registry.oracle.com/mysql/community-server:8.4");

    @Override
    protected JdbcDatabaseContainer<?> createContainer(List<String> serverOptions) {
        return new MySQLContainer(DockerImageName.parse(IMAGE).asCompatibleSubstituteFor("mysql"))
                .withUsername("root")
                .withPassword(ROOT_PASSWORD)
                // The Oracle images only allow root to connect from localhost by default
                .withEnv("MYSQL_ROOT_HOST", "%")
                .withCommand(serverOptions.toArray(String[]::new))
                .withStartupTimeout(Duration.ofMinutes(3));
    }

    @Override
    protected Configuration.Builder configureConnection(Configuration.Builder builder) {
        return builder
                .with(MySqlConnectorConfig.SSL_MODE, MySqlConnectorConfig.MySqlSecureConnectionMode.DISABLED)
                .with("driver.allowPublicKeyRetrieval", "true");
    }
}
