/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.mariadb.MariaDBContainer;
import org.testcontainers.utility.DockerImageName;

import io.debezium.config.Configuration;
import io.debezium.connector.binlog.BinlogLowerCaseTableNamesIT;

public class MariaDbLowerCaseTableNamesIT extends BinlogLowerCaseTableNamesIT<MariaDbConnector> implements MariaDbCommon {

    private static final String IMAGE = System.getProperty("mariadb.server.image", "quay.io/debezium/official-mariadb:11.4");

    @Override
    protected JdbcDatabaseContainer<?> createContainer(List<String> serverOptions) {
        final List<String> options = new ArrayList<>(serverOptions);
        options.addAll(List.of("--log-bin=mysql-bin", "--binlog-format=ROW", "--server-id=1"));
        return new MariaDBContainer(DockerImageName.parse(IMAGE).asCompatibleSubstituteFor("mariadb"))
                .withUsername("root")
                .withPassword(ROOT_PASSWORD)
                .withCommand(options.toArray(String[]::new))
                .withStartupTimeout(Duration.ofMinutes(3));
    }

    @Override
    protected Configuration.Builder configureConnection(Configuration.Builder builder) {
        return builder.with(MariaDbConnectorConfig.SSL_MODE, MariaDbConnectorConfig.MariaDbSecureConnectionMode.DISABLE);
    }
}
