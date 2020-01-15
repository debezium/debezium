/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.testcontainers;

import java.util.List;

import org.testcontainers.containers.JdbcDatabaseContainer;

/**
 * Specific configuration class that is auto-configured with given JDBCDatabaseContainer.
 */
public class JdbcConfiguration extends Configuration {

    private static final String CONNECTOR = "connector.class";
    private static final String HOSTNAME = "database.hostname";
    private static final String PORT = "database.port";
    private static final String USER = "database.user";
    private static final String PASSWORD = "database.password";
    private static final String DBNAME = "database.dbname";

    public static JdbcConfiguration fromJdbcContainer(JdbcDatabaseContainer<?> jdbcDatabaseContainer) {

        final JdbcConfiguration jdbcConfiguration = new JdbcConfiguration();

        jdbcConfiguration.with(HOSTNAME, jdbcDatabaseContainer.getContainerInfo().getConfig().getHostName());

        final List<Integer> exposedPorts = jdbcDatabaseContainer.getExposedPorts();
        jdbcConfiguration.with(PORT, exposedPorts.get(0));

        jdbcConfiguration.with(USER, jdbcDatabaseContainer.getUsername());
        jdbcConfiguration.with(PASSWORD, jdbcDatabaseContainer.getPassword());

        final String driverClassName = jdbcDatabaseContainer.getDriverClassName();
        jdbcConfiguration.with(CONNECTOR, ConnectorResolver.getConnectorByJdbcDriver(driverClassName));

        // This property is valid for all databases except MySQL
        if (!isMySQL(driverClassName)) {
            jdbcConfiguration.with(DBNAME, jdbcDatabaseContainer.getDatabaseName());
        }

        return jdbcConfiguration;
    }

    private static boolean isMySQL(String driverClassName) {
        return "com.mysql.cj.jdbc.Driver".equals(driverClassName) || "com.mysql.jdbc.Driver".equals(driverClassName);
    }
}
