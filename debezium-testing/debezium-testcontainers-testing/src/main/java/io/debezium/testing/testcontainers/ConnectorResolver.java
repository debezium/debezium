/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.testcontainers;

import java.util.HashMap;
import java.util.Map;

/**
 * Class that resolves the connector class from JDBC driver.
 */
public class ConnectorResolver {

    private static final Map<String, String> driverConnector = new HashMap<>();

    static {
        driverConnector.put("org.postgresql.Driver", "io.debezium.connector.postgresql.PostgresConnector");
        driverConnector.put("com.mysql.cj.jdbc.Driver", "io.debezium.connector.mysql.MySqlConnector");
        driverConnector.put("com.mysql.jdbc.Driver", "io.debezium.connector.mysql.MySqlConnector");
        driverConnector.put("com.microsoft.sqlserver.jdbc.SQLServerDriver",
                "io.debezium.connector.sqlserver.SqlServerConnector");
        driverConnector.put("oracle.jdbc.OracleDriver", "io.debezium.connector.oracle.OracleConnector");
    }

    public static String getConnectorByJdbcDriver(String jdbcDriver) {
        if (driverConnector.containsKey(jdbcDriver)) {
            return driverConnector.get(jdbcDriver);
        }

        throw new IllegalArgumentException(String.format("%s JDBC driver is passed but only %s are supported.", jdbcDriver, driverConnector.keySet().toString()));
    }

}
