/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.testcontainers;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Class that resolves the connector class from JDBC driver.
 */
public class ConnectorResolver {

    private static final Map<String, String> CONNECTORS_BY_DRIVER;

    static {
        Map<String, String> tmp = new HashMap<>();

        tmp.put("org.postgresql.Driver", "io.debezium.connector.postgresql.PostgresConnector");
        tmp.put("com.mysql.cj.jdbc.Driver", "io.debezium.connector.mysql.MySqlConnector");
        tmp.put("com.mysql.jdbc.Driver", "io.debezium.connector.mysql.MySqlConnector");
        tmp.put("com.microsoft.sqlserver.jdbc.SQLServerDriver",
                "io.debezium.connector.sqlserver.SqlServerConnector");
        tmp.put("oracle.jdbc.OracleDriver", "io.debezium.connector.oracle.OracleConnector");
        tmp.put("com.ibm.db2.jcc.DB2Driver", "io.debezium.connector.db2.Db2Connector");
        tmp.put("org.mariadb.jdbc.Driver", "io.debezium.connector.mariadb.MariaDbConnector");

        CONNECTORS_BY_DRIVER = Collections.unmodifiableMap(tmp);
    }

    public static String getConnectorByJdbcDriver(String jdbcDriver) {
        if (CONNECTORS_BY_DRIVER.containsKey(jdbcDriver)) {
            return CONNECTORS_BY_DRIVER.get(jdbcDriver);
        }

        throw new IllegalArgumentException(String.format("%s JDBC driver is passed but only %s are supported.", jdbcDriver, CONNECTORS_BY_DRIVER.keySet().toString()));
    }
}
