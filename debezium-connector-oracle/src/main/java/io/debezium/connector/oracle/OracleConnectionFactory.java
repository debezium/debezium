/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import static io.debezium.connector.oracle.OracleConnectorConfig.CONNECTOR_ADAPTER;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import io.debezium.connector.oracle.OracleConnectorConfig.ConnectorAdapter;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection.ConnectionFactory;

public class OracleConnectionFactory implements ConnectionFactory {

    @Override
    public Connection connect(JdbcConfiguration config) throws SQLException {
        String hostName = config.getHostname();
        int port = config.getPort();
        String database = config.getDatabase();
        String user = config.getUser();
        String password = config.getPassword();

        final String url = "jdbc:oracle:" + getDriverType(config) + ":@" + hostName + ":" + port + "/" + database;

        return DriverManager.getConnection(url, user, password);
    }

    private static String getDriverType(JdbcConfiguration config) {
        return ConnectorAdapter.parse(config.getString(CONNECTOR_ADAPTER)).getDriverType();
    }
}
