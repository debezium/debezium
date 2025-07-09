/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import java.sql.Connection;
import java.sql.SQLException;

import io.debezium.jdbc.JdbcConfiguration;

/**
 * Read-only connection class, extends the oracle connection, for read-only databases.
 * @author Lucas Gazire
 */
public class ReadOnlyOracleConnection extends OracleConnection {

    public ReadOnlyOracleConnection(JdbcConfiguration config) {
        super(config);
    }

    @Override
    public synchronized Connection connection(boolean executeOnConnect) throws SQLException {
        Connection conn = super.connection(executeOnConnect);
        conn.setReadOnly(true);
        return conn;
    }

}
