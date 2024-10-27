/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Read-only connection class, extends the oracle connection, for read-only databases.
 * @author Lucas Gazire
 */
public class ReadOnlyOracleConnection extends OracleConnection {

    public ReadOnlyOracleConnection(JdbcConfiguration config) {
        super(config);
    }

    @Override
    public Connection connection() throws SQLException {
        Connection conn = super.connection();
        conn.setReadOnly(true);
        return conn;
    }

    @Override
    public synchronized Connection connection(boolean executOnConnect) throws SQLException {
        Connection conn = super.connection(executOnConnect);
        conn.setReadOnly(true);
        return conn;
    }

    @Override
    public JdbcConnection prepareUpdate(String stmt, StatementPreparer preparer) {
        throw new UnsupportedOperationException("Updates are not allowed for read-only connections");
    }

    @Override
    public JdbcConnection executeWithoutCommitting(String... statements) throws SQLException {
        return super.executeWithoutCommitting(statements);
    }
}