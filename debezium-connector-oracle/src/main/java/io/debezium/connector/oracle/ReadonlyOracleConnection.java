package io.debezium.connector.oracle;

import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;

import java.sql.Connection;
import java.sql.SQLException;

public class ReadonlyOracleConnection extends OracleConnection{
    public ReadonlyOracleConnection(JdbcConfiguration config) {
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
    public JdbcConnection executeWithoutCommitting(String... statements) {
        throw new UnsupportedOperationException("Updates are not allowed for read-only connections");
    }
}
