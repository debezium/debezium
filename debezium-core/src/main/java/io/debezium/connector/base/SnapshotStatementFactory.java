/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.base;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.RelationalDatabaseConnectorConfig;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Builds the snapshot statement required for reading tables
 */
public class SnapshotStatementFactory implements JdbcConnection.StatementFactory {
    private final JdbcConnection jdbcConnection;
    private final RelationalDatabaseConnectorConfig connectorConfig;

    /**
     * Create a snapshot producer
     *
     * @param connectorConfig relational connector configuration
     * @param jdbcConnection  JDBC connection
     */
    public SnapshotStatementFactory(RelationalDatabaseConnectorConfig connectorConfig,
                                    JdbcConnection jdbcConnection) {
        this.connectorConfig = connectorConfig;
        this.jdbcConnection = jdbcConnection;
    }

    /**
     * Create 'Read' table statement for snapshot
     * @return statement
     * @throws SQLException any SQL exception thrown
     */
    public Statement readTableStatement() throws SQLException {
        return createStatement(jdbcConnection.connection());
    }

    /**
     * Create statement for snapshot reading
     * @param connection the JDBC connection; never null
     * @return statement for snapshot reading
     * @throws SQLException any SQL exception thrown
     */
    @Override
    public Statement createStatement(Connection connection) throws SQLException {
        int rowsFetchSize = connectorConfig.rowsFetchSize();
        Statement statement = connection.createStatement(); // the default cursor is FORWARD_ONLY
        statement.setFetchSize(rowsFetchSize);
        return statement;
    }
}
