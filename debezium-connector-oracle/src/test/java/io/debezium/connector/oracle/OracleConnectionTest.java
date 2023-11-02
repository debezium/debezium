/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.sql.Statement;

import org.apache.kafka.connect.errors.RetriableException;
import org.junit.Before;
import org.junit.Test;

import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;

public class OracleConnectionTest {

    private Statement statement;
    private JdbcConfiguration jdbcConfiguration;
    private JdbcConnection.ConnectionFactory connectionFactory;

    @Before
    public void setUp() throws Exception {

        jdbcConfiguration = mock(JdbcConfiguration.class);
        connectionFactory = mock(JdbcConnection.ConnectionFactory.class);
        Connection connection = mock(Connection.class);
        statement = mock(Statement.class);
        when(connection.createStatement()).thenReturn(statement);
        when(connectionFactory.connect(jdbcConfiguration)).thenReturn(connection);

    }

    @Test
    public void whenOracleConnectionGetSQLRecoverableExceptionThenARetriableExceptionWillBeThrown() throws SQLException {

        when(statement.executeQuery(any()))
                .thenThrow(new SQLRecoverableException("IO Error: The Network Adapter could not establish the connection (CONNECTION_ID=u/VErjYySfO0HgLtwdCuTQ==)"));

        assertThrows(RetriableException.class,
                () -> new OracleConnection(new OracleConnection.OracleConnectionConfiguration(jdbcConfiguration), connectionFactory, true));
    }
}
