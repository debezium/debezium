/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.sql.Statement;
import java.time.Duration;

import org.apache.kafka.connect.errors.RetriableException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.doc.FixFor;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;

public class OracleConnectionTest {

    private Statement statement;
    private JdbcConfiguration jdbcConfiguration;
    private Connection connection;
    private JdbcConnection.ConnectionFactory connectionFactory;

    @BeforeEach
    void setUp() throws Exception {

        jdbcConfiguration = mock(JdbcConfiguration.class);
        when(jdbcConfiguration.getQueryTimeout()).thenReturn(Duration.ZERO);
        connectionFactory = mock(JdbcConnection.ConnectionFactory.class);
        connection = mock(Connection.class);
        doNothing().when(connection).setAutoCommit(anyBoolean());
        statement = mock(Statement.class);
        when(connection.createStatement()).thenReturn(statement);
        when(statement.getConnection()).thenReturn(connection);
        when(connectionFactory.connect(jdbcConfiguration)).thenReturn(connection);

    }

    @Test
    void whenOracleConnectionGetSQLRecoverableExceptionThenARetriableExceptionWillBeThrown() throws SQLException {

        when(statement.executeQuery(any()))
                .thenThrow(new SQLRecoverableException("IO Error: The Network Adapter could not establish the connection (CONNECTION_ID=u/VErjYySfO0HgLtwdCuTQ==)"));

        when(connection.getMetaData())
                .thenThrow(new SQLRecoverableException("IO Error: The Network Adapter could not establish the connection (CONNECTION_ID=u/VErjYySfO0HgLtwdCuTQ==)"));

        assertThrows(RetriableException.class, () -> {
            try (OracleConnection connection = new OracleConnection(jdbcConfiguration, connectionFactory, true)) {
                // Force a connection call to the database.
                connection.getOracleVersion();
            }
        });
    }

    @Test
    @FixFor("debezium/dbz#2653")
    void whenTableIdHasCatalogThenQuotedTableIdStringOmitsIt() throws Exception {
        try (OracleConnection connection = createOfflineConnection()) {
            assertThat(connection.quotedTableIdString(new TableId("ORCLPDB1", "DEBEZIUM", "CUSTOMERS")))
                    .isEqualTo("\"DEBEZIUM\".\"CUSTOMERS\"");
        }
    }

    @Test
    @FixFor("debezium/dbz#2653")
    void whenTableIdHasNoCatalogThenQuotedTableIdStringIsUnchanged() throws Exception {
        try (OracleConnection connection = createOfflineConnection()) {
            assertThat(connection.quotedTableIdString(new TableId(null, "DEBEZIUM", "CUSTOMERS")))
                    .isEqualTo("\"DEBEZIUM\".\"CUSTOMERS\"");
        }
    }

    @Test
    @FixFor("debezium/dbz#2653")
    void whenTableIdHasMixedCaseNamesThenQuotedTableIdStringPreservesThem() throws Exception {
        try (OracleConnection connection = createOfflineConnection()) {
            assertThat(connection.quotedTableIdString(new TableId("ORCLPDB1", "Debezium", "Customers")))
                    .isEqualTo("\"Debezium\".\"Customers\"");
        }
    }

    /**
     * Creates a connection that is never opened, sufficient for exercising query construction.
     */
    private static OracleConnection createOfflineConnection() {
        final JdbcConfiguration config = JdbcConfiguration.adapt(
                Configuration.create().with("url", "jdbc:oracle:thin:@localhost:1521/ORCLPDB1").build());
        return new OracleConnection(config, c -> {
            throw new SQLException("The connection should not be established");
        }, true);
    }
}
