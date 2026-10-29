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
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.sql.Statement;
import java.time.Duration;

import org.apache.kafka.connect.errors.RetriableException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import io.debezium.DebeziumException;
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
    @FixFor("debezium/dbz#2704")
    void whenAutonomousCheckQueryFailsThenIsAutonomousReturnsFalse() throws Exception {
        when(statement.executeQuery(any())).thenThrow(new SQLException("ORA-00942: table or view does not exist"));

        try (OracleConnection connection = new OracleConnection(jdbcConfiguration, connectionFactory, true)) {
            assertThat(connection.isAutonomous()).isFalse();
        }
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

    @Test
    @FixFor("debezium/dbz#2972")
    void whenReconnectedAfterSwitchingToPdbThenPdbContainerIsRestored() throws Exception {
        final Statement newStatement = stubReconnect();

        try (OracleConnection oracleConnection = new OracleConnection(jdbcConfiguration, connectionFactory, true)) {
            oracleConnection.setSessionToPdb("PDB1");
            verify(statement).execute("alter session set container=PDB1");

            when(connection.isClosed()).thenReturn(true);
            oracleConnection.execute("UPDATE HEARTBEAT SET TS = SYSTIMESTAMP");

            final InOrder inOrder = inOrder(newStatement);
            inOrder.verify(newStatement).execute("alter session set container=PDB1");
            inOrder.verify(newStatement).execute("UPDATE HEARTBEAT SET TS = SYSTIMESTAMP");
        }
    }

    @Test
    @FixFor("debezium/dbz#2972")
    void whenReconnectedAfterResettingToCdbThenCdbRootContainerIsRestored() throws Exception {
        final Statement newStatement = stubReconnect();

        try (OracleConnection oracleConnection = new OracleConnection(jdbcConfiguration, connectionFactory, true)) {
            oracleConnection.setSessionToPdb("PDB1");
            oracleConnection.resetSessionToCdb();

            when(connection.isClosed()).thenReturn(true);
            oracleConnection.execute("SELECT 1 FROM DUAL");

            final InOrder inOrder = inOrder(newStatement);
            inOrder.verify(newStatement).execute("alter session set container=cdb$root");
            inOrder.verify(newStatement).execute("SELECT 1 FROM DUAL");
            verify(newStatement, never()).execute("alter session set container=PDB1");
        }
    }

    @Test
    @FixFor("debezium/dbz#2972")
    void whenReconnectedWithoutContainerSwitchThenNoContainerIsSet() throws Exception {
        final Statement newStatement = stubReconnect();

        try (OracleConnection oracleConnection = new OracleConnection(jdbcConfiguration, connectionFactory, true)) {
            oracleConnection.connection();

            when(connection.isClosed()).thenReturn(true);
            oracleConnection.execute("SELECT 1 FROM DUAL");

            verify(newStatement).execute("SELECT 1 FROM DUAL");
            verify(newStatement, never()).execute(startsWith("alter session set container"));
        }
    }

    @Test
    @FixFor("debezium/dbz#2972")
    void whenContainerSwitchFailsThenPreviousContainerIsRestoredOnReconnect() throws Exception {
        final Statement newStatement = stubReconnect();

        try (OracleConnection oracleConnection = new OracleConnection(jdbcConfiguration, connectionFactory, true)) {
            oracleConnection.setSessionToPdb("PDB1");

            when(statement.execute("alter session set container=PDB2")).thenThrow(new SQLException("ORA-65011: Pluggable database PDB2 does not exist."));
            assertThrows(DebeziumException.class, () -> oracleConnection.setSessionToPdb("PDB2"));

            when(connection.isClosed()).thenReturn(true);
            oracleConnection.execute("SELECT 1 FROM DUAL");

            verify(newStatement).execute("alter session set container=PDB1");
            verify(newStatement, never()).execute("alter session set container=PDB2");
        }
    }

    @Test
    @FixFor("debezium/dbz#2972")
    void whenConnectionIsExplicitlyClosedThenContainerIsNotRestoredOnReuse() throws Exception {
        final Statement newStatement = stubReconnect();

        try (OracleConnection oracleConnection = new OracleConnection(jdbcConfiguration, connectionFactory, true)) {
            oracleConnection.setSessionToPdb("PDB1");
            oracleConnection.close();

            oracleConnection.execute("SELECT 1 FROM DUAL");

            verify(newStatement).execute("SELECT 1 FROM DUAL");
            verify(newStatement, never()).execute(startsWith("alter session set container"));
        }
    }

    @Test
    @FixFor("debezium/dbz#2972")
    void whenContainerRestoreFailsOnReconnectThenConnectionIsClosedAndRestoreIsRetried() throws Exception {
        final Statement newStatement = stubReconnect();
        final Connection newConnection = newStatement.getConnection();
        when(newStatement.execute("alter session set container=PDB1"))
                .thenThrow(new SQLException("ORA-65011: Pluggable database PDB1 does not exist."))
                .thenReturn(false);

        try (OracleConnection oracleConnection = new OracleConnection(jdbcConfiguration, connectionFactory, true)) {
            oracleConnection.setSessionToPdb("PDB1");

            when(connection.isClosed()).thenReturn(true);
            assertThrows(SQLException.class, () -> oracleConnection.execute("SELECT 1 FROM DUAL"));

            verify(newConnection).close();
            verify(newStatement, never()).execute("SELECT 1 FROM DUAL");

            // The closed connection is detected on the next call, which reconnects and retries the switch
            when(newConnection.isClosed()).thenReturn(true, false);
            oracleConnection.execute("SELECT 1 FROM DUAL");

            final InOrder inOrder = inOrder(newStatement);
            inOrder.verify(newStatement, times(2)).execute("alter session set container=PDB1");
            inOrder.verify(newStatement).execute("SELECT 1 FROM DUAL");
        }
    }

    @Test
    @FixFor("debezium/dbz#2972")
    void whenExplicitlyReconnectedThenContainerIsRestored() throws Exception {
        final Statement newStatement = stubReconnect();

        try (OracleConnection oracleConnection = new OracleConnection(jdbcConfiguration, connectionFactory, true)) {
            oracleConnection.setSessionToPdb("PDB1");
            oracleConnection.reconnect();

            verify(newStatement).execute("alter session set container=PDB1");
        }
    }

    /**
     * Makes the connection factory return a second, fresh connection on the next connect call.
     *
     * @return the statement created by the second connection
     */
    private Statement stubReconnect() throws SQLException {
        final Connection newConnection = mock(Connection.class);
        final Statement newStatement = mock(Statement.class);
        when(newConnection.createStatement()).thenReturn(newStatement);
        when(newStatement.getConnection()).thenReturn(newConnection);
        when(connectionFactory.connect(jdbcConfiguration)).thenReturn(connection, newConnection);
        return newStatement;
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
