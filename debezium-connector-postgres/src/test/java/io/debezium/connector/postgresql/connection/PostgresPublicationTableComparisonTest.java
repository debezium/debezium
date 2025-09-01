/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.connection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.PostgresSchema;
import io.debezium.connector.postgresql.TypeRegistry;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.relational.RelationalTableFilters;
import io.debezium.relational.TableId;

/**
 * Unit tests for PostgresReplicationConnection publication table comparison logic.
 *
 * @author Debezium Contributors
 */
public class PostgresPublicationTableComparisonTest {

    @Mock
    private PostgresConnectorConfig connectorConfig;

    @Mock
    private PostgresConnection jdbcConnection;

    @Mock
    private RelationalTableFilters tableFilter;

    @Mock
    private TypeRegistry typeRegistry;

    @Mock
    private PostgresSchema schema;

    @Mock
    private Statement statement;

    @Mock
    private Connection connection;

    @Mock
    private PreparedStatement preparedStatement;

    @Mock
    private ResultSet resultSet;

    private PostgresReplicationConnection replicationConnection;
    private static final String TEST_PUBLICATION_NAME = "test_publication";
    private static final String TEST_DATABASE_NAME = "test_db";

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);

        // Mock connector config
        when(connectorConfig.databaseName()).thenReturn(TEST_DATABASE_NAME);
        when(connectorConfig.getJdbcConfig()).thenReturn(JdbcConfiguration.create().build());

        replicationConnection = createTestReplicationConnection();

        // Mock statement and connection setup
        when(statement.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
    }

    private PostgresReplicationConnection createTestReplicationConnection() throws Exception {
        return (PostgresReplicationConnection) ReplicationConnection.builder(connectorConfig)
                .withSlot("test_slot")
                .withPublication(TEST_PUBLICATION_NAME)
                .withTableFilter(tableFilter)
                .withPublicationAutocreateMode(PostgresConnectorConfig.AutoCreateMode.FILTERED)
                .withPlugin(PostgresConnectorConfig.LogicalDecoder.PGOUTPUT)
                .dropSlotOnClose(false)
                .createFailOverSlot(false)
                .statusUpdateInterval(Duration.ofSeconds(10))
                .jdbcMetadataConnection(jdbcConnection)
                .withTypeRegistry(typeRegistry)
                .streamParams("")
                .withSchema(schema)
                .build();
    }

    @Test
    public void testGetCurrentPublicationTablesSuccess() throws Exception {
        // Setup current publication tables
        Set<TableId> currentTables = new HashSet<>(Arrays.asList(
                new TableId(null, "public", "customers"),
                new TableId(null, "inventory", "orders")));
        mockGetCurrentPublicationTables(currentTables);

        // Setup mock for determineCapturedTables to return different tables to trigger publication check
        Set<TableId> capturedTables = new HashSet<>(Arrays.asList(
                new TableId(null, "public", "customers"),
                new TableId(null, "inventory", "orders"),
                new TableId(null, "public", "products")));
        mockDetermineCapturedTables(capturedTables);

        // Test indirectly through isPublicationUpdateRequired which calls getCurrentPublicationTables
        boolean result = replicationConnection.isPublicationUpdateRequired(statement);

        // Since current publication has 2 tables but captured tables has 3, update should be required
        assertThat(result).isTrue();

        // Verify SQL query was executed correctly
        verify(connection).prepareStatement(eq(String.format("SELECT schemaname, tablename FROM pg_publication_tables WHERE pubname = '%s'", TEST_PUBLICATION_NAME)));
        verify(preparedStatement).executeQuery();
    }

    @Test
    public void testGetCurrentPublicationTablesWithSQLException() throws Exception {
        // Setup mock to throw SQLException
        when(preparedStatement.executeQuery()).thenThrow(new SQLException("permission denied for relation pg_publication_tables"));

        // Setup mock for determineCapturedTables
        Set<TableId> capturedTables = new HashSet<>(Arrays.asList(
                new TableId(null, "public", "customers")));
        mockDetermineCapturedTables(capturedTables);

        // Test indirectly through isPublicationUpdateRequired which calls getCurrentPublicationTables
        // When getCurrentPublicationTables returns Optional.empty() (due to SQLException), update should be required
        boolean result = replicationConnection.isPublicationUpdateRequired(statement);

        // Verify that update is required when getCurrentPublicationTables fails (returns Optional.empty())
        assertThat(result).isTrue();
    }

    @Test
    public void testGetCurrentPublicationTablesEmptyResult() throws Exception {
        // Setup mock result set with no data
        when(resultSet.next()).thenReturn(false);

        // Setup mock for determineCapturedTables to return some tables
        Set<TableId> capturedTables = new HashSet<>(Arrays.asList(
                new TableId(null, "public", "customers")));
        mockDetermineCapturedTables(capturedTables);

        // Test indirectly through isPublicationUpdateRequired which calls getCurrentPublicationTables
        // When getCurrentPublicationTables returns empty set and captured tables has 1 table, update should be required
        boolean result = replicationConnection.isPublicationUpdateRequired(statement);

        // Verify that update is required when current publication is empty but captured tables exist
        assertThat(result).isTrue();
    }

    @Test
    public void testIsPublicationUpdateRequiredWhenTablesMatch() throws Exception {
        // Setup current publication tables
        Set<TableId> currentTables = new HashSet<>(Arrays.asList(
                new TableId(null, "public", "customers"),
                new TableId(null, "inventory", "orders")));

        // Mock getCurrentPublicationTables to return current tables
        mockGetCurrentPublicationTables(currentTables);

        // Mock determineCapturedTables to return same tables
        mockDetermineCapturedTables(currentTables);

        // Call public method directly
        boolean result = replicationConnection.isPublicationUpdateRequired(statement);

        // Verify no update is required
        assertThat(result).isFalse();
    }

    @Test
    public void testIsPublicationUpdateRequiredWhenTablesDiffer() throws Exception {
        // Setup current publication tables
        Set<TableId> currentTables = new HashSet<>(Arrays.asList(
                new TableId(TEST_DATABASE_NAME, "public", "customers")));

        // Setup desired tables (different from current)
        Set<TableId> desiredTables = new HashSet<>(Arrays.asList(
                new TableId(TEST_DATABASE_NAME, "public", "customers"),
                new TableId(TEST_DATABASE_NAME, "inventory", "orders")));

        // Mock getCurrentPublicationTables to return current tables
        mockGetCurrentPublicationTables(currentTables);

        // Mock determineCapturedTables to return desired tables
        mockDetermineCapturedTables(desiredTables);

        // Call public method directly
        boolean result = replicationConnection.isPublicationUpdateRequired(statement);

        // Verify update is required
        assertThat(result).isTrue();
    }

    @Test
    public void testIsPublicationUpdateRequiredWhenCurrentTablesIsNull() throws Exception {
        // Mock getCurrentPublicationTables to return Optional.empty() (simulating SQL exception)
        mockGetCurrentPublicationTables(null);

        // Mock determineCapturedTables to return some tables
        Set<TableId> desiredTables = new HashSet<>(Arrays.asList(
                new TableId(TEST_DATABASE_NAME, "public", "customers")));
        mockDetermineCapturedTables(desiredTables);

        // Call public method directly
        boolean result = replicationConnection.isPublicationUpdateRequired(statement);

        // Verify update is required when current tables cannot be determined
        assertThat(result).isTrue();
    }

    @Test
    public void testIsPublicationUpdateRequiredWhenNoDesiredTables() throws Exception {
        // Setup current publication tables
        Set<TableId> currentTables = new HashSet<>(Arrays.asList(
                new TableId(TEST_DATABASE_NAME, "public", "customers")));

        // Mock getCurrentPublicationTables to return current tables
        mockGetCurrentPublicationTables(currentTables);

        // Mock determineCapturedTables to return empty set
        mockDetermineCapturedTables(new HashSet<>());

        // Call public method directly
        boolean result = replicationConnection.isPublicationUpdateRequired(statement);

        // Verify no update is required when no desired tables
        assertThat(result).isFalse();
    }

    private void mockGetCurrentPublicationTables(Set<TableId> tables) throws Exception {
        // This is a bit complex to mock private methods, so we'll rely on the actual implementation
        // and mock the underlying database calls instead
        if (tables == null) {
            when(preparedStatement.executeQuery()).thenThrow(new SQLException("permission denied"));
        }
        else if (tables.isEmpty()) {
            when(resultSet.next()).thenReturn(false);
        }
        else {
            // Setup result set for multiple tables
            // getCurrentPublicationTables should return Optional.of(tables)
            Boolean[] nextResults = new Boolean[tables.size() + 1];
            String[] schemaNames = new String[tables.size()];
            String[] tableNames = new String[tables.size()];

            int i = 0;
            for (TableId tableId : tables) {
                nextResults[i] = true;
                schemaNames[i] = tableId.schema();
                tableNames[i] = tableId.table();
                i++;
                // Mock the createTableId method call for each table
                when(jdbcConnection.createTableId(TEST_DATABASE_NAME, tableId.schema(), tableId.table()))
                        .thenReturn(tableId);
            }
            nextResults[i] = false; // Last call returns false

            when(resultSet.next()).thenReturn(nextResults[0], Arrays.copyOfRange(nextResults, 1, nextResults.length));
            when(resultSet.getString("schemaname")).thenReturn(schemaNames[0], Arrays.copyOfRange(schemaNames, 1, schemaNames.length));
            when(resultSet.getString("tablename")).thenReturn(tableNames[0], Arrays.copyOfRange(tableNames, 1, tableNames.length));
        }
    }

    private void mockDetermineCapturedTables(Set<TableId> tables) throws Exception {
        when(jdbcConnection.getAllTableIds(TEST_DATABASE_NAME)).thenReturn(tables);

        // Mock table filter to include all tables
        when(tableFilter.dataCollectionFilter()).thenReturn(tableId -> tables.contains(tableId));
    }
}
