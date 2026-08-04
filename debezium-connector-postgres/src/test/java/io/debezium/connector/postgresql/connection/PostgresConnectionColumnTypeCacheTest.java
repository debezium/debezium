/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.connection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.junit.jupiter.api.Test;

import io.debezium.connector.postgresql.PgOid;
import io.debezium.connector.postgresql.PostgresType;
import io.debezium.connector.postgresql.TestHelper;
import io.debezium.connector.postgresql.TypeRegistry;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.relational.Column;
import io.debezium.relational.Table;

/**
 * Unit tests for the per-column {@link PostgresType} cache in {@link PostgresConnection#getColumnValue}. The cache
 * resolves a column's type ({@link ResultSetMetaData#getColumnTypeName} + {@link TypeRegistry#get}) once per column for
 * the lifetime of a {@link ResultSet}, instead of once per column per row. These tests pin the two properties every
 * snapshot flavor relies on: (1) within a single ResultSet a column is resolved exactly once regardless of row count
 * (initial/blocking snapshot), and (2) a new ResultSet forces re-resolution, so a connection reused across many chunk
 * queries (incremental / read-only-incremental / ad-hoc snapshots) never serves a stale type. The resolved type and
 * the returned value stay identical to the un-cached path.
 */
public class PostgresConnectionColumnTypeCacheTest {

    /**
     * Builds a {@link PostgresConnection} whose real {@code getColumnValue}/type-cache code runs, without opening a
     * database connection: the (config, null, null, usage) constructor takes the no-value-converter branch (so
     * {@code getTimestampUtils()} is never called) while still running the field initializers, so the cache's
     * {@code ThreadLocal} is set up exactly as in production. {@link PostgresConnection#getTypeRegistry()} is then
     * stubbed to the supplied registry.
     */
    private static PostgresConnection connectionWith(TypeRegistry registry) {
        JdbcConfiguration config = TestHelper.defaultJdbcConfig();
        PostgresConnection connection = mock(
                PostgresConnection.class,
                withSettings().useConstructor(config, null, null, "test").defaultAnswer(CALLS_REAL_METHODS));
        doReturn(registry).when(connection).getTypeRegistry();
        return connection;
    }

    private static PostgresType scalarType(String name, int oid) {
        PostgresType type = mock(PostgresType.class);
        when(type.getOid()).thenReturn(oid);
        when(type.getName()).thenReturn(name);
        when(type.isArrayType()).thenReturn(false);
        return type;
    }

    private static ResultSet resultSetWithColumns(int columnCount) throws SQLException {
        ResultSet rs = mock(ResultSet.class);
        ResultSetMetaData metaData = mock(ResultSetMetaData.class);
        when(rs.getMetaData()).thenReturn(metaData);
        when(metaData.getColumnCount()).thenReturn(columnCount);
        return rs;
    }

    @Test
    public void columnTypeIsResolvedOncePerColumnRegardlessOfRowCount() throws SQLException {
        TypeRegistry registry = mock(TypeRegistry.class);
        PostgresType int4 = scalarType("int4", PgOid.INT4);
        when(registry.get("int4")).thenReturn(int4);

        ResultSet rs = resultSetWithColumns(1);
        ResultSetMetaData metaData = rs.getMetaData();
        when(metaData.getColumnTypeName(1)).thenReturn("int4");
        when(rs.getObject(1)).thenReturn(42);

        PostgresConnection connection = connectionWith(registry);
        Column column = mock(Column.class);
        Table table = mock(Table.class);

        // Read the same column across many rows.
        for (int row = 0; row < 1_000; row++) {
            assertThat(connection.getColumnValue(rs, 1, column, table)).isEqualTo(42);
        }

        // Type resolution ran exactly once; only the per-row value read repeated.
        verify(metaData, times(1)).getColumnTypeName(1);
        verify(registry, times(1)).get("int4");
        verify(rs, times(1_000)).getObject(1);
    }

    @Test
    public void differentColumnsAreCachedIndependently() throws SQLException {
        TypeRegistry registry = mock(TypeRegistry.class);
        PostgresType int4 = scalarType("int4", PgOid.INT4);
        PostgresType varchar = scalarType("varchar", PgOid.VARCHAR);
        when(registry.get("int4")).thenReturn(int4);
        when(registry.get("varchar")).thenReturn(varchar);

        ResultSet rs = resultSetWithColumns(2);
        ResultSetMetaData metaData = rs.getMetaData();
        when(metaData.getColumnTypeName(1)).thenReturn("int4");
        when(metaData.getColumnTypeName(2)).thenReturn("varchar");
        when(rs.getObject(1)).thenReturn(42);
        when(rs.getObject(2)).thenReturn("hello");

        PostgresConnection connection = connectionWith(registry);
        Column column = mock(Column.class);
        Table table = mock(Table.class);

        for (int row = 0; row < 10; row++) {
            assertThat(connection.getColumnValue(rs, 1, column, table)).isEqualTo(42);
            assertThat(connection.getColumnValue(rs, 2, column, table)).isEqualTo("hello");
        }

        verify(metaData, times(1)).getColumnTypeName(1);
        verify(metaData, times(1)).getColumnTypeName(2);
        verify(registry, times(1)).get("int4");
        verify(registry, times(1)).get("varchar");
    }

    @Test
    public void newResultSetForcesReResolution() throws SQLException {
        TypeRegistry registry = mock(TypeRegistry.class);
        PostgresType int4 = scalarType("int4", PgOid.INT4);
        when(registry.get("int4")).thenReturn(int4);

        PostgresConnection connection = connectionWith(registry);
        Column column = mock(Column.class);
        Table table = mock(Table.class);

        // First chunk's ResultSet.
        ResultSet firstChunk = resultSetWithColumns(1);
        ResultSetMetaData firstMeta = firstChunk.getMetaData();
        when(firstMeta.getColumnTypeName(1)).thenReturn("int4");
        when(firstChunk.getObject(1)).thenReturn(1);
        for (int row = 0; row < 5; row++) {
            connection.getColumnValue(firstChunk, 1, column, table);
        }

        // A distinct ResultSet, as produced by the next incremental-snapshot chunk on the same connection.
        ResultSet secondChunk = resultSetWithColumns(1);
        ResultSetMetaData secondMeta = secondChunk.getMetaData();
        when(secondMeta.getColumnTypeName(1)).thenReturn("int4");
        when(secondChunk.getObject(1)).thenReturn(2);
        for (int row = 0; row < 5; row++) {
            connection.getColumnValue(secondChunk, 1, column, table);
        }

        // Each ResultSet resolved its own column exactly once: the cache invalidated on the new ResultSet.
        verify(firstMeta, times(1)).getColumnTypeName(1);
        verify(secondMeta, times(1)).getColumnTypeName(1);
        verify(registry, times(2)).get("int4");
    }
}
