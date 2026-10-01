/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.debezium.doc.FixFor;
import io.debezium.sink.valuebinding.ValueBindDescriptor;

/**
 * Unit tests for {@link PreparedStatementQueryBinder}.
 */
@Tag("UnitTests")
class PreparedStatementQueryBinderTest {

    @Test
    @FixFor("debezium/dbz#2571")
    @DisplayName("Should pass a typed array to createArrayOf as-is")
    void testBindsTypedArrayAsIs() throws SQLException {
        final PreparedStatement statement = mock(PreparedStatement.class);
        final Connection connection = mock(Connection.class);
        when(statement.getConnection()).thenReturn(connection);

        final byte[][] value = new byte[][]{ { 1, 2 }, null, { 3 } };
        new PreparedStatementQueryBinder(statement)
                .bind(new ValueBindDescriptor(1, value, Types.ARRAY, "bytea"));

        verify(connection).createArrayOf(eq("bytea"), eq(value));
    }

    @Test
    @FixFor("debezium/dbz#2571")
    @DisplayName("Should convert a collection to Object[] for createArrayOf")
    void testBindsCollectionAsObjectArray() throws SQLException {
        final PreparedStatement statement = mock(PreparedStatement.class);
        final Connection connection = mock(Connection.class);
        when(statement.getConnection()).thenReturn(connection);

        new PreparedStatementQueryBinder(statement)
                .bind(new ValueBindDescriptor(1, List.of(1, 2, 42), Types.ARRAY, "int4"));

        verify(connection).createArrayOf(eq("int4"), eq(new Object[]{ 1, 2, 42 }));
    }

    @Test
    @FixFor("debezium/dbz#2571")
    @DisplayName("Should reject an ARRAY value that is neither a collection nor an object array")
    void testRejectsUnsupportedArrayValue() {
        final PreparedStatement statement = mock(PreparedStatement.class);

        assertThatThrownBy(() -> new PreparedStatementQueryBinder(statement)
                .bind(new ValueBindDescriptor(1, new int[]{ 1, 2 }, Types.ARRAY, "int4")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unsupported ARRAY value type")
                .hasMessageContaining(int[].class.getName());
    }
}
