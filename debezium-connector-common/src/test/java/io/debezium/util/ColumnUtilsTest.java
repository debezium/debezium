/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.SQLException;
import java.sql.Types;

import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetMetaDataImpl;
import javax.sql.rowset.RowSetProvider;

import org.junit.jupiter.api.Test;

import io.debezium.doc.FixFor;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;

class ColumnUtilsTest {

    @Test
    @FixFor("debezium/dbz#2604")
    void shouldIdentifyMissingCachedColumnAsSchemaMismatch() throws SQLException {
        final var table = Table.editor().tableId(new TableId("test", null, "a"))
                .addColumn(column("pk", 1)).setPrimaryKeyNames("pk").create();

        try (var rows = resultSet("pk", "c")) {
            assertThatThrownBy(() -> ColumnUtils.toArray(rows, table))
                    .isInstanceOf(ColumnUtils.SchemaMismatchException.class)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Column 'c'")
                    .hasMessageContaining("test.a");
        }
    }

    @Test
    @FixFor("debezium/dbz#2604")
    void shouldRetainResultSetOrderWhenAllColumnsExist() throws SQLException {
        final var primaryKey = column("pk", 1);
        final var value = column("c", 2);
        final var table = Table.editor().tableId(new TableId("test", null, "a"))
                .addColumns(primaryKey, value).setPrimaryKeyNames("pk").create();

        try (var rows = resultSet("c", "pk")) {
            final var mapped = ColumnUtils.toArray(rows, table);
            assertThat(mapped.getColumns()).containsExactly(table.columnWithName("c"), table.columnWithName("pk"));
            assertThat(mapped.getGreatestColumnPosition()).isEqualTo(2);
        }
    }

    private static Column column(String name, int position) {
        return Column.editor().name(name).position(position).jdbcType(Types.INTEGER).type("INT").create();
    }

    private static CachedRowSet resultSet(String... columns) throws SQLException {
        final var metadata = new RowSetMetaDataImpl();
        metadata.setColumnCount(columns.length);
        for (int i = 0; i < columns.length; ++i) {
            metadata.setColumnName(i + 1, columns[i]);
            metadata.setColumnType(i + 1, Types.INTEGER);
        }
        final var rows = RowSetProvider.newFactory().createCachedRowSet();
        rows.setMetaData(metadata);
        return rows;
    }
}
