/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Types;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.util.Interner;

/**
 * Tests that Column and Attribute objects are interned at creation time, and that
 * structurally identical tables across tenants share object references, reducing memory usage.
 */
public class TableImplInterningTest {

    @BeforeEach
    public void enableInterner() {
        Interner.setEnabled(true);
    }

    @AfterEach
    public void resetInterner() {
        Interner.clear();
        Interner.setEnabled(false);
    }

    @Test
    public void shouldShareColumnObjectsAcrossTablesWithSameStructure() {
        Table table1 = createTable("tenant1", "schema1", "users");
        Table table2 = createTable("tenant2", "schema2", "users");

        assertThat(table1.id()).isNotEqualTo(table2.id());
        // Columns are interned at creation — same structure yields same references
        for (int i = 0; i < table1.columns().size(); i++) {
            assertThat(table1.columns().get(i)).isSameAs(table2.columns().get(i));
        }
    }

    @Test
    public void shouldShareIndividualColumnObjectsAcrossTablesWithDifferentStructure() {
        Table table1 = Table.editor()
                .tableId(new TableId("tenant1", "schema1", "t"))
                .addColumns(
                        Column.editor().name("id").type("INT").jdbcType(Types.INTEGER).optional(false).create(),
                        Column.editor().name("name").type("VARCHAR").jdbcType(Types.VARCHAR).length(255).optional(true).create())
                .setPrimaryKeyNames("id")
                .create();

        Table table2 = Table.editor()
                .tableId(new TableId("tenant2", "schema2", "t"))
                .addColumns(
                        Column.editor().name("id").type("INT").jdbcType(Types.INTEGER).optional(false).create(),
                        Column.editor().name("email").type("VARCHAR").jdbcType(Types.VARCHAR).length(255).optional(true).create())
                .setPrimaryKeyNames("id")
                .create();

        // Lists differ, but the shared "id" column is the same interned instance
        assertThat(table1.columns().get(0)).isSameAs(table2.columns().get(0));
        assertThat(table1.columns().get(1)).isNotSameAs(table2.columns().get(1));
    }

    @Test
    public void shouldShareAttributeObjectsAcrossTablesWithSameStructure() {
        Table table1 = createTable("tenant1", "schema1", "users");
        Table table2 = createTable("tenant2", "schema2", "users");

        for (int i = 0; i < table1.attributes().size(); i++) {
            assertThat(table1.attributes().get(i)).isSameAs(table2.attributes().get(i));
        }
    }

    @Test
    public void shouldShareIndividualAttributeObjectsAcrossTablesWithDifferentAttributes() {
        Table table1 = Table.editor()
                .tableId(new TableId("tenant1", "schema1", "t"))
                .addColumns(Column.editor().name("id").type("INT").jdbcType(Types.INTEGER).optional(false).create())
                .setPrimaryKeyNames("id")
                .addAttribute(Attribute.editor().name("engine").value("InnoDB").create())
                .addAttribute(Attribute.editor().name("partition").value("hash").create())
                .create();

        Table table2 = Table.editor()
                .tableId(new TableId("tenant2", "schema2", "t"))
                .addColumns(Column.editor().name("id").type("INT").jdbcType(Types.INTEGER).optional(false).create())
                .setPrimaryKeyNames("id")
                .addAttribute(Attribute.editor().name("engine").value("InnoDB").create())
                .addAttribute(Attribute.editor().name("row_format").value("DYNAMIC").create())
                .create();

        // The "engine" attribute is identical and should be the same interned instance
        assertThat(table1.attributeWithName("engine")).isSameAs(table2.attributeWithName("engine"));
    }

    @Test
    public void shouldPreservePkColumnNameOrder() {
        Table table = Table.editor()
                .tableId(new TableId("cat", "sch", "t"))
                .addColumns(
                        Column.editor().name("b").type("INT").jdbcType(Types.INTEGER).optional(false).create(),
                        Column.editor().name("a").type("INT").jdbcType(Types.INTEGER).optional(false).create())
                .setPrimaryKeyNames("b", "a")
                .create();

        assertThat(table.primaryKeyColumnNames()).containsExactly("b", "a");
    }

    @Test
    public void shouldShareDefaultCharsetNameString() {
        Table table1 = Table.editor()
                .tableId(new TableId("tenant1", "schema1", "users"))
                .addColumns(Column.editor().name("id").type("BIGINT").jdbcType(Types.BIGINT).length(19)
                        .optional(false).create())
                .setPrimaryKeyNames("id")
                .setDefaultCharsetName(new String("utf8mb4"))
                .create();

        Table table2 = Table.editor()
                .tableId(new TableId("tenant2", "schema2", "users"))
                .addColumns(Column.editor().name("id").type("BIGINT").jdbcType(Types.BIGINT).length(19)
                        .optional(false).create())
                .setPrimaryKeyNames("id")
                .setDefaultCharsetName(new String("utf8mb4"))
                .create();

        assertThat(table1.defaultCharsetName()).isSameAs(table2.defaultCharsetName());
    }

    @Test
    public void shouldShareColumnStringFields() {
        Column col1 = Column.editor().name(new String("id")).type(new String("BIGINT"))
                .jdbcType(Types.BIGINT).length(19).optional(false).create();
        Column col2 = Column.editor().name(new String("id")).type(new String("BIGINT"))
                .jdbcType(Types.BIGINT).length(19).optional(false).create();

        // Same interned Column instance
        assertThat(col1).isSameAs(col2);
        // String fields are interned
        assertThat(col1.name()).isSameAs(col2.name());
        assertThat(col1.typeName()).isSameAs(col2.typeName());
    }

    @Test
    public void shouldShareColumnListAcrossTablesWithSameStructure() {
        Table table1 = createTable("tenant1", "schema1", "users");
        Table table2 = createTable("tenant2", "schema2", "users");

        // The entire column list instance is shared, not just individual columns
        assertThat(table1.columns()).isSameAs(table2.columns());
    }

    @Test
    public void shouldSharePkColumnNamesListAcrossTablesWithSameStructure() {
        Table table1 = createTable("tenant1", "schema1", "users");
        Table table2 = createTable("tenant2", "schema2", "users");

        assertThat(table1.primaryKeyColumnNames()).isSameAs(table2.primaryKeyColumnNames());
    }

    @Test
    public void shouldShareAttributeListAcrossTablesWithSameStructure() {
        Table table1 = createTable("tenant1", "schema1", "users");
        Table table2 = createTable("tenant2", "schema2", "users");

        assertThat(table1.attributes()).isSameAs(table2.attributes());
    }

    @Test
    public void shouldShareTableNameStringAcrossTableIds() {
        TableId id1 = new TableId(new String("tenant1"), new String("schema1"), new String("users"));
        TableId id2 = new TableId(new String("tenant2"), new String("schema2"), new String("users"));

        // Table names are interned — "users" is the same reference
        assertThat(id1.table()).isSameAs(id2.table());
    }

    @Test
    public void shouldWorkCorrectlyWithTablesOverwriteTable() {
        Tables tables = new Tables();

        tables.overwriteTable(createTable("tenant1", "schema1", "users"));
        tables.overwriteTable(createTable("tenant2", "schema2", "users"));

        Table stored1 = tables.forTable(new TableId("tenant1", "schema1", "users"));
        Table stored2 = tables.forTable(new TableId("tenant2", "schema2", "users"));

        // Individual columns are shared
        for (int i = 0; i < stored1.columns().size(); i++) {
            assertThat(stored1.columns().get(i)).isSameAs(stored2.columns().get(i));
        }
        // Entire column list is shared
        assertThat(stored1.columns()).isSameAs(stored2.columns());
    }

    private static Table createTable(String catalog, String schema, String tableName) {
        return Table.editor()
                .tableId(new TableId(catalog, schema, tableName))
                .addColumns(
                        Column.editor().name("id").type("BIGINT").jdbcType(Types.BIGINT).length(19)
                                .optional(false).create(),
                        Column.editor().name("name").type("VARCHAR").jdbcType(Types.VARCHAR).length(255)
                                .optional(true).create(),
                        Column.editor().name("email").type("VARCHAR").jdbcType(Types.VARCHAR).length(255)
                                .optional(true).create())
                .setPrimaryKeyNames("id")
                .addAttribute(Attribute.editor().name("engine").value("InnoDB").create())
                .create();
    }
}
