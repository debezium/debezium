/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import static org.fest.assertions.Assertions.assertThat;

import java.sql.Types;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Before;
import org.junit.Test;

import io.debezium.doc.FixFor;

public class TableEditorTest {

    private final TableId id = new TableId("catalog", "schema", "table");
    private TableEditor editor;
    private Table table;
    private ColumnEditor columnEditor;

    @Before
    public void beforeEach() {
        editor = Table.editor();
        table = null;
        columnEditor = Column.editor();
    }

    @Test
    public void shouldNotHaveColumnsIfEmpty() {
        assertThat(editor.columnWithName("any")).isNull();
        assertThat(editor.columns()).isEmpty();
        assertThat(editor.primaryKeyColumnNames()).isEmpty();
    }

    @Test(expected = IllegalStateException.class)
    public void shouldFailToCreateTableWhenEditorIsMissingTableId() {
        editor.create();
    }

    @Test
    public void shouldCreateTableWhenEditorHasIdButNoColumns() {
        table = editor.tableId(id).create();
        assertThat(table.columnWithName("any")).isNull();
        assertThat(table.columns()).isEmpty();
        assertThat(table.primaryKeyColumnNames()).isEmpty();
    }

    @Test(expected = IllegalArgumentException.class)
    @FixFor("DBZ-2580")
    public void shouldNotAllowAddingPrimaryKeyColumnWhenNotFound() {
        editor.tableId(id);
        editor.setPrimaryKeyNames("C1", "WOOPS");
        Column c1 = columnEditor.name("C1").type("VARCHAR").jdbcType(Types.VARCHAR).length(10).position(1).create();
        Column c2 = columnEditor.name("C2").type("NUMBER").jdbcType(Types.NUMERIC).length(5).position(1).create();
        Column c3 = columnEditor.name("C3").type("DATE").jdbcType(Types.DATE).position(1).create();
        editor.addColumns(c1, c2, c3);
        editor.create();
    }

    @Test
    public void shouldAllowAddingPrimaryKeyColumnWhenFound() {
        editor.tableId(id);
        Column c1 = columnEditor.name("C1").type("VARCHAR").jdbcType(Types.VARCHAR).length(10).position(1).create();
        Column c2 = columnEditor.name("C2").type("NUMBER").jdbcType(Types.NUMERIC).length(5).position(1).create();
        Column c3 = columnEditor.name("C3").type("DATE").jdbcType(Types.DATE).position(1).create();
        editor.addColumns(c1, c2, c3);
        editor.setPrimaryKeyNames("C1");
        c1 = editor.columnWithName(c1.name());
        c2 = editor.columnWithName(c2.name());
        c3 = editor.columnWithName(c3.name());
        assertThat(c1.position()).isEqualTo(1);
        assertThat(c2.position()).isEqualTo(2);
        assertThat(c3.position()).isEqualTo(3);
        table = editor.create();
        assertThat(table.retrieveColumnNames()).containsExactly("C1", "C2", "C3");
        assertThat(table.columns()).containsExactly(c1, c2, c3);
        assertThat(table.primaryKeyColumnNames()).containsOnly("C1");
        assertValidPositions(editor);
    }

    @Test
    public void shouldFindNonExistingColumnByNameIndependentOfCase() {
        editor.tableId(id);
        Column c1 = columnEditor.name("C1").type("VARCHAR").jdbcType(Types.VARCHAR).length(10).position(1).create();
        Column c2 = columnEditor.name("C2").type("NUMBER").jdbcType(Types.NUMERIC).length(5).position(1).create();
        Column c3 = columnEditor.name("C3").type("DATE").jdbcType(Types.DATE).position(1).create();
        editor.addColumns(c1, c2, c3);
        editor.columns().forEach(col -> {
            assertThat(editor.columnWithName(col.name())).isNotNull();
            assertThat(editor.columnWithName(col.name().toUpperCase())).isNotNull();
            assertThat(editor.columnWithName(col.name().toLowerCase())).isNotNull();
        });
        assertThat(editor.columnWithName("WOOPS")).isNull();
    }

    @Test
    public void shouldFindGeneratedColumns() {
        editor.tableId(id);
        Column c1 = columnEditor.name("C1").type("VARCHAR").jdbcType(Types.VARCHAR).length(10).position(1).create();
        Column c2 = columnEditor.name("C2").type("NUMBER").jdbcType(Types.NUMERIC).length(5).generated(true).create();
        Column c3 = columnEditor.name("C3").type("DATE").jdbcType(Types.DATE).generated(true).create();
        editor.addColumns(c1, c2, c3);
        editor.setPrimaryKeyNames("C1");
        table = editor.create();
        assertThat(table.retrieveColumnNames()).containsExactly("C1", "C2", "C3");
        table.columns().forEach(col -> {
            assertThat(table.isGenerated(col.name())).isEqualTo(col.isGenerated());
        });
        assertValidPositions(editor);
    }

    @Test
    public void shouldFindAutoIncrementedColumns() {
        editor.tableId(id);
        Column c1 = columnEditor.name("C1").type("VARCHAR").jdbcType(Types.VARCHAR).length(10).position(1).create();
        Column c2 = columnEditor.name("C2").type("NUMBER").jdbcType(Types.NUMERIC).length(5).autoIncremented(true).create();
        Column c3 = columnEditor.name("C3").type("DATE").jdbcType(Types.DATE).autoIncremented(true).create();
        editor.addColumns(c1, c2, c3);
        editor.setPrimaryKeyNames("C1");
        table = editor.create();
        assertThat(table.retrieveColumnNames()).containsExactly("C1", "C2", "C3");
        table.columns().forEach(col -> {
            assertThat(table.isAutoIncremented(col.name())).isEqualTo(col.isAutoIncremented());
        });
        assertValidPositions(editor);
    }

    @Test
    public void shouldReorderColumns() {
        editor.tableId(id);
        Column c1 = columnEditor.name("C1").type("VARCHAR").jdbcType(Types.VARCHAR).length(10).position(1).create();
        Column c2 = columnEditor.name("C2").type("NUMBER").jdbcType(Types.NUMERIC).length(5).autoIncremented(true).create();
        Column c3 = columnEditor.name("C3").type("DATE").jdbcType(Types.DATE).autoIncremented(true).create();
        editor.addColumns(c1, c2, c3);
        assertValidPositions(editor);
        editor.reorderColumn("C1", null);
        assertThat(editor.columns()).containsExactly(editor.columnWithName("C1"),
                editor.columnWithName("C2"),
                editor.columnWithName("C3"));
        assertValidPositions(editor);
        editor.reorderColumn("C2", "C1");
        assertThat(editor.columns()).containsExactly(editor.columnWithName("C1"),
                editor.columnWithName("C2"),
                editor.columnWithName("C3"));
        assertValidPositions(editor);
        editor.reorderColumn("C3", "C2");
        assertThat(editor.columns()).containsExactly(editor.columnWithName("C1"),
                editor.columnWithName("C2"),
                editor.columnWithName("C3"));
        assertValidPositions(editor);
        editor.reorderColumn("C3", "C1");
        assertThat(editor.columns()).containsExactly(editor.columnWithName("C1"),
                editor.columnWithName("C3"),
                editor.columnWithName("C2"));
        assertValidPositions(editor);
        editor.reorderColumn("C3", null);
        assertThat(editor.columns()).containsExactly(editor.columnWithName("C3"),
                editor.columnWithName("C1"),
                editor.columnWithName("C2"));
        assertValidPositions(editor);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldNotReorderColumnIfNameDoesNotMatch() {
        editor.tableId(id);
        Column c1 = columnEditor.name("C1").type("VARCHAR").jdbcType(Types.VARCHAR).length(10).position(1).create();
        Column c2 = columnEditor.name("C2").type("NUMBER").jdbcType(Types.NUMERIC).length(5).autoIncremented(true).create();
        Column c3 = columnEditor.name("C3").type("DATE").jdbcType(Types.DATE).autoIncremented(true).create();
        editor.addColumns(c1, c2, c3);
        editor.reorderColumn("WOOPS", "C2");
        assertValidPositions(editor);
    }

    @Test
    public void shouldRemoveColumnByName() {
        editor.tableId(id);
        Column c1 = columnEditor.name("C1").type("VARCHAR").jdbcType(Types.VARCHAR).length(10).position(1).create();
        Column c2 = columnEditor.name("C2").type("NUMBER").jdbcType(Types.NUMERIC).length(5).autoIncremented(true).create();
        Column c3 = columnEditor.name("C3").type("DATE").jdbcType(Types.DATE).autoIncremented(true).create();
        editor.addColumns(c1, c2, c3);
        editor.removeColumn("C2");
        assertThat(editor.columns()).containsExactly(editor.columnWithName("C1"),
                editor.columnWithName("C3"));
        assertValidPositions(editor);
    }

    protected void assertValidPositions(TableEditor editor) {
        AtomicInteger position = new AtomicInteger(1);
        assertThat(editor.columns().stream().allMatch(defn -> defn.position() == position.getAndIncrement())).isTrue();
    }

    protected void assertValidPositions(Table editor) {
        AtomicInteger position = new AtomicInteger(1);
        assertThat(editor.columns().stream().allMatch(defn -> defn.position() == position.getAndIncrement())).isTrue();
    }
}
