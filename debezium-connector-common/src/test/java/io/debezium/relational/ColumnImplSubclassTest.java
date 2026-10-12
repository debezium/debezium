/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.Types;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.DebeziumException;
import io.debezium.doc.FixFor;
import io.debezium.relational.ExtendedMetadata.ExtendedColumn;
import io.debezium.relational.ExtendedMetadata.ExtendedColumnEditor;
import io.debezium.relational.ExtendedMetadata.ExtendedColumnEditorImpl;
import io.debezium.util.Interner;
import io.debezium.util.Interner.Mode;

/**
 * Verifies that a connector can subclass {@link ColumnImpl} and {@link ColumnEditorImpl} to carry
 * additional column metadata through editing, equality, interning, the table editor and {@link Tables}.
 * The sample subclasses live in {@link ExtendedMetadata}.
 *
 * @author Chris Cranford
 */
public class ColumnImplSubclassTest {

    private final TableId tableId = new TableId("catalog", "schema", "table");

    @BeforeEach
    public void enableInterner() {
        Interner.activate(Mode.ON);
    }

    @AfterEach
    public void resetInterner() {
        Interner.clear();
        Interner.activate(Mode.OFF);
    }

    @Test
    @FixFor("debezium/dbz#2843")
    public void shouldSetBaseAndExtensionFieldsThroughTypedEditor() {
        final ExtendedColumnEditor editor = new ExtendedColumnEditorImpl();
        editor.name("id").type("NUMBER").jdbcType(Types.NUMERIC).optional(false).position(3);
        editor.region("us");
        final ExtendedColumn column = editor.create();

        assertThat(column.name()).isEqualTo("id");
        assertThat(column.jdbcType()).isEqualTo(Types.NUMERIC);
        assertThat(column.isOptional()).isFalse();
        assertThat(column.position()).isEqualTo(3);
        assertThat(column.region()).isEqualTo("us");
    }

    @Test
    @FixFor("debezium/dbz#2843")
    public void shouldPreserveExtensionThroughEdit() {
        final ExtendedColumn column = column("id", "us");

        final Column edited = column.edit().position(5).comment("pk").create();

        assertThat(edited.position()).isEqualTo(5);
        assertThat(edited.comment()).isEqualTo("pk");
        assertThat(edited.unwrap(ExtendedColumn.class).region()).isEqualTo("us");
    }

    @Test
    @FixFor("debezium/dbz#2843")
    public void shouldPreserveExtensionWhenAddedToTableEditor() {
        // The table editor re-creates every added column through edit() to assign its position
        final Table table = Table.editor().tableId(tableId).addColumn(column("id", "us")).setPrimaryKeyNames("id").create();

        final Column column = table.columnWithName("id");
        assertThat(column).isInstanceOf(ExtendedColumn.class);
        assertThat(column.unwrap(ExtendedColumn.class).region()).isEqualTo("us");
    }

    @Test
    @FixFor("debezium/dbz#2843")
    public void shouldIncludeExtensionInEquality() {
        assertThat(column("id", "us")).isEqualTo(column("id", "us")).hasSameHashCodeAs(column("id", "us"));
        assertThat(column("id", "us")).isNotEqualTo(column("id", "eu"));
        assertThat(column("id", "us")).isNotEqualTo(plainColumn("id"));
    }

    @Test
    @FixFor("debezium/dbz#2843")
    public void shouldNotInternExtendedColumnAsEqualPlainColumn() {
        // A plain column pooled first, as another connector sharing the pool would leave it
        final Column plain = plainColumn("id");
        final ExtendedColumn column = column("id", "us");

        assertThat(column).isNotSameAs(plain);
        assertThat(column.region()).isEqualTo("us");
    }

    @Test
    @FixFor("debezium/dbz#2843")
    public void shouldNotInternColumnsThatDifferOnlyByExtension() {
        final ExtendedColumn us = column("id", "us");
        final ExtendedColumn eu = column("id", "eu");
        final ExtendedColumn usAgain = column("id", "us");

        assertThat(eu).isNotSameAs(us);
        assertThat(usAgain).isSameAs(us);
    }

    @Test
    @FixFor("debezium/dbz#2843")
    public void shouldDetectChangeInTablesWhenOnlyExtensionChanges() {
        final Tables tables = new Tables();
        tables.overwriteTable(tableId, List.of(column("id", "us")), List.of("id"), null, List.of());
        assertThat(tables.drainChanges()).containsExactly(tableId);

        tables.overwriteTable(tableId, List.of(column("id", "us")), List.of("id"), null, List.of());
        assertThat(tables.drainChanges()).isEmpty();

        tables.overwriteTable(tableId, List.of(column("id", "eu")), List.of("id"), null, List.of());
        assertThat(tables.drainChanges()).containsExactly(tableId);
        assertThat(tables.forTable(tableId).columnWithName("id").unwrap(ExtendedColumn.class).region()).isEqualTo("eu");
    }

    @Test
    @FixFor("debezium/dbz#2843")
    public void shouldRenderExtensionInToString() {
        assertThat(column("id", "us").toString()).contains("REGION us");
    }

    @Test
    @FixFor("debezium/dbz#2843")
    public void shouldUnwrapColumnAndEditorToExtensionTypes() {
        final Column column = column("id", "us");
        assertThat(column.unwrap(ExtendedColumn.class).region()).isEqualTo("us");
        assertThat(column.unwrap(ColumnImpl.class)).isSameAs(column);

        final ColumnEditor editor = new ExtendedColumnEditorImpl().region("us");
        assertThat(editor.unwrap(ExtendedColumnEditor.class).region()).isEqualTo("us");
    }

    @Test
    @FixFor("debezium/dbz#2843")
    public void shouldFailToUnwrapDefaultColumnToExtensionType() {
        final Column column = Column.editor().name("id").create();

        assertThatThrownBy(() -> column.unwrap(ExtendedColumn.class))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("Column 'id' is a ColumnImpl, not a ExtendedColumn");

        assertThatThrownBy(() -> Column.editor().name("id").unwrap(ExtendedColumnEditor.class))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("ColumnEditor 'id' is a ColumnEditorImpl, not a ExtendedColumnEditor");
    }

    private static Column plainColumn(String name) {
        return Column.editor().name(name).type("NUMBER").jdbcType(Types.NUMERIC).optional(false).create();
    }

    private static ExtendedColumn column(String name, String region) {
        final ExtendedColumnEditorImpl editor = new ExtendedColumnEditorImpl();
        editor.name(name).type("NUMBER").jdbcType(Types.NUMERIC).optional(false);
        return editor.region(region).create();
    }
}
