/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Types;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.doc.FixFor;
import io.debezium.junit.relational.TestRelationalDatabaseConfig;
import io.debezium.relational.ExtendedMetadata.ExtendedColumn;
import io.debezium.relational.ExtendedMetadata.ExtendedRelationalMetadataFactory;
import io.debezium.relational.ExtendedMetadata.ExtendedTable;
import io.debezium.relational.ExtendedMetadata.ExtendedTableEditor;
import io.debezium.relational.Tables.TableFilter;
import io.debezium.util.Interner;
import io.debezium.util.Interner.Mode;

/**
 * Verifies that {@link Tables} creates every table through its {@link RelationalMetadataFactory}, keeps a
 * connector's {@link TableImpl} subclass intact when tables are stored, renamed, cloned or filtered, and
 * takes the factory from the connector config.
 *
 * @author Chris Cranford
 */
public class TablesTest {

    private final TableId tableId = new TableId("catalog", "schema", "table");
    private final TableId otherTableId = new TableId("catalog", "schema", "other");
    private Tables tables;

    @BeforeEach
    public void beforeEach() {
        Interner.activate(Mode.ON);
        tables = new Tables(false, ExtendedRelationalMetadataFactory.INSTANCE);
    }

    @AfterEach
    public void afterEach() {
        Interner.clear();
        Interner.activate(Mode.OFF);
    }

    @Test
    @FixFor("debezium/dbz#2843")
    public void shouldDefaultToStandardFactory() {
        assertThat(new Tables().editOrCreateTable(tableId)).isExactlyInstanceOf(TableEditorImpl.class);
        assertThat(new Tables(true).editOrCreateTable(tableId)).isExactlyInstanceOf(TableEditorImpl.class);
    }

    @Test
    @FixFor("debezium/dbz#2843")
    public void shouldCreateTablesThroughFactoryWhenOverwritingFromColumns() {
        tables.overwriteTable(tableId, List.of(column("id")), List.of("id"), null, List.of());

        final Table table = tables.forTable(tableId);
        assertThat(table).isInstanceOf(ExtendedTable.class);
        assertThat(table.columnWithName("id")).isInstanceOf(ExtendedColumn.class);
    }

    @Test
    @FixFor("debezium/dbz#2843")
    public void shouldCreateEditorThroughFactoryForUnknownTable() {
        final TableEditor editor = tables.editOrCreateTable(tableId);
        assertThat(editor).isInstanceOf(ExtendedTableEditor.class);

        editor.unwrap(ExtendedTableEditor.class).label("orders");
        tables.overwriteTable(editor.addColumn(column("id")).create());

        assertThat(tables.forTable(tableId).unwrap(ExtendedTable.class).label()).isEqualTo("orders");
    }

    @Test
    @FixFor("debezium/dbz#2843")
    public void shouldKeepSubclassWhenOverwritingTable() {
        tables.overwriteTable(labeledTable(tableId, "orders"));

        assertThat(tables.forTable(tableId).unwrap(ExtendedTable.class).label()).isEqualTo("orders");
        assertThat(tables.editTable(tableId)).isInstanceOf(ExtendedTableEditor.class);
    }

    @Test
    @FixFor("debezium/dbz#2843")
    public void shouldDetectChangeWhenOnlyExtensionChanges() {
        tables.overwriteTable(labeledTable(tableId, "orders"));
        tables.drainChanges();

        tables.overwriteTable(tableId, List.of(column("id")), List.of("id"), null, List.of());
        assertThat(tables.drainChanges()).containsExactly(tableId);
        assertThat(tables.forTable(tableId).unwrap(ExtendedTable.class).label()).isNull();
    }

    @Test
    @FixFor("debezium/dbz#2843")
    public void shouldKeepExtensionWhenRenamingTable() {
        tables.overwriteTable(labeledTable(tableId, "orders"));

        tables.renameTable(tableId, otherTableId);

        assertThat(tables.forTable(tableId)).isNull();
        final Table renamed = tables.forTable(otherTableId);
        assertThat(renamed.id()).isEqualTo(otherTableId);
        assertThat(renamed.unwrap(ExtendedTable.class).label()).isEqualTo("orders");
        assertThat(renamed.columnWithName("id")).isInstanceOf(ExtendedColumn.class);
    }

    @Test
    @FixFor("debezium/dbz#2843")
    public void shouldKeepExtensionWhenUpdatingTable() {
        tables.overwriteTable(labeledTable(tableId, "orders"));

        tables.updateTable(tableId, table -> table.edit().addColumn(column("name")).create());

        final Table updated = tables.forTable(tableId);
        assertThat(updated.columnWithName("name")).isInstanceOf(ExtendedColumn.class);
        assertThat(updated.unwrap(ExtendedTable.class).label()).isEqualTo("orders");
    }

    @Test
    @FixFor("debezium/dbz#2843")
    public void shouldKeepFactoryAndSubclassesWhenCloningAndFiltering() {
        tables.overwriteTable(labeledTable(tableId, "orders"));
        tables.overwriteTable(labeledTable(otherTableId, "customers"));

        final Tables clone = tables.clone();
        assertThat(clone.forTable(tableId).unwrap(ExtendedTable.class).label()).isEqualTo("orders");
        assertThat(clone.editOrCreateTable(new TableId("c", "s", "new"))).isInstanceOf(ExtendedTableEditor.class);

        final Tables subset = tables.subset(TableFilter.fromPredicate(tableId::equals));
        assertThat(subset.size()).isEqualTo(1);
        assertThat(subset.forTable(tableId).unwrap(ExtendedTable.class).label()).isEqualTo("orders");
        assertThat(subset.editOrCreateTable(otherTableId)).isInstanceOf(ExtendedTableEditor.class);
    }

    @Test
    @FixFor("debezium/dbz#2843")
    public void shouldTakeFactoryFromConnectorConfig() {
        final Configuration configuration = Configuration.create().with(CommonConnectorConfig.TOPIC_PREFIX, "test").build();

        final RelationalDatabaseConnectorConfig defaultConfig = new TestRelationalDatabaseConfig(configuration, null, null, 1);
        assertThat(defaultConfig.getRelationalMetadataFactory()).isSameAs(DefaultRelationalMetadataFactory.INSTANCE);
        assertThat(new Tables(false, defaultConfig).editOrCreateTable(tableId)).isExactlyInstanceOf(TableEditorImpl.class);

        final RelationalDatabaseConnectorConfig extendedConfig = new TestRelationalDatabaseConfig(configuration, null, null, 1) {
            @Override
            public RelationalMetadataFactory getRelationalMetadataFactory() {
                return ExtendedRelationalMetadataFactory.INSTANCE;
            }
        };
        assertThat(new Tables(false, extendedConfig).editOrCreateTable(tableId)).isInstanceOf(ExtendedTableEditor.class);
    }

    @Test
    @FixFor("debezium/dbz#2843")
    public void shouldNotInternExtendedTableAsEqualPlainTable() {
        // A plain table pooled first, as another connector sharing the pool would leave it
        final Table plain = Table.editor().tableId(tableId).addColumn(column("id")).setPrimaryKeyNames("id").create();
        final ExtendedTable table = labeledTable(tableId, "orders");

        assertThat(table).isNotSameAs(plain).isNotEqualTo(plain);
        assertThat(table.label()).isEqualTo("orders");
        assertThat(labeledTable(tableId, "orders")).isEqualTo(table).hasSameHashCodeAs(table);
    }

    private static ExtendedTable labeledTable(TableId id, String label) {
        final ExtendedTableEditor editor = ExtendedRelationalMetadataFactory.INSTANCE.tableEditor();
        editor.tableId(id).addColumn(column("id")).setPrimaryKeyNames("id");
        return editor.label(label).create();
    }

    private static ExtendedColumn column(String name) {
        return ExtendedRelationalMetadataFactory.INSTANCE.columnEditor()
                .region("us")
                .name(name).type("NUMBER").jdbcType(Types.NUMERIC).optional(false)
                .unwrap(ExtendedMetadata.ExtendedColumnEditor.class)
                .create();
    }
}
