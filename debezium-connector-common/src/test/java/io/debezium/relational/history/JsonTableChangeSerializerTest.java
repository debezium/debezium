/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.history;

import static io.debezium.relational.ExtendedMetadata.EXTENDED_KEY;
import static io.debezium.relational.ExtendedMetadata.LABEL_KEY;
import static io.debezium.relational.ExtendedMetadata.REGION_KEY;
import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Types;
import java.util.Iterator;

import org.junit.jupiter.api.Test;

import io.debezium.doc.FixFor;
import io.debezium.document.Array;
import io.debezium.document.Document;
import io.debezium.relational.Column;
import io.debezium.relational.ExtendedMetadata;
import io.debezium.relational.ExtendedMetadata.ExtendedColumn;
import io.debezium.relational.ExtendedMetadata.ExtendedRelationalMetadataFactory;
import io.debezium.relational.ExtendedMetadata.ExtendedTable;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.TableImpl;
import io.debezium.relational.history.TableChanges.TableChange;
import io.debezium.relational.history.TableChanges.TableChangeType;

/**
 * Verifies that {@link JsonTableChangeSerializer} rebuilds tables through its metadata factory and that a
 * subclass can persist connector-specific metadata through the extension hooks without touching the base layout.
 *
 * @author Chris Cranford
 */
public class JsonTableChangeSerializerTest {

    private final TableId tableId = new TableId("db", "schema", "orders");
    private final TableId renamedId = new TableId("db", "schema", "orders_v2");

    @Test
    @FixFor("debezium/dbz#2843")
    public void shouldRoundTripBaseFieldsWithDefaultSerializer() {
        final Table table = baseTable();
        final JsonTableChangeSerializer serializer = new JsonTableChangeSerializer();

        final Array serialized = serializer.serialize(new TableChanges().create(table));
        assertThat(tableDocument(serialized).has(EXTENDED_KEY)).isFalse();
        assertThat(columnDocument(serialized).has(EXTENDED_KEY)).isFalse();

        final TableChange change = serializer.deserialize(serialized, false).iterator().next();
        assertThat(change.getType()).isEqualTo(TableChangeType.CREATE);
        assertThat(change.getTable()).isExactlyInstanceOf(TableImpl.class);
        assertThat(change.getTable()).isEqualTo(table);
    }

    @Test
    @FixFor("debezium/dbz#2843")
    public void shouldRoundTripExtensionsThroughSubclassSerializer() {
        final JsonTableChangeSerializer serializer = ExtendedRelationalMetadataFactory.INSTANCE.jsonTableChangesSerializer();
        final ExtendedTable table = ExtendedMetadata.table(tableId, "orders", "us");
        final Table renamed = table.edit().tableId(renamedId).create();

        final Array serialized = serializer.serialize(new TableChanges().create(table).rename(renamed, tableId).drop(renamedId));

        // the extension fields nest under one entry and the base fields keep their keys
        final Document column = columnDocument(serialized);
        assertThat(column.getString("name")).isEqualTo("id");
        assertThat(column.getInteger("jdbcType")).isEqualTo(Types.NUMERIC);
        assertThat(column.getDocument(EXTENDED_KEY).getString(REGION_KEY)).isEqualTo("us");
        assertThat(tableDocument(serialized).getDocument(EXTENDED_KEY).getString(LABEL_KEY)).isEqualTo("orders");

        final Iterator<TableChange> changes = serializer.deserialize(serialized, false).iterator();

        final TableChange create = changes.next();
        assertThat(create.getType()).isEqualTo(TableChangeType.CREATE);
        assertThat(create.getTable()).isEqualTo(table);
        assertThat(create.getTable().unwrap(ExtendedTable.class).label()).isEqualTo("orders");
        assertThat(create.getTable().columnWithName("id").unwrap(ExtendedColumn.class).region()).isEqualTo("us");

        final TableChange rename = changes.next();
        assertThat(rename.getType()).isEqualTo(TableChangeType.ALTER);
        assertThat(rename.getPreviousId()).isEqualTo(tableId);
        assertThat(rename.getTable().id()).isEqualTo(renamedId);
        assertThat(rename.getTable().unwrap(ExtendedTable.class).label()).isEqualTo("orders");

        final TableChange drop = changes.next();
        assertThat(drop.getType()).isEqualTo(TableChangeType.DROP);
        assertThat(drop.getId()).isEqualTo(renamedId);
        assertThat(changes.hasNext()).isFalse();
    }

    @Test
    @FixFor("debezium/dbz#2843")
    public void shouldLeaveEditorDefaultsWhenExtensionEntryIsMissing() {
        // a record written before the connector persisted extensions has no nested entry
        final Array serialized = new JsonTableChangeSerializer().serialize(new TableChanges().create(baseTable()));

        final TableChange change = ExtendedRelationalMetadataFactory.INSTANCE.jsonTableChangesSerializer()
                .deserialize(serialized, false).iterator().next();

        final Table table = change.getTable();
        assertThat(table).isInstanceOf(ExtendedTable.class);
        assertThat(table.unwrap(ExtendedTable.class).label()).isNull();
        assertThat(table.primaryKeyColumnNames()).containsExactly("id");

        final Column column = table.columnWithName("id");
        assertThat(column).isInstanceOf(ExtendedColumn.class);
        assertThat(column.unwrap(ExtendedColumn.class).region()).isNull();
        assertThat(column.typeName()).isEqualTo("NUMBER");
        assertThat(column.length()).isEqualTo(10);
    }

    private Table baseTable() {
        return Table.editor()
                .tableId(tableId)
                .addColumn(Column.editor().name("id").type("NUMBER").jdbcType(Types.NUMERIC).length(10).optional(false).create())
                .setPrimaryKeyNames("id")
                .create();
    }

    private static Document tableDocument(Array serialized) {
        return serialized.get(0).asDocument().getDocument("table");
    }

    private static Document columnDocument(Array serialized) {
        return tableDocument(serialized).getArray("columns").get(0).asDocument();
    }
}
