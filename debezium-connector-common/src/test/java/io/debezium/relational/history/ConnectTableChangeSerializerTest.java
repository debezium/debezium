/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.history;

import static io.debezium.relational.ExtendedMetadata.EXTENDED_KEY;
import static io.debezium.relational.ExtendedMetadata.LABEL_KEY;
import static io.debezium.relational.ExtendedMetadata.REGION_KEY;
import static io.debezium.relational.history.ConnectTableChangeSerializer.COLUMNS_KEY;
import static io.debezium.relational.history.ConnectTableChangeSerializer.NAME_KEY;
import static io.debezium.relational.history.ConnectTableChangeSerializer.TABLE_KEY;
import static io.debezium.relational.history.ConnectTableChangeSerializer.TYPE_NAME_KEY;
import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Types;
import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;

import io.debezium.doc.FixFor;
import io.debezium.relational.Column;
import io.debezium.relational.ExtendedMetadata;
import io.debezium.relational.ExtendedMetadata.ExtendedRelationalMetadataFactory;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.schema.SchemaNameAdjuster;

/**
 * Verifies that {@link ConnectTableChangeSerializer} keeps its base struct layout and that a subclass built
 * with its own {@code SchemaFactory} can add a nested struct through the extension hooks.
 *
 * @author Chris Cranford
 */
public class ConnectTableChangeSerializerTest {

    private final TableId tableId = new TableId("db", "schema", "orders");

    @Test
    @FixFor("debezium/dbz#2843")
    public void shouldSerializeBaseFieldsWithDefaultSchema() {
        final ConnectTableChangeSerializer serializer = new ConnectTableChangeSerializer(SchemaNameAdjuster.NO_OP);
        final Table table = Table.editor()
                .tableId(tableId)
                .addColumn(Column.editor().name("id").type("NUMBER").jdbcType(Types.NUMERIC).optional(false).create())
                .setPrimaryKeyNames("id")
                .create();

        final List<Struct> changes = serializer.serialize(new TableChanges().create(table));

        final Struct tableStruct = changes.get(0).getStruct(TABLE_KEY);
        assertThat(tableStruct.schema().field(EXTENDED_KEY)).isNull();
        final Struct columnStruct = columnStruct(tableStruct);
        assertThat(columnStruct.schema().field(EXTENDED_KEY)).isNull();
        assertThat(columnStruct.getString(NAME_KEY)).isEqualTo("id");
        assertThat(columnStruct.getString(TYPE_NAME_KEY)).isEqualTo("NUMBER");
    }

    @Test
    @FixFor("debezium/dbz#2843")
    public void shouldWriteExtensionsIntoNestedStruct() {
        final ConnectTableChangeSerializer serializer = ExtendedRelationalMetadataFactory.INSTANCE.connectTableChangesSerializer(SchemaNameAdjuster.NO_OP);

        final List<Struct> changes = serializer.serialize(new TableChanges().create(ExtendedMetadata.table(tableId, "orders", "us")));

        final Struct tableStruct = changes.get(0).getStruct(TABLE_KEY);
        assertThat(tableStruct.getStruct(EXTENDED_KEY).getString(LABEL_KEY)).isEqualTo("orders");
        final Struct columnStruct = columnStruct(tableStruct);
        assertThat(columnStruct.getString(NAME_KEY)).isEqualTo("id");
        assertThat(columnStruct.getStruct(EXTENDED_KEY).getString(REGION_KEY)).isEqualTo("us");

        // the change schema the dispatcher publishes carries the nested field as well
        final Schema columnSchema = serializer.getChangeSchema().field(TABLE_KEY).schema().field(COLUMNS_KEY).schema().valueSchema();
        assertThat(columnSchema.field(EXTENDED_KEY)).isNotNull();
        assertThat(columnSchema.field(EXTENDED_KEY).schema().field(REGION_KEY)).isNotNull();
    }

    private static Struct columnStruct(Struct tableStruct) {
        final List<Struct> columns = tableStruct.getArray(COLUMNS_KEY);
        return columns.get(0);
    }
}
