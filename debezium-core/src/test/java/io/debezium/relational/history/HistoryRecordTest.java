/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.history;

import static org.fest.assertions.Assertions.assertThat;

import java.sql.Types;
import java.util.Map;

import org.junit.Test;

import io.debezium.document.Array;
import io.debezium.document.DocumentReader;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges.TableChangesSerializer;
import io.debezium.util.Collect;

/**
 * @author Randall Hauch
 *
 */
public class HistoryRecordTest {

    @Test
    public void canSerializeAndDeserializeHistoryRecord() throws Exception {
        Map<String, Object> source = Collect.linkMapOf("server", "abc");
        Map<String, Object> position = Collect.linkMapOf("file", "x.log", "positionInt", 100, "positionLong", Long.MAX_VALUE, "entry", 1);
        String databaseName = "db";
        String schemaName = "myschema";
        String ddl = "CREATE TABLE foo ( first VARCHAR(22) NOT NULL );";

        Table table = Table.editor()
                .tableId(new TableId(databaseName, schemaName, "foo"))
                .addColumn(Column.editor()
                        .name("first")
                        .jdbcType(Types.VARCHAR)
                        .type("VARCHAR")
                        .length(22)
                        .optional(false)
                        .create())
                .setPrimaryKeyNames("first")
                .create();

        TableChanges tableChanges = new TableChanges().create(table);

        HistoryRecord record = new HistoryRecord(source, position, databaseName, schemaName, ddl, tableChanges);

        String serialized = record.toString();
        DocumentReader reader = DocumentReader.defaultReader();
        HistoryRecord deserialized = new HistoryRecord(reader.read(serialized));

        assertThat(deserialized.source()).isNotNull();
        assertThat(deserialized.source().get("server")).isEqualTo("abc");

        assertThat(deserialized.position()).isNotNull();
        assertThat(deserialized.position().get("file")).isEqualTo("x.log");
        assertThat(deserialized.position().get("positionInt")).isEqualTo(100);
        assertThat(deserialized.position().get("positionLong")).isEqualTo(Long.MAX_VALUE);
        assertThat(deserialized.position().get("entry")).isEqualTo(1);

        assertThat(deserialized.databaseName()).isEqualTo(databaseName);
        assertThat(deserialized.schemaName()).isEqualTo(schemaName);
        assertThat(deserialized.ddl()).isEqualTo(ddl);

        System.out.println(record);
        final TableChangesSerializer<Array> tableChangesSerializer = new JsonTableChangeSerializer();
        assertThat((Object) tableChangesSerializer.deserialize(deserialized.tableChanges(), true)).isEqualTo(tableChanges);

    }
}
