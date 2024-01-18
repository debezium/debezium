/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.history;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Types;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Struct;
import org.junit.Test;

import io.debezium.document.Array;
import io.debezium.document.Document;
import io.debezium.document.DocumentReader;
import io.debezium.relational.Attribute;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges.TableChangesSerializer;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.util.Clock;
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
        String ddl = "CREATE TABLE foo ("
                + "  first VARCHAR(22) NOT NULL COMMENT 'first comment',"
                + "  second ENUM('1','2') NOT NULL DEFAULT '1'"
                + "  third VARCHAR(22) NULL DEFAULT '1' COMMENT 'third comment'"
                + ") COMMENT='table comment';";

        Table table = Table.editor()
                .tableId(new TableId(databaseName, schemaName, "foo"))
                .addColumn(Column.editor()
                        .name("first")
                        .jdbcType(Types.VARCHAR)
                        .type("VARCHAR")
                        .length(22)
                        .optional(false)
                        .comment("first comment")
                        .create())
                .addColumn(Column.editor()
                        .name("second")
                        .jdbcType(Types.INTEGER)
                        .type("ENUM", "ENUM")
                        .optional(false)
                        .defaultValueExpression("1")
                        .enumValues(Collect.arrayListOf("1", "2"))
                        .create())
                .addColumn(Column.editor()
                        .name("third")
                        .jdbcType(Types.VARCHAR)
                        .type("VARCHAR")
                        .length(22)
                        .optional(true)
                        .defaultValueExpression("1")
                        .comment("third comment")
                        .create())
                .setPrimaryKeyNames("first")
                .setComment("table comment")
                .addAttribute(Attribute.editor().name("object_id").value("12345").create())
                .addAttribute(Attribute.editor().name("other").value("test").create())
                .create();

        TableChanges tableChanges = new TableChanges().create(table);
        Instant timestamp = Clock.SYSTEM.currentTimeAsInstant();

        HistoryRecord record = new HistoryRecord(source, position, databaseName, schemaName, ddl, tableChanges, timestamp);

        String serialized = record.toString();
        DocumentReader reader = DocumentReader.defaultReader();
        HistoryRecord deserialized = new HistoryRecord(reader.read(serialized));

        assertThat((Comparable<Document>) deserialized.source()).isNotNull();
        assertThat(deserialized.source().get("server")).isEqualTo("abc");

        assertThat((Comparable<Document>) deserialized.position()).isNotNull();
        assertThat(deserialized.position().get("file")).isEqualTo("x.log");
        assertThat(deserialized.position().get("positionInt")).isEqualTo(100);
        assertThat(deserialized.position().get("positionLong")).isEqualTo(Long.MAX_VALUE);
        assertThat(deserialized.position().get("entry")).isEqualTo(1);
        assertThat(deserialized.timestamp()).isEqualTo(timestamp.toEpochMilli());

        assertThat(deserialized.databaseName()).isEqualTo(databaseName);
        assertThat(deserialized.schemaName()).isEqualTo(schemaName);
        assertThat(deserialized.ddl()).isEqualTo(ddl);

        Document secondColumn = deserialized.tableChanges()
                .get(0).asDocument()
                .getDocument("table")
                .getArray("columns")
                .get(1).asDocument();

        assertThat(secondColumn.get("defaultValueExpression")).isEqualTo("1");

        Document firstAttribute = deserialized.tableChanges()
                .get(0).asDocument()
                .getDocument("table")
                .getArray("attributes")
                .get(0).asDocument();

        assertThat(firstAttribute.get("name")).isEqualTo("object_id");
        assertThat(firstAttribute.get("value")).isEqualTo("12345");

        final TableChangesSerializer<Array> tableChangesSerializer = new JsonTableChangeSerializer();
        assertThat((Object) tableChangesSerializer.deserialize(deserialized.tableChanges(), true)).isEqualTo(tableChanges);

        final TableChangesSerializer<List<Struct>> connectTableChangeSerializer = new ConnectTableChangeSerializer(SchemaNameAdjuster.NO_OP);
        Struct struct = connectTableChangeSerializer.serialize(tableChanges).get(0);
        Struct tableStruct = (Struct) struct.get(ConnectTableChangeSerializer.TABLE_KEY);
        assertThat(tableStruct.get(ConnectTableChangeSerializer.COMMENT_KEY)).isEqualTo("table comment");
        List<Struct> columnStructs = (List<Struct>) tableStruct.get(ConnectTableChangeSerializer.COLUMNS_KEY);
        assertThat(columnStructs.get(0).get(ConnectTableChangeSerializer.COMMENT_KEY)).isEqualTo("first comment");
        assertThat(columnStructs.get(0).get(ConnectTableChangeSerializer.ENUM_VALUES)).isNull();
        assertThat(columnStructs.get(1).get(ConnectTableChangeSerializer.DEFAULT_VALUE_EXPRESSION)).isEqualTo("1");
        assertThat(columnStructs.get(1).get(ConnectTableChangeSerializer.ENUM_VALUES)).isEqualTo(Collect.arrayListOf("1", "2"));
    }
}
