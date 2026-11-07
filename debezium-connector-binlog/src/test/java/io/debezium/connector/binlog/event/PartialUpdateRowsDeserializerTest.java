/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog.event;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import com.github.shyiko.mysql.binlog.event.deserialization.ColumnType;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;

import io.debezium.config.CommonConnectorConfig.EventProcessingFailureHandlingMode;
import io.debezium.connector.binlog.jdbc.BinlogValueConverters;
import io.debezium.doc.FixFor;

/**
 * Unit tests for {@link RowDeserializers.PartialUpdateRowsDeserializer}. The event bodies were written with
 * {@code binlog_row_image=FULL} and {@code binlog_row_value_options=PARTIAL_JSON}.
 */
class PartialUpdateRowsDeserializerTest {

    // CREATE TABLE t_plain (id INT PRIMARY KEY, name VARCHAR(20))
    private static final long PLAIN_TABLE_ID = 115;
    // CREATE TABLE t_json (id BIGINT PRIMARY KEY, v INT, j JSON)
    private static final long JSON_TABLE_ID = 116;
    // CREATE TABLE t_part (id INT PRIMARY KEY, name VARCHAR(20)) PARTITION BY HASH(id) PARTITIONS 2
    private static final long PARTITIONED_TABLE_ID = 117;

    // {"a": 3, "b": 2} in MySQL binary JSON format
    private static final String DOCUMENT = "00 02 00 14 00 12 00 01 00 13 00 01 00 05 03 00 05 02 00 61 62";

    @Test
    @FixFor("debezium/dbz#2958")
    void shouldDeserializeRowsOfTableWithoutJsonColumns() throws IOException {
        // UPDATE t_plain SET name = CONCAT(name, 'x'), updating (1, 'a1') and (2, 'b')
        UpdateRowsEventData data = deserialize(PLAIN_TABLE_ID, "73 00 00 00 00 00 01 00 02 00",
                "02 ff ff 00 01 00 00 00 02 61 31 00 00 01 00 00 00 03 61 31 78 00 02 00 00 00 01 62 00 00 02 00 00 00 02 62 78");

        List<Map.Entry<Serializable[], Serializable[]>> rows = data.getRows();
        assertThat(rows).hasSize(2);
        assertThat(rows.get(0).getKey()).containsExactly(1, bytes("a1"));
        assertThat(rows.get(0).getValue()).containsExactly(1, bytes("a1x"));
        assertThat(rows.get(1).getKey()).containsExactly(2, bytes("b"));
        assertThat(rows.get(1).getValue()).containsExactly(2, bytes("bx"));
    }

    @Test
    @FixFor("debezium/dbz#2958")
    void shouldDeserializeRowsOfPartitionedTable() throws IOException {
        // UPDATE t_part SET name = 'p1' WHERE id = 1, with partition information in the extra data
        UpdateRowsEventData data = deserialize(PARTITIONED_TABLE_ID, "75 00 00 00 00 00 01 00 07 00 01 01 00 01 00",
                "02 ff ff 00 01 00 00 00 01 70 00 00 01 00 00 00 02 70 31");

        assertThat(data.getRows()).hasSize(1);
        assertThat(data.getRows().get(0).getKey()).containsExactly(1, bytes("p"));
        assertThat(data.getRows().get(0).getValue()).containsExactly(1, bytes("p1"));
    }

    @Test
    @FixFor("debezium/dbz#2958")
    void shouldTakeUnmodifiedJsonColumnFromBeforeImage() throws IOException {
        // UPDATE t_json SET v = v + 1 WHERE id = 1, which logs the JSON column as an empty diff
        UpdateRowsEventData data = deserialize(JSON_TABLE_ID, "74 00 00 00 00 00 01 00 02 00",
                "03 ff ff 00 01 00 00 00 00 00 00 00 0a 00 00 00 15 00 00 00 " + DOCUMENT
                        + " 01 01 00 01 00 00 00 00 00 00 00 0b 00 00 00 00 00 00 00");

        assertThat(data.getRows()).hasSize(1);
        assertThat(data.getRows().get(0).getKey()).containsExactly(1L, 10, hex(DOCUMENT));
        assertThat(data.getRows().get(0).getValue()).containsExactly(1L, 11, hex(DOCUMENT));
    }

    @Test
    @FixFor("debezium/dbz#2958")
    void shouldDeserializeJsonColumnReplacedWithNewDocument() throws IOException {
        // UPDATE t_json SET j = '{"x": 1}' WHERE id = 1, which logs the full document
        String newDocument = "00 01 00 0c 00 0b 00 01 00 05 01 00 78";
        UpdateRowsEventData data = deserialize(JSON_TABLE_ID, "74 00 00 00 00 00 01 00 02 00",
                "03 ff ff 00 01 00 00 00 00 00 00 00 0b 00 00 00 15 00 00 00 00 02 00 14 00 12 00 01 00 13 00 01 00 05 5b 04 05 02 00 61 62"
                        + " 00 00 01 00 00 00 00 00 00 00 0b 00 00 00 0d 00 00 00 " + newDocument);

        assertThat(data.getRows().get(0).getValue()).containsExactly(1L, 11, hex(newDocument));
    }

    @Test
    @FixFor("debezium/dbz#2958")
    void shouldMarkJsonDiffAsUnavailable() throws IOException {
        // UPDATE t_json SET j = JSON_REPLACE(j, '$.a', 1115) WHERE id = 1
        UpdateRowsEventData data = deserialize(JSON_TABLE_ID, "74 00 00 00 00 00 01 00 02 00",
                "03 ff ff 00 01 00 00 00 00 00 00 00 0b 00 00 00 15 00 00 00 " + DOCUMENT
                        + " 01 01 00 01 00 00 00 00 00 00 00 0b 00 00 00 09 00 00 00 00 03 24 2e 61 03 05 5b 04");

        assertThat(data.getRows()).hasSize(1);
        assertThat(data.getRows().get(0).getKey()).containsExactly(1L, 11, hex(DOCUMENT));
        assertThat(data.getRows().get(0).getValue()).containsExactly(1L, 11, BinlogValueConverters.UNAVAILABLE_VALUE);
    }

    @Test
    @FixFor("debezium/dbz#2958")
    void shouldMarkJsonDiffsOfAllRowsAsUnavailable() throws IOException {
        // UPDATE t_json SET j = JSON_SET(j, '$.b', 7), v = v + 1, updating two rows
        UpdateRowsEventData data = deserialize(JSON_TABLE_ID, "74 00 00 00 00 00 01 00 02 00",
                "03 ff ff 00 01 00 00 00 00 00 00 00 0b 00 00 00 0d 00 00 00 00 01 00 0c 00 0b 00 01 00 05 01 00 78"
                        + " 01 01 00 01 00 00 00 00 00 00 00 0c 00 00 00 09 00 00 00 01 03 24 2e 62 03 05 07 00"
                        + " 00 02 00 00 00 00 00 00 00 14 00 00 00 15 00 00 00 00 02 00 14 00 12 00 01 00 13 00 01 00 05 05 00 05 06 00 61 62"
                        + " 01 01 00 02 00 00 00 00 00 00 00 15 00 00 00 09 00 00 00 00 03 24 2e 62 03 05 07 00");

        assertThat(data.getRows()).hasSize(2);
        assertThat(data.getRows().get(0).getValue()).containsExactly(1L, 12, BinlogValueConverters.UNAVAILABLE_VALUE);
        assertThat(data.getRows().get(1).getValue()).containsExactly(2L, 21, BinlogValueConverters.UNAVAILABLE_VALUE);
    }

    private static UpdateRowsEventData deserialize(long tableId, String header, String rows) throws IOException {
        TableMapEventData tableMap = new TableMapEventData();
        tableMap.setTableId(tableId);
        if (tableId == JSON_TABLE_ID) {
            tableMap.setColumnTypes(new byte[]{ (byte) ColumnType.LONGLONG.getCode(), (byte) ColumnType.LONG.getCode(), (byte) ColumnType.JSON.getCode() });
            tableMap.setColumnMetadata(new int[]{ 0, 0, 4 });
        }
        else {
            tableMap.setColumnTypes(new byte[]{ (byte) ColumnType.LONG.getCode(), (byte) ColumnType.VARCHAR.getCode() });
            tableMap.setColumnMetadata(new int[]{ 0, 80 });
        }
        RowDeserializers.PartialUpdateRowsDeserializer deserializer = new RowDeserializers.PartialUpdateRowsDeserializer(Map.of(tableId, tableMap),
                EventProcessingFailureHandlingMode.FAIL, false);

        ByteArrayInputStream input = new ByteArrayInputStream(hex(header + " " + rows));
        UpdateRowsEventData data = deserializer.deserialize(input);
        assertThat(data.getTableId()).isEqualTo(tableId);
        assertThat(input.available()).isZero();
        return data;
    }

    private static byte[] hex(String bytes) {
        return HexFormat.of().parseHex(bytes.replace(" ", ""));
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
