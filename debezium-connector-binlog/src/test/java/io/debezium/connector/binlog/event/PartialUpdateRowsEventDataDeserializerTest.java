/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog.event;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.HexFormat;

import org.junit.jupiter.api.Test;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.XidEventData;
import com.github.shyiko.mysql.binlog.event.deserialization.ChecksumType;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;

import io.debezium.doc.FixFor;

/**
 * Unit tests for {@link PartialUpdateRowsEventDataDeserializer}. Each event was written with
 * {@code binlog_row_value_options=PARTIAL_JSON} and is followed by the {@code XID} event of its transaction.
 */
class PartialUpdateRowsEventDataDeserializerTest {

    @Test
    @FixFor("debezium/dbz#2958")
    void shouldReadTableIdOfJsonDiffEvent() throws IOException {
        // UPDATE t_json SET j = JSON_REPLACE(j, '$.a', 1115) WHERE id = 1, table id 116
        assertTableIdAndNextEvent(116, 15465164,
                "b7bce16a278929000066000000ee6bb80000007400000000000100020003ffff0001000000000000000b000000150000000002"
                        + "0014001200010013000100050300050200616201010001000000000000000b000000090000000003242e6103055b04a1023a7d",
                "b7bce16a10892900001f0000000d6cb8000000ccfaeb0000000000a29378b9");
    }

    @Test
    @FixFor("debezium/dbz#2958")
    void shouldReadTableIdOfPartitionedTableEvent() throws IOException {
        // UPDATE t_part SET name = 'p1' WHERE id = 1, table id 117, with partition information in the extra data
        assertTableIdAndNextEvent(117, 15465170,
                "b7bce16a2789290000390000009470b800000075000000000001000700010100010002ffff00010000000170000001000000027031246be891",
                "b7bce16a10892900001f000000b370b8000000d2faeb00000000004c51ca1d");
    }

    private static void assertTableIdAndNextEvent(long tableId, long xid, String partialUpdateEvent, String xidEvent) throws IOException {
        final var deserializer = new EventDeserializer();
        deserializer.setChecksumType(ChecksumType.CRC32);
        deserializer.setEventDataDeserializer(EventType.PARTIAL_UPDATE_ROWS_EVENT, new PartialUpdateRowsEventDataDeserializer());
        final var input = new ByteArrayInputStream(HexFormat.of().parseHex(partialUpdateEvent + xidEvent));

        final Event event = deserializer.nextEvent(input);
        assertThat(event.getHeader().getEventType()).isEqualTo(EventType.PARTIAL_UPDATE_ROWS_EVENT);
        assertThat(((PartialUpdateRowsEventData) event.getData()).getTableId()).isEqualTo(tableId);

        // The rest of the event body is skipped, so the following event is read correctly
        final Event next = deserializer.nextEvent(input);
        assertThat(next.getHeader().getEventType()).isEqualTo(EventType.XID);
        assertThat(((XidEventData) next.getData()).getXid()).isEqualTo(xid);
    }
}
