/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.jdbc;

import static java.util.function.Predicate.not;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("UnitTests")
class RecordBufferTest {

    @Test
    @DisplayName("When 10 sink records arrives and buffer size is 5 then the buffer will be flushed 2 times")
    void correctlyBuffer() {

        JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(Map.of("batch.size", "5"));

        RecordBuffer recordBuffer = new RecordBuffer(config);

        SinkRecord record = new SinkRecord("topic", 0, Schema.STRING_SCHEMA, null, null, null, 1);
        List<SinkRecord> sinkRecords = IntStream.range(0, 10)
                .mapToObj(i -> record)
                .collect(Collectors.toList());

        List<List<SinkRecord>> batches = sinkRecords.stream().map(recordBuffer::add)
                .filter(not(List::isEmpty))
                .collect(Collectors.toList());

        assertThat(batches.size()).isEqualTo(2);

    }

    @Test
    @DisplayName("When key schema changes then the buffer will be flushed")
    void keySchemaChange() {

        JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(Map.of("batch.size", "5"));

        RecordBuffer recordBuffer = new RecordBuffer(config);

        SinkRecord record = new SinkRecord("topic", 0, Schema.STRING_SCHEMA, null, null, null, 1);
        List<SinkRecord> sinkRecords = IntStream.range(0, 3)
                .mapToObj(i -> record)
                .collect(Collectors.toList());

        sinkRecords.add(new SinkRecord("topic", 0, Schema.INT8_SCHEMA, null, null, null, 1));

        List<List<SinkRecord>> batches = sinkRecords.stream().map(recordBuffer::add)
                .filter(not(List::isEmpty))
                .collect(Collectors.toList());

        assertThat(batches.size()).isEqualTo(1);

    }

    @Test
    @DisplayName("When value schema changes then the buffer will be flushed")
    void valueSchemaChange() {

        JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(Map.of("batch.size", "5"));

        RecordBuffer recordBuffer = new RecordBuffer(config);

        SinkRecord record = new SinkRecord("topic", 0, Schema.INT8_SCHEMA, null, Schema.FLOAT64_SCHEMA, null, 1);
        List<SinkRecord> sinkRecords = IntStream.range(0, 3)
                .mapToObj(i -> record)
                .collect(Collectors.toList());

        sinkRecords.add(new SinkRecord("topic", 0, Schema.INT8_SCHEMA, null, Schema.INT64_SCHEMA, null, 1));

        List<List<SinkRecord>> batches = sinkRecords.stream().map(recordBuffer::add)
                .filter(not(List::isEmpty))
                .collect(Collectors.toList());

        assertThat(batches.size()).isEqualTo(1);

    }
}
