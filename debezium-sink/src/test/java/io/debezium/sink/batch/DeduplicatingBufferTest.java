/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.sink.batch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Set;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.bindings.kafka.KafkaDebeziumSinkRecord;
import io.debezium.metadata.CollectionId;
import io.debezium.sink.DebeziumSinkRecord;
import io.debezium.sink.SinkConnectorConfig;

class DeduplicatingBufferTest {

    private DeduplicatingBuffer buffer;
    private SinkConnectorConfig config;
    private CollectionId collectionId;

    @BeforeEach
    void setUp() {
        config = mock(SinkConnectorConfig.class);
        when(config.getBatchSize()).thenReturn(500);
        when(config.getPrimaryKeyMode()).thenReturn(SinkConnectorConfig.PrimaryKeyMode.RECORD_KEY);
        when(config.getPrimaryKeyFields()).thenReturn(Set.of());
        when(config.fieldFilter()).thenReturn(null);
        buffer = new DeduplicatingBuffer(config);
        collectionId = new CollectionId("test_table");
    }

    @Test
    void shouldDeduplicateRecordsWithSameKey() {
        KafkaDebeziumSinkRecord record1 = mockKafkaRecord("key1");
        KafkaDebeziumSinkRecord record2 = mockKafkaRecord("key1");

        buffer.enqueue(collectionId, record1);
        buffer.enqueue(collectionId, record2);

        assertThat(buffer.size()).isEqualTo(1);

        Batch batch = buffer.forcePoll();
        assertThat(batch).hasSize(1);
        assertThat(batch.get(0).record()).isSameAs(record2);
    }

    @Test
    void shouldKeepRecordsWithDifferentKeys() {
        KafkaDebeziumSinkRecord record1 = mockKafkaRecord("key1");
        KafkaDebeziumSinkRecord record2 = mockKafkaRecord("key2");

        buffer.enqueue(collectionId, record1);
        buffer.enqueue(collectionId, record2);

        assertThat(buffer.size()).isEqualTo(2);
    }

    @Test
    void shouldAcceptTruncateRecordWithoutKey() {
        DebeziumSinkRecord truncateRecord = mock(DebeziumSinkRecord.class);
        when(truncateRecord.isTruncate()).thenReturn(true);
        when(truncateRecord.key()).thenReturn(null);

        buffer.enqueue(collectionId, truncateRecord);

        assertThat(buffer.size()).isEqualTo(1);
        Batch batch = buffer.forcePoll();
        assertThat(batch.get(0).record()).isSameAs(truncateRecord);
    }

    @Test
    void shouldRejectNonKafkaRecordType() {
        DebeziumSinkRecord record = mock(DebeziumSinkRecord.class);
        when(record.isTruncate()).thenReturn(false);

        assertThatThrownBy(() -> buffer.enqueue(collectionId, record))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("DeduplicatingBuffer requires KafkaDebeziumRecord type record");
    }

    @Test
    void shouldRejectRecordWithoutStructKey() {
        KafkaDebeziumSinkRecord record = mock(KafkaDebeziumSinkRecord.class);
        when(record.isTruncate()).thenReturn(false);
        when(record.getFilteredKey(
                SinkConnectorConfig.PrimaryKeyMode.RECORD_KEY,
                Set.of(),
                null)).thenReturn(null);

        assertThatThrownBy(() -> buffer.enqueue(collectionId, record))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("No struct-based primary key defined");
    }

    @Test
    void truncateShouldClearExistingAndAddTruncateRecord() {
        KafkaDebeziumSinkRecord record1 = mockKafkaRecord("key1");
        KafkaDebeziumSinkRecord record2 = mockKafkaRecord("key2");

        buffer.enqueue(collectionId, record1);
        buffer.enqueue(collectionId, record2);
        assertThat(buffer.size()).isEqualTo(2);

        DebeziumSinkRecord truncateRecord = mock(DebeziumSinkRecord.class);
        when(truncateRecord.isTruncate()).thenReturn(true);
        when(truncateRecord.key()).thenReturn(null);

        buffer.truncate(collectionId, truncateRecord);

        assertThat(buffer.size()).isEqualTo(1);
        Batch batch = buffer.forcePoll();
        assertThat(batch.get(0).record()).isSameAs(truncateRecord);
    }

    private KafkaDebeziumSinkRecord mockKafkaRecord(Object key) {
        Schema keySchema = SchemaBuilder.struct().field("id", Schema.STRING_SCHEMA).build();
        Struct keyStruct = new Struct(keySchema).put("id", key.toString());

        KafkaDebeziumSinkRecord record = mock(KafkaDebeziumSinkRecord.class);
        when(record.isTruncate()).thenReturn(false);
        when(record.key()).thenReturn(key);
        when(record.getFilteredKey(
                SinkConnectorConfig.PrimaryKeyMode.RECORD_KEY,
                Set.of(),
                null)).thenReturn(keyStruct);
        return record;
    }
}
