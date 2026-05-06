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

import java.util.List;
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

class KeyedPassthroughBufferTest {

    private KeyedPassthroughBuffer buffer;
    private SinkConnectorConfig config;
    private CollectionId collectionId;

    @BeforeEach
    void setUp() {
        config = mock(SinkConnectorConfig.class);
        when(config.getBatchSize()).thenReturn(500);
        when(config.getPrimaryKeyMode()).thenReturn(SinkConnectorConfig.PrimaryKeyMode.RECORD_KEY);
        when(config.getPrimaryKeyFields()).thenReturn(Set.of());
        when(config.fieldFilter()).thenReturn(null);
        buffer = new KeyedPassthroughBuffer(config);
        collectionId = new CollectionId("test_table");
    }

    @Test
    void shouldPreserveAllRecordsWithSameKey() {
        KafkaDebeziumSinkRecord record1 = mockKafkaRecord("key1");
        KafkaDebeziumSinkRecord record2 = mockKafkaRecord("key1");

        buffer.enqueue(collectionId, record1);
        buffer.enqueue(collectionId, record2);

        assertThat(buffer.size()).isEqualTo(1);

        List<Batch> batches = buffer.forcePoll();
        assertThat(batches).hasSize(2);
        assertThat(batches.get(0)).hasSize(1);
        assertThat(batches.get(0).get(0).record()).isSameAs(record1);
        assertThat(batches.get(1)).hasSize(1);
        assertThat(batches.get(1).get(0).record()).isSameAs(record2);
    }

    @Test
    void shouldKeepRecordsWithDifferentKeysInSameBatch() {
        KafkaDebeziumSinkRecord record1 = mockKafkaRecord("key1");
        KafkaDebeziumSinkRecord record2 = mockKafkaRecord("key2");

        buffer.enqueue(collectionId, record1);
        buffer.enqueue(collectionId, record2);

        assertThat(buffer.size()).isEqualTo(2);

        List<Batch> batches = buffer.forcePoll();
        assertThat(batches).hasSize(1);
        assertThat(batches.get(0)).hasSize(2);
    }

    @Test
    void shouldSplitOnDuplicateKeyAndPreserveOrder() {
        KafkaDebeziumSinkRecord r1 = mockKafkaRecord("key1");
        KafkaDebeziumSinkRecord r2 = mockKafkaRecord("key2");
        KafkaDebeziumSinkRecord r3 = mockKafkaRecord("key1");
        KafkaDebeziumSinkRecord r4 = mockKafkaRecord("key2");

        buffer.enqueue(collectionId, r1);
        buffer.enqueue(collectionId, r2);
        buffer.enqueue(collectionId, r3);
        buffer.enqueue(collectionId, r4);

        // r3 triggers flush of {r1,r2}, then r3 and r4 go into fresh map (no duplicate)
        List<Batch> batches = buffer.forcePoll();
        assertThat(batches).hasSize(2);

        assertThat(batches.get(0)).hasSize(2);
        assertThat(batches.get(0).get(0).record()).isSameAs(r1);
        assertThat(batches.get(0).get(1).record()).isSameAs(r2);

        assertThat(batches.get(1)).hasSize(2);
        assertThat(batches.get(1).get(0).record()).isSameAs(r3);
        assertThat(batches.get(1).get(1).record()).isSameAs(r4);
    }

    @Test
    void pollShouldReturnCompletedBatches() {
        KafkaDebeziumSinkRecord r1 = mockKafkaRecord("key1");
        KafkaDebeziumSinkRecord r2 = mockKafkaRecord("key1");

        buffer.enqueue(collectionId, r1);
        buffer.enqueue(collectionId, r2);

        List<Batch> batches = buffer.poll();
        assertThat(batches).hasSize(1);
        assertThat(batches.get(0).get(0).record()).isSameAs(r1);
    }

    @Test
    void pollShouldReturnEmptyWhenNoDuplicatesAndBelowBatchSize() {
        KafkaDebeziumSinkRecord r1 = mockKafkaRecord("key1");
        KafkaDebeziumSinkRecord r2 = mockKafkaRecord("key2");

        buffer.enqueue(collectionId, r1);
        buffer.enqueue(collectionId, r2);

        List<Batch> batches = buffer.poll();
        assertThat(batches).isEmpty();
    }

    @Test
    void shouldRespectBatchSizeWhenSplitting() {
        SinkConnectorConfig smallBatchConfig = mock(SinkConnectorConfig.class);
        when(smallBatchConfig.getBatchSize()).thenReturn(2);
        when(smallBatchConfig.getPrimaryKeyMode()).thenReturn(SinkConnectorConfig.PrimaryKeyMode.RECORD_KEY);
        when(smallBatchConfig.getPrimaryKeyFields()).thenReturn(Set.of());
        when(smallBatchConfig.fieldFilter()).thenReturn(null);
        KeyedPassthroughBuffer smallBuffer = new KeyedPassthroughBuffer(smallBatchConfig);

        KafkaDebeziumSinkRecord r1 = mockKafkaRecord("key1");
        KafkaDebeziumSinkRecord r2 = mockKafkaRecord("key2");
        KafkaDebeziumSinkRecord r3 = mockKafkaRecord("key3");
        KafkaDebeziumSinkRecord r4 = mockKafkaRecord("key1");

        smallBuffer.enqueue(collectionId, r1);
        smallBuffer.enqueue(collectionId, r2);
        smallBuffer.enqueue(collectionId, r3);
        // r4 triggers flush of {r1,r2,r3}, split into [{r1,r2},{r3}] by batchSize=2
        smallBuffer.enqueue(collectionId, r4);

        List<Batch> batches = smallBuffer.forcePoll();
        // 3 batches: [{r1,r2}, {r3}] from split + [{r4}] from remaining
        assertThat(batches).hasSize(3);
        assertThat(batches.get(0)).hasSize(2);
        assertThat(batches.get(0).get(0).record()).isSameAs(r1);
        assertThat(batches.get(0).get(1).record()).isSameAs(r2);
        assertThat(batches.get(1)).hasSize(1);
        assertThat(batches.get(1).get(0).record()).isSameAs(r3);
        assertThat(batches.get(2)).hasSize(1);
        assertThat(batches.get(2).get(0).record()).isSameAs(r4);
    }

    @Test
    void shouldAcceptTruncateRecordWithoutKey() {
        DebeziumSinkRecord truncateRecord = mock(DebeziumSinkRecord.class);
        when(truncateRecord.isTruncate()).thenReturn(true);
        when(truncateRecord.key()).thenReturn(null);

        buffer.enqueue(collectionId, truncateRecord);

        assertThat(buffer.size()).isEqualTo(1);
        List<Batch> batches = buffer.forcePoll();
        assertThat(batches.get(0).get(0).record()).isSameAs(truncateRecord);
    }

    @Test
    void shouldRejectNonKafkaRecordType() {
        DebeziumSinkRecord record = mock(DebeziumSinkRecord.class);
        when(record.isTruncate()).thenReturn(false);

        assertThatThrownBy(() -> buffer.enqueue(collectionId, record))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Keyed buffer requires KafkaDebeziumRecord type record");
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
    void truncateShouldClearFromCompletedBatchesAndRecords() {
        KafkaDebeziumSinkRecord record1 = mockKafkaRecord("key1");
        KafkaDebeziumSinkRecord record2 = mockKafkaRecord("key1");
        KafkaDebeziumSinkRecord record3 = mockKafkaRecord("key1");

        buffer.enqueue(collectionId, record1);
        buffer.enqueue(collectionId, record2);
        buffer.enqueue(collectionId, record3);

        DebeziumSinkRecord truncateRecord = mock(DebeziumSinkRecord.class);
        when(truncateRecord.isTruncate()).thenReturn(true);
        when(truncateRecord.key()).thenReturn(null);

        buffer.truncate(collectionId, truncateRecord);

        List<Batch> batches = buffer.forcePoll();
        assertThat(batches).hasSize(1);
        assertThat(batches.get(0)).hasSize(1);
        assertThat(batches.get(0).get(0).record()).isSameAs(truncateRecord);
    }

    @Test
    void truncateShouldNotAffectOtherCollectionsInCompletedBatches() {
        CollectionId table1 = new CollectionId("table1");
        CollectionId table2 = new CollectionId("table2");
        KafkaDebeziumSinkRecord r1 = mockKafkaRecord("key1");
        KafkaDebeziumSinkRecord r2 = mockKafkaRecord("key1");

        buffer.enqueue(table1, r1);
        buffer.enqueue(table2, mockKafkaRecord("key2"));
        buffer.enqueue(table1, r2);

        DebeziumSinkRecord truncateRecord = mock(DebeziumSinkRecord.class);
        when(truncateRecord.isTruncate()).thenReturn(true);
        when(truncateRecord.key()).thenReturn(null);

        buffer.truncate(table1, truncateRecord);

        List<Batch> batches = buffer.forcePoll();
        long table2Records = batches.stream()
                .flatMap(batch -> batch.stream())
                .filter(br -> br.collectionId().equals(table2))
                .count();
        assertThat(table2Records).isEqualTo(1);
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
