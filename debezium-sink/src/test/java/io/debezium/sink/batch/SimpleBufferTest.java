/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.sink.batch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.metadata.CollectionId;
import io.debezium.sink.DebeziumSinkRecord;
import io.debezium.sink.SinkConnectorConfig;

class SimpleBufferTest {

    private SimpleBuffer buffer;
    private CollectionId collectionId;

    @BeforeEach
    void setUp() {
        SinkConnectorConfig config = mock(SinkConnectorConfig.class);
        when(config.getBatchSize()).thenReturn(500);
        buffer = new SimpleBuffer(config);
        collectionId = new CollectionId("test_table");
    }

    @Test
    void shouldPreserveAllRecordsWithSameKey() {
        DebeziumSinkRecord record1 = mockRecord(null);
        DebeziumSinkRecord record2 = mockRecord(null);
        DebeziumSinkRecord record3 = mockRecord(null);

        buffer.enqueue(collectionId, record1);
        buffer.enqueue(collectionId, record2);
        buffer.enqueue(collectionId, record3);

        assertThat(buffer.size()).isEqualTo(3);

        Batch batch = buffer.forcePoll();
        assertThat(batch).hasSize(3);
        assertThat(batch.get(0).record()).isSameAs(record1);
        assertThat(batch.get(1).record()).isSameAs(record2);
        assertThat(batch.get(2).record()).isSameAs(record3);
    }

    @Test
    void shouldPreserveRecordsAcrossMultipleTables() {
        CollectionId table1 = new CollectionId("table1");
        CollectionId table2 = new CollectionId("table2");
        DebeziumSinkRecord record1 = mockRecord(null);
        DebeziumSinkRecord record2 = mockRecord(null);

        buffer.enqueue(table1, record1);
        buffer.enqueue(table2, record2);

        assertThat(buffer.size()).isEqualTo(2);

        Batch batch = buffer.forcePoll();
        assertThat(batch).hasSize(2);
        assertThat(batch.get(0).collectionId()).isEqualTo(table1);
        assertThat(batch.get(1).collectionId()).isEqualTo(table2);
    }

    @Test
    void pollShouldReturnEmptyWhenBelowBatchSize() {
        buffer.enqueue(collectionId, mockRecord(null));

        Batch batch = buffer.poll();
        assertThat(batch).isEmpty();
        assertThat(buffer.size()).isEqualTo(1);
    }

    @Test
    void forcePollShouldDrainBuffer() {
        buffer.enqueue(collectionId, mockRecord(null));
        buffer.enqueue(collectionId, mockRecord(null));

        Batch batch = buffer.forcePoll();
        assertThat(batch).hasSize(2);
        assertThat(buffer.size()).isEqualTo(0);
    }

    @Test
    void forcePollOnEmptyBufferShouldReturnEmptyBatch() {
        Batch batch = buffer.forcePoll();
        assertThat(batch).isEmpty();
    }

    @Test
    void truncateShouldClearRecordsForCollection() {
        DebeziumSinkRecord record1 = mockRecord("key1");
        DebeziumSinkRecord record2 = mockRecord("key2");
        DebeziumSinkRecord truncateRecord = mockRecord(null);
        when(truncateRecord.isTruncate()).thenReturn(true);

        buffer.enqueue(collectionId, record1);
        buffer.enqueue(collectionId, record2);
        assertThat(buffer.size()).isEqualTo(2);

        buffer.truncate(collectionId, truncateRecord);
        assertThat(buffer.size()).isEqualTo(1);

        Batch batch = buffer.forcePoll();
        assertThat(batch.get(0).record()).isSameAs(truncateRecord);
    }

    @Test
    void truncateShouldNotAffectOtherCollections() {
        CollectionId table1 = new CollectionId("table1");
        CollectionId table2 = new CollectionId("table2");
        DebeziumSinkRecord record1 = mockRecord(null);
        DebeziumSinkRecord record2 = mockRecord(null);
        DebeziumSinkRecord truncateRecord = mockRecord(null);
        when(truncateRecord.isTruncate()).thenReturn(true);

        buffer.enqueue(table1, record1);
        buffer.enqueue(table2, record2);

        buffer.truncate(table1, truncateRecord);

        assertThat(buffer.size()).isEqualTo(2);

        Batch batch = buffer.forcePoll();
        assertThat(batch.get(0).collectionId()).isEqualTo(table2);
        assertThat(batch.get(1).collectionId()).isEqualTo(table1);
        assertThat(batch.get(1).record()).isSameAs(truncateRecord);
    }

    private DebeziumSinkRecord mockRecord(Object key) {
        DebeziumSinkRecord record = mock(DebeziumSinkRecord.class);
        when(record.key()).thenReturn(key);
        return record;
    }
}
