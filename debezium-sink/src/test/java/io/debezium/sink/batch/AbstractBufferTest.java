/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.sink.batch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;

import org.junit.jupiter.api.Test;

import io.debezium.metadata.CollectionId;
import io.debezium.sink.DebeziumSinkRecord;
import io.debezium.sink.SinkConnectorConfig;

class AbstractBufferTest {

    @Test
    void pollShouldReturnBatchWhenBatchSizeReached() {
        SinkConnectorConfig config = mock(SinkConnectorConfig.class);
        when(config.getBatchSize()).thenReturn(3);
        KeylessPassthroughBuffer buffer = new KeylessPassthroughBuffer(config);
        CollectionId id = new CollectionId("table");

        buffer.enqueue(id, mockRecord());
        buffer.enqueue(id, mockRecord());
        assertThat(buffer.poll()).isEmpty();

        buffer.enqueue(id, mockRecord());
        List<Batch> batches = buffer.poll();
        assertThat(batches).hasSize(1);
        assertThat(batches.get(0)).hasSize(3);
        assertThat(buffer.size()).isEqualTo(0);
    }

    @Test
    void pollShouldReturnBatchSizeChunkWhenMoreRecordsExist() {
        SinkConnectorConfig config = mock(SinkConnectorConfig.class);
        when(config.getBatchSize()).thenReturn(2);
        KeylessPassthroughBuffer buffer = new KeylessPassthroughBuffer(config);
        CollectionId id = new CollectionId("table");

        buffer.enqueue(id, mockRecord());
        buffer.enqueue(id, mockRecord());
        buffer.enqueue(id, mockRecord());
        buffer.enqueue(id, mockRecord());
        buffer.enqueue(id, mockRecord());

        List<Batch> first = buffer.poll();
        assertThat(first).hasSize(1);
        assertThat(first.get(0)).hasSize(2);
        assertThat(buffer.size()).isEqualTo(3);

        List<Batch> second = buffer.poll();
        assertThat(second).hasSize(1);
        assertThat(second.get(0)).hasSize(2);
        assertThat(buffer.size()).isEqualTo(1);

        List<Batch> third = buffer.poll();
        assertThat(third).isEmpty();
        assertThat(buffer.size()).isEqualTo(1);
    }

    @Test
    void forcePollShouldReturnAllRemainingRecords() {
        SinkConnectorConfig config = mock(SinkConnectorConfig.class);
        when(config.getBatchSize()).thenReturn(100);
        KeylessPassthroughBuffer buffer = new KeylessPassthroughBuffer(config);
        CollectionId id = new CollectionId("table");

        buffer.enqueue(id, mockRecord());
        buffer.enqueue(id, mockRecord());
        buffer.enqueue(id, mockRecord());

        assertThat(buffer.poll()).isEmpty();

        List<Batch> batches = buffer.forcePoll();
        assertThat(batches).hasSize(1);
        assertThat(batches.get(0)).hasSize(3);
        assertThat(buffer.size()).isEqualTo(0);
    }

    @Test
    void pollWithBatchSizeOneShouldReturnOneAtATime() {
        SinkConnectorConfig config = mock(SinkConnectorConfig.class);
        when(config.getBatchSize()).thenReturn(1);
        KeylessPassthroughBuffer buffer = new KeylessPassthroughBuffer(config);
        CollectionId id = new CollectionId("table");

        DebeziumSinkRecord r1 = mockRecord();
        DebeziumSinkRecord r2 = mockRecord();
        buffer.enqueue(id, r1);
        buffer.enqueue(id, r2);

        List<Batch> first = buffer.poll();
        assertThat(first).hasSize(1);
        assertThat(first.get(0)).hasSize(1);
        assertThat(first.get(0).get(0).record()).isSameAs(r1);

        List<Batch> second = buffer.poll();
        assertThat(second).hasSize(1);
        assertThat(second.get(0)).hasSize(1);
        assertThat(second.get(0).get(0).record()).isSameAs(r2);

        assertThat(buffer.poll()).isEmpty();
    }

    private DebeziumSinkRecord mockRecord() {
        DebeziumSinkRecord record = mock(DebeziumSinkRecord.class);
        when(record.key()).thenReturn(null);
        return record;
    }
}
