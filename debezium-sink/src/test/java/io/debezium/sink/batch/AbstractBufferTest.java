/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.sink.batch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;

import io.debezium.metadata.CollectionId;
import io.debezium.sink.DebeziumSinkRecord;
import io.debezium.sink.SinkConnectorConfig;

class AbstractBufferTest {

    @Test
    void pollShouldReturnBatchWhenBatchSizeReached() {
        SinkConnectorConfig config = mock(SinkConnectorConfig.class);
        when(config.getBatchSize()).thenReturn(3);
        SimpleBuffer buffer = new SimpleBuffer(config);
        CollectionId id = new CollectionId("table");

        buffer.enqueue(id, mockRecord());
        buffer.enqueue(id, mockRecord());
        assertThat(buffer.poll()).isEmpty();

        buffer.enqueue(id, mockRecord());
        Batch batch = buffer.poll();
        assertThat(batch).hasSize(3);
        assertThat(buffer.size()).isEqualTo(0);
    }

    @Test
    void pollShouldReturnBatchSizeChunkWhenMoreRecordsExist() {
        SinkConnectorConfig config = mock(SinkConnectorConfig.class);
        when(config.getBatchSize()).thenReturn(2);
        SimpleBuffer buffer = new SimpleBuffer(config);
        CollectionId id = new CollectionId("table");

        buffer.enqueue(id, mockRecord());
        buffer.enqueue(id, mockRecord());
        buffer.enqueue(id, mockRecord());
        buffer.enqueue(id, mockRecord());
        buffer.enqueue(id, mockRecord());

        Batch first = buffer.poll();
        assertThat(first).hasSize(2);
        assertThat(buffer.size()).isEqualTo(3);

        Batch second = buffer.poll();
        assertThat(second).hasSize(2);
        assertThat(buffer.size()).isEqualTo(1);

        Batch third = buffer.poll();
        assertThat(third).isEmpty();
        assertThat(buffer.size()).isEqualTo(1);
    }

    @Test
    void forcePollShouldReturnAllRemainingRecords() {
        SinkConnectorConfig config = mock(SinkConnectorConfig.class);
        when(config.getBatchSize()).thenReturn(100);
        SimpleBuffer buffer = new SimpleBuffer(config);
        CollectionId id = new CollectionId("table");

        buffer.enqueue(id, mockRecord());
        buffer.enqueue(id, mockRecord());
        buffer.enqueue(id, mockRecord());

        assertThat(buffer.poll()).isEmpty();

        Batch batch = buffer.forcePoll();
        assertThat(batch).hasSize(3);
        assertThat(buffer.size()).isEqualTo(0);
    }

    @Test
    void pollWithBatchSizeOneShouldReturnOneAtATime() {
        SinkConnectorConfig config = mock(SinkConnectorConfig.class);
        when(config.getBatchSize()).thenReturn(1);
        SimpleBuffer buffer = new SimpleBuffer(config);
        CollectionId id = new CollectionId("table");

        DebeziumSinkRecord r1 = mockRecord();
        DebeziumSinkRecord r2 = mockRecord();
        buffer.enqueue(id, r1);
        buffer.enqueue(id, r2);

        Batch first = buffer.poll();
        assertThat(first).hasSize(1);
        assertThat(first.get(0).record()).isSameAs(r1);

        Batch second = buffer.poll();
        assertThat(second).hasSize(1);
        assertThat(second.get(0).record()).isSameAs(r2);

        assertThat(buffer.poll()).isEmpty();
    }

    private DebeziumSinkRecord mockRecord() {
        DebeziumSinkRecord record = mock(DebeziumSinkRecord.class);
        when(record.key()).thenReturn(null);
        return record;
    }
}
