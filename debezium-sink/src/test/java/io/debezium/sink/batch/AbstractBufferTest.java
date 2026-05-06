/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.sink.batch;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.Test;

import io.debezium.doc.FixFor;
import io.debezium.metadata.CollectionId;
import io.debezium.sink.DebeziumSinkRecord;
import io.debezium.sink.TestSinkConnectorConfig;
import io.debezium.sink.TestSinkRecords;

class AbstractBufferTest {

    @FixFor("debezium/dbz#1185")
    @Test
    void pollShouldReturnBatchWhenBatchSizeReached() {
        PassthroughBuffer buffer = new PassthroughBuffer(new TestSinkConnectorConfig().withBatchSize(3), false);
        CollectionId id = new CollectionId("table");

        buffer.enqueue(id, TestSinkRecords.flat("topic", (byte) 1, "a"));
        buffer.enqueue(id, TestSinkRecords.flat("topic", (byte) 2, "b"));
        assertThat(buffer.poll()).isEmpty();

        buffer.enqueue(id, TestSinkRecords.flat("topic", (byte) 3, "c"));
        List<Batch> batches = buffer.poll();
        assertThat(batches).hasSize(1);
        assertThat(batches.get(0)).hasSize(3);
        assertThat(buffer.size()).isEqualTo(0);
    }

    @FixFor("debezium/dbz#1185")
    @Test
    void pollShouldReturnBatchSizeChunkWhenMoreRecordsExist() {
        PassthroughBuffer buffer = new PassthroughBuffer(new TestSinkConnectorConfig().withBatchSize(2), false);
        CollectionId id = new CollectionId("table");

        for (int i = 0; i < 5; i++) {
            buffer.enqueue(id, TestSinkRecords.flat("topic", (byte) i, "v" + i));
        }

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

    @FixFor("debezium/dbz#1185")
    @Test
    void forcePollShouldReturnAllRemainingRecords() {
        PassthroughBuffer buffer = new PassthroughBuffer(new TestSinkConnectorConfig().withBatchSize(100), false);
        CollectionId id = new CollectionId("table");

        for (int i = 0; i < 3; i++) {
            buffer.enqueue(id, TestSinkRecords.flat("topic", (byte) i, "v" + i));
        }

        assertThat(buffer.poll()).isEmpty();

        List<Batch> batches = buffer.forcePoll();
        assertThat(batches).hasSize(1);
        assertThat(batches.get(0)).hasSize(3);
        assertThat(buffer.size()).isEqualTo(0);
    }

    @FixFor("debezium/dbz#1185")
    @Test
    void pollWithBatchSizeOneShouldReturnOneAtATime() {
        PassthroughBuffer buffer = new PassthroughBuffer(new TestSinkConnectorConfig().withBatchSize(1), false);
        CollectionId id = new CollectionId("table");

        DebeziumSinkRecord r1 = TestSinkRecords.flat("topic", (byte) 1, "a");
        DebeziumSinkRecord r2 = TestSinkRecords.flat("topic", (byte) 2, "b");
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
}
