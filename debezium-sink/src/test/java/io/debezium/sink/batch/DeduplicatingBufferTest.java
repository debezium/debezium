/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.sink.batch;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import io.debezium.bindings.kafka.KafkaDebeziumSinkRecord;
import io.debezium.doc.FixFor;
import io.debezium.metadata.CollectionId;
import io.debezium.sink.DebeziumSinkRecord;
import io.debezium.sink.SinkConnectorConfig;
import io.debezium.sink.TestSinkConnectorConfig;
import io.debezium.sink.TestSinkRecords;

class DeduplicatingBufferTest {

    @Nested
    class KeyedMode {

        private DeduplicatingBuffer buffer;
        private CollectionId collectionId;

        @BeforeEach
        void setUp() {
            SinkConnectorConfig config = new TestSinkConnectorConfig()
                    .withPrimaryKeyMode(SinkConnectorConfig.PrimaryKeyMode.RECORD_KEY);
            buffer = new DeduplicatingBuffer(config, true);
            collectionId = new CollectionId("test_table");
        }

        @FixFor("debezium/dbz#1185")
        @Test
        void shouldDeduplicateRecordsWithSameKey() {
            KafkaDebeziumSinkRecord record1 = TestSinkRecords.keyed("topic", "key1");
            KafkaDebeziumSinkRecord record2 = TestSinkRecords.keyed("topic", "key1");

            buffer.enqueue(collectionId, record1);
            buffer.enqueue(collectionId, record2);

            assertThat(buffer.size()).isEqualTo(1);

            List<Batch> batches = buffer.forcePoll();
            assertThat(batches).hasSize(1);
            assertThat(batches.get(0)).hasSize(1);
            assertThat(batches.get(0).get(0).record()).isSameAs(record2);
        }

        @FixFor("debezium/dbz#1185")
        @Test
        void shouldKeepRecordsWithDifferentKeys() {
            KafkaDebeziumSinkRecord record1 = TestSinkRecords.keyed("topic", "key1");
            KafkaDebeziumSinkRecord record2 = TestSinkRecords.keyed("topic", "key2");

            buffer.enqueue(collectionId, record1);
            buffer.enqueue(collectionId, record2);

            assertThat(buffer.size()).isEqualTo(2);
        }

        @FixFor("debezium/dbz#1185")
        @Test
        void shouldAcceptTruncateRecordWithoutKey() {
            DebeziumSinkRecord truncateRecord = TestSinkRecords.truncate("topic");

            buffer.truncate(collectionId, truncateRecord);

            assertThat(buffer.size()).isEqualTo(1);
            List<Batch> batches = buffer.forcePoll();
            assertThat(batches.get(0).get(0).record()).isSameAs(truncateRecord);
        }

        @FixFor("debezium/dbz#1185")
        @Test
        void truncateShouldRemoveRecordsAndAddTruncateEvent() {
            KafkaDebeziumSinkRecord record1 = TestSinkRecords.keyed("topic", "key1");
            KafkaDebeziumSinkRecord record2 = TestSinkRecords.keyed("topic", "key2");

            buffer.enqueue(collectionId, record1);
            buffer.enqueue(collectionId, record2);
            assertThat(buffer.size()).isEqualTo(2);

            DebeziumSinkRecord truncateRecord = TestSinkRecords.truncate("topic");

            buffer.truncate(collectionId, truncateRecord);

            assertThat(buffer.size()).isEqualTo(1);
            List<Batch> batches = buffer.forcePoll();
            assertThat(batches.get(0).get(0).record()).isSameAs(truncateRecord);
        }
    }

    @Nested
    class KeylessMode {

        private DeduplicatingBuffer buffer;
        private CollectionId collectionId;

        @BeforeEach
        void setUp() {
            buffer = new DeduplicatingBuffer(new TestSinkConnectorConfig(), false);
            collectionId = new CollectionId("test_table");
        }

        @FixFor("debezium/dbz#1185")
        @Test
        void shouldPreserveAllRecords() {
            DebeziumSinkRecord record1 = TestSinkRecords.flat("topic", (byte) 1, "a");
            DebeziumSinkRecord record2 = TestSinkRecords.flat("topic", (byte) 2, "b");
            DebeziumSinkRecord record3 = TestSinkRecords.flat("topic", (byte) 3, "c");

            buffer.enqueue(collectionId, record1);
            buffer.enqueue(collectionId, record2);
            buffer.enqueue(collectionId, record3);

            assertThat(buffer.size()).isEqualTo(3);

            List<Batch> batches = buffer.forcePoll();
            assertThat(batches).hasSize(1);
            assertThat(batches.get(0)).hasSize(3);
        }

        @FixFor("debezium/dbz#1185")
        @Test
        void truncateShouldRemoveRecordsForCollection() {
            DebeziumSinkRecord record1 = TestSinkRecords.flat("topic", (byte) 1, "a");
            DebeziumSinkRecord record2 = TestSinkRecords.flat("topic", (byte) 2, "b");
            DebeziumSinkRecord truncateRecord = TestSinkRecords.truncate("topic");

            buffer.enqueue(collectionId, record1);
            buffer.enqueue(collectionId, record2);
            assertThat(buffer.size()).isEqualTo(2);

            buffer.truncate(collectionId, truncateRecord);

            assertThat(buffer.size()).isEqualTo(1);
            List<Batch> batches = buffer.forcePoll();
            assertThat(batches.get(0).get(0).record()).isSameAs(truncateRecord);
        }

        @FixFor("debezium/dbz#1185")
        @Test
        void truncateShouldNotAffectOtherCollections() {
            CollectionId table1 = new CollectionId("table1");
            CollectionId table2 = new CollectionId("table2");
            DebeziumSinkRecord record1 = TestSinkRecords.flat("topic", (byte) 1, "a");
            DebeziumSinkRecord record2 = TestSinkRecords.flat("topic", (byte) 2, "b");
            DebeziumSinkRecord truncateRecord = TestSinkRecords.truncate("topic");

            buffer.enqueue(table1, record1);
            buffer.enqueue(table2, record2);

            buffer.truncate(table1, truncateRecord);

            assertThat(buffer.size()).isEqualTo(2);
            List<Batch> batches = buffer.forcePoll();
            assertThat(batches.get(0).get(0).collectionId()).isEqualTo(table2);
            assertThat(batches.get(0).get(1).collectionId()).isEqualTo(table1);
            assertThat(batches.get(0).get(1).record()).isSameAs(truncateRecord);
        }
    }
}
