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
import java.util.Set;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import io.debezium.bindings.kafka.KafkaDebeziumSinkRecord;
import io.debezium.doc.FixFor;
import io.debezium.metadata.CollectionId;
import io.debezium.sink.DebeziumSinkRecord;
import io.debezium.sink.SinkConnectorConfig;

class PassthroughBufferTest {

    @Nested
    class KeylessMode {

        private PassthroughBuffer buffer;
        private CollectionId collectionId;

        @BeforeEach
        void setUp() {
            SinkConnectorConfig config = mock(SinkConnectorConfig.class);
            when(config.getBatchSize()).thenReturn(500);
            buffer = new PassthroughBuffer(config, false);
            collectionId = new CollectionId("test_table");
        }

        // --- enqueue ---

        @FixFor("debezium/dbz#1185")
        @Test
        void shouldPreserveAllRecords() {
            DebeziumSinkRecord r1 = mockRecord(null);
            DebeziumSinkRecord r2 = mockRecord(null);
            DebeziumSinkRecord r3 = mockRecord(null);

            buffer.enqueue(collectionId, r1);
            buffer.enqueue(collectionId, r2);
            buffer.enqueue(collectionId, r3);

            assertThat(buffer.size()).isEqualTo(3);

            List<Batch> batches = buffer.forcePoll();
            assertThat(batches).hasSize(1);
            assertThat(batches.get(0)).hasSize(3);
            assertThat(batches.get(0).get(0).record()).isSameAs(r1);
            assertThat(batches.get(0).get(1).record()).isSameAs(r2);
            assertThat(batches.get(0).get(2).record()).isSameAs(r3);
        }

        @FixFor("debezium/dbz#1185")
        @Test
        void shouldPreserveRecordsAcrossMultipleTables() {
            CollectionId table1 = new CollectionId("table1");
            CollectionId table2 = new CollectionId("table2");
            DebeziumSinkRecord r1 = mockRecord(null);
            DebeziumSinkRecord r2 = mockRecord(null);

            buffer.enqueue(table1, r1);
            buffer.enqueue(table2, r2);

            List<Batch> batches = buffer.forcePoll();
            assertThat(batches).hasSize(1);
            assertThat(batches.get(0).get(0).collectionId()).isEqualTo(table1);
            assertThat(batches.get(0).get(1).collectionId()).isEqualTo(table2);
        }

        // --- poll ---

        @FixFor("debezium/dbz#1185")
        @Test
        void pollShouldReturnEmptyWhenBelowBatchSize() {
            buffer.enqueue(collectionId, mockRecord(null));

            assertThat(buffer.poll()).isEmpty();
            assertThat(buffer.size()).isEqualTo(1);
        }

        @FixFor("debezium/dbz#1185")
        @Test
        void pollShouldReturnFullBatchWhenBatchSizeReached() {
            SinkConnectorConfig config = mock(SinkConnectorConfig.class);
            when(config.getBatchSize()).thenReturn(3);
            PassthroughBuffer smallBuffer = new PassthroughBuffer(config, false);

            for (int i = 0; i < 3; i++) {
                smallBuffer.enqueue(collectionId, mockRecord(null));
            }

            List<Batch> batches = smallBuffer.poll();
            assertThat(batches).hasSize(1);
            assertThat(batches.get(0)).hasSize(3);
            assertThat(smallBuffer.size()).isEqualTo(0);
        }

        @FixFor("debezium/dbz#1185")
        @Test
        void pollShouldLeaveRemainderInBuffer() {
            SinkConnectorConfig config = mock(SinkConnectorConfig.class);
            when(config.getBatchSize()).thenReturn(3);
            PassthroughBuffer smallBuffer = new PassthroughBuffer(config, false);

            for (int i = 0; i < 5; i++) {
                smallBuffer.enqueue(collectionId, mockRecord(null));
            }

            List<Batch> batches = smallBuffer.poll();
            assertThat(batches).hasSize(1);
            assertThat(batches.get(0)).hasSize(3);
            assertThat(smallBuffer.size()).isEqualTo(2);
        }

        // --- forcePoll ---

        @FixFor("debezium/dbz#1185")
        @Test
        void forcePollShouldDrainBuffer() {
            buffer.enqueue(collectionId, mockRecord(null));
            buffer.enqueue(collectionId, mockRecord(null));

            List<Batch> batches = buffer.forcePoll();
            assertThat(batches).hasSize(1);
            assertThat(batches.get(0)).hasSize(2);
            assertThat(buffer.size()).isEqualTo(0);
        }

        @FixFor("debezium/dbz#1185")
        @Test
        void forcePollOnEmptyBufferShouldReturnEmptyList() {
            assertThat(buffer.forcePoll()).isEmpty();
        }

        @FixFor("debezium/dbz#1185")
        @Test
        void forcePollShouldReturnIncompleteLastBatch() {
            SinkConnectorConfig config = mock(SinkConnectorConfig.class);
            when(config.getBatchSize()).thenReturn(3);
            PassthroughBuffer smallBuffer = new PassthroughBuffer(config, false);

            for (int i = 0; i < 5; i++) {
                smallBuffer.enqueue(collectionId, mockRecord(null));
            }

            List<Batch> batches = smallBuffer.forcePoll();
            assertThat(batches).hasSize(2);
            assertThat(batches.get(0)).hasSize(3);
            assertThat(batches.get(1)).hasSize(2);
        }

        // --- truncate ---

        @FixFor("debezium/dbz#1185")
        @Test
        void truncateShouldFlushRecordsBeforeTruncate() {
            DebeziumSinkRecord r1 = mockRecord("key1");
            DebeziumSinkRecord r2 = mockRecord("key2");
            DebeziumSinkRecord truncateRecord = mockTruncateRecord();

            buffer.enqueue(collectionId, r1);
            buffer.enqueue(collectionId, r2);

            buffer.truncate(collectionId, truncateRecord);

            List<Batch> batches = buffer.forcePoll();
            int totalRecords = batches.stream().mapToInt(Batch::size).sum();
            assertThat(totalRecords).isEqualTo(3);
            assertThat(batches.get(0).get(0).record()).isSameAs(r1);
            assertThat(batches.get(0).get(1).record()).isSameAs(r2);
        }

        @FixFor("debezium/dbz#1185")
        @Test
        void truncateShouldNotAffectOtherCollections() {
            CollectionId table1 = new CollectionId("table1");
            CollectionId table2 = new CollectionId("table2");

            buffer.enqueue(table1, mockRecord(null));
            buffer.enqueue(table2, mockRecord(null));

            buffer.truncate(table1, mockTruncateRecord());

            List<Batch> batches = buffer.forcePoll();
            int totalRecords = batches.stream().mapToInt(Batch::size).sum();
            assertThat(totalRecords).isEqualTo(3);
        }

        @FixFor("debezium/dbz#1185")
        @Test
        void multipleTruncatesShouldAllBePreserved() {
            DebeziumSinkRecord r1 = mockRecord(null);
            DebeziumSinkRecord truncate1 = mockTruncateRecord();
            DebeziumSinkRecord r2 = mockRecord(null);
            DebeziumSinkRecord truncate2 = mockTruncateRecord();

            buffer.enqueue(collectionId, r1);
            buffer.truncate(collectionId, truncate1);
            buffer.enqueue(collectionId, r2);
            buffer.truncate(collectionId, truncate2);

            List<Batch> batches = buffer.forcePoll();
            int totalRecords = batches.stream().mapToInt(Batch::size).sum();
            assertThat(totalRecords).isEqualTo(4);
        }

        @FixFor("debezium/dbz#1185")
        @Test
        void pollShouldReturnCompletedBatchesImmediatelyAfterTruncate() {
            buffer.enqueue(collectionId, mockRecord(null));
            buffer.truncate(collectionId, mockTruncateRecord());

            List<Batch> batches = buffer.poll();
            assertThat(batches).isNotEmpty();
            assertThat(batches.get(0).get(0).record().isTruncate()).isFalse();
        }

        // --- helpers ---

        private DebeziumSinkRecord mockRecord(Object key) {
            DebeziumSinkRecord record = mock(DebeziumSinkRecord.class);
            when(record.key()).thenReturn(key);
            return record;
        }

        private DebeziumSinkRecord mockTruncateRecord() {
            DebeziumSinkRecord record = mock(DebeziumSinkRecord.class);
            when(record.isTruncate()).thenReturn(true);
            when(record.key()).thenReturn(null);
            return record;
        }
    }

    @Nested
    class KeyedMode {

        private PassthroughBuffer buffer;
        private SinkConnectorConfig config;
        private CollectionId collectionId;

        @BeforeEach
        void setUp() {
            config = mock(SinkConnectorConfig.class);
            when(config.getBatchSize()).thenReturn(500);
            when(config.getPrimaryKeyMode()).thenReturn(SinkConnectorConfig.PrimaryKeyMode.RECORD_KEY);
            when(config.getPrimaryKeyFields()).thenReturn(Set.of());
            when(config.fieldFilter()).thenReturn(null);
            buffer = new PassthroughBuffer(config, true);
            collectionId = new CollectionId("test_table");
        }

        // --- enqueue ---

        @FixFor("debezium/dbz#1185")
        @Test
        void shouldPreserveAllRecordsWithSameKey() {
            KafkaDebeziumSinkRecord r1 = mockKafkaRecord("key1");
            KafkaDebeziumSinkRecord r2 = mockKafkaRecord("key1");

            buffer.enqueue(collectionId, r1);
            buffer.enqueue(collectionId, r2);

            assertThat(buffer.size()).isEqualTo(1);

            List<Batch> batches = buffer.forcePoll();
            assertThat(batches).hasSize(2);
            assertThat(batches.get(0).get(0).record()).isSameAs(r1);
            assertThat(batches.get(1).get(0).record()).isSameAs(r2);
        }

        @FixFor("debezium/dbz#1185")
        @Test
        void shouldKeepRecordsWithDifferentKeysInSameBatch() {
            KafkaDebeziumSinkRecord r1 = mockKafkaRecord("key1");
            KafkaDebeziumSinkRecord r2 = mockKafkaRecord("key2");

            buffer.enqueue(collectionId, r1);
            buffer.enqueue(collectionId, r2);

            List<Batch> batches = buffer.forcePoll();
            assertThat(batches).hasSize(1);
            assertThat(batches.get(0)).hasSize(2);
        }

        @FixFor("debezium/dbz#1185")
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

            List<Batch> batches = buffer.forcePoll();
            assertThat(batches).hasSize(2);
            assertThat(batches.get(0)).hasSize(2);
            assertThat(batches.get(0).get(0).record()).isSameAs(r1);
            assertThat(batches.get(0).get(1).record()).isSameAs(r2);
            assertThat(batches.get(1)).hasSize(2);
            assertThat(batches.get(1).get(0).record()).isSameAs(r3);
            assertThat(batches.get(1).get(1).record()).isSameAs(r4);
        }

        // --- poll ---

        @FixFor("debezium/dbz#1185")
        @Test
        void pollShouldReturnCompletedBatchesImmediatelyAfterDuplicateKey() {
            KafkaDebeziumSinkRecord r1 = mockKafkaRecord("key1");
            KafkaDebeziumSinkRecord r2 = mockKafkaRecord("key1");

            buffer.enqueue(collectionId, r1);
            buffer.enqueue(collectionId, r2);

            List<Batch> batches = buffer.poll();
            assertThat(batches).hasSize(1);
            assertThat(batches.get(0).get(0).record()).isSameAs(r1);
        }

        @FixFor("debezium/dbz#1185")
        @Test
        void pollShouldReturnEmptyWhenNoDuplicatesAndBelowBatchSize() {
            buffer.enqueue(collectionId, mockKafkaRecord("key1"));
            buffer.enqueue(collectionId, mockKafkaRecord("key2"));

            assertThat(buffer.poll()).isEmpty();
        }

        @FixFor("debezium/dbz#1185")
        @Test
        void pollShouldAlsoExtractFullBatchesFromRecordsMap() {
            SinkConnectorConfig smallConfig = mock(SinkConnectorConfig.class);
            when(smallConfig.getBatchSize()).thenReturn(2);
            when(smallConfig.getPrimaryKeyMode()).thenReturn(SinkConnectorConfig.PrimaryKeyMode.RECORD_KEY);
            when(smallConfig.getPrimaryKeyFields()).thenReturn(Set.of());
            when(smallConfig.fieldFilter()).thenReturn(null);
            PassthroughBuffer smallBuffer = new PassthroughBuffer(smallConfig, true);

            // key1, key2, key1 → completed: [{key1,key2}], records: {key1}
            smallBuffer.enqueue(collectionId, mockKafkaRecord("key1"));
            smallBuffer.enqueue(collectionId, mockKafkaRecord("key2"));
            smallBuffer.enqueue(collectionId, mockKafkaRecord("key1"));
            // add key3 → records: {key1, key3} → size=2 = batchSize
            smallBuffer.enqueue(collectionId, mockKafkaRecord("key3"));

            List<Batch> batches = smallBuffer.poll();
            // completed batch [{key1,key2}] + one full batch [{key1,key3}] from records
            assertThat(batches).hasSize(2);
            assertThat(batches.get(0)).hasSize(2);
            assertThat(batches.get(1)).hasSize(2);
            assertThat(smallBuffer.size()).isEqualTo(0);
        }

        // --- forcePoll ---

        @FixFor("debezium/dbz#1185")
        @Test
        void forcePollShouldReturnIncompleteLastBatch() {
            SinkConnectorConfig smallConfig = mock(SinkConnectorConfig.class);
            when(smallConfig.getBatchSize()).thenReturn(2);
            when(smallConfig.getPrimaryKeyMode()).thenReturn(SinkConnectorConfig.PrimaryKeyMode.RECORD_KEY);
            when(smallConfig.getPrimaryKeyFields()).thenReturn(Set.of());
            when(smallConfig.fieldFilter()).thenReturn(null);
            PassthroughBuffer smallBuffer = new PassthroughBuffer(smallConfig, true);

            smallBuffer.enqueue(collectionId, mockKafkaRecord("key1"));
            smallBuffer.enqueue(collectionId, mockKafkaRecord("key2"));
            smallBuffer.enqueue(collectionId, mockKafkaRecord("key3"));
            smallBuffer.enqueue(collectionId, mockKafkaRecord("key1"));

            List<Batch> batches = smallBuffer.forcePoll();
            // completed: [{key1,key2},{key3}] from split, records: {key1} drained
            assertThat(batches).hasSize(3);
            assertThat(batches.get(0)).hasSize(2);
            assertThat(batches.get(1)).hasSize(1);
            assertThat(batches.get(2)).hasSize(1);
        }

        // --- truncate ---

        @FixFor("debezium/dbz#1185")
        @Test
        void truncateShouldFlushAllRecordsBeforeTruncate() {
            buffer.enqueue(collectionId, mockKafkaRecord("key1"));
            buffer.enqueue(collectionId, mockKafkaRecord("key1"));
            buffer.enqueue(collectionId, mockKafkaRecord("key1"));

            buffer.truncate(collectionId, mockTruncateRecord());

            List<Batch> batches = buffer.forcePoll();
            int totalRecords = batches.stream().mapToInt(Batch::size).sum();
            assertThat(totalRecords).isEqualTo(4);
        }

        @FixFor("debezium/dbz#1185")
        @Test
        void truncateShouldNotAffectOtherCollections() {
            CollectionId table1 = new CollectionId("table1");
            CollectionId table2 = new CollectionId("table2");

            buffer.enqueue(table1, mockKafkaRecord("key1"));
            buffer.enqueue(table2, mockKafkaRecord("key2"));
            buffer.enqueue(table1, mockKafkaRecord("key1"));

            buffer.truncate(table1, mockTruncateRecord());

            List<Batch> batches = buffer.forcePoll();
            long table2Records = batches.stream()
                    .flatMap(Batch::stream)
                    .filter(br -> br.collectionId().equals(table2))
                    .count();
            assertThat(table2Records).isEqualTo(1);
        }

        @FixFor("debezium/dbz#1185")
        @Test
        void multipleTruncatesShouldAllBePreserved() {
            buffer.enqueue(collectionId, mockKafkaRecord("key1"));
            buffer.truncate(collectionId, mockTruncateRecord());
            buffer.enqueue(collectionId, mockKafkaRecord("key1"));
            buffer.truncate(collectionId, mockTruncateRecord());

            List<Batch> batches = buffer.forcePoll();
            int totalRecords = batches.stream().mapToInt(Batch::size).sum();
            assertThat(totalRecords).isEqualTo(4);
            long truncateCount = batches.stream()
                    .flatMap(Batch::stream)
                    .filter(br -> br.record().isTruncate())
                    .count();
            assertThat(truncateCount).isEqualTo(2);
        }

        @FixFor("debezium/dbz#1185")
        @Test
        void pollShouldReturnCompletedBatchesImmediatelyAfterTruncate() {
            buffer.enqueue(collectionId, mockKafkaRecord("key1"));
            buffer.truncate(collectionId, mockTruncateRecord());

            List<Batch> batches = buffer.poll();
            assertThat(batches).isNotEmpty();
            assertThat(batches.get(0).get(0).record().isTruncate()).isFalse();
        }

        @FixFor("debezium/dbz#1185")
        @Test
        void truncateFollowedByInsertsShouldPreserveOrdering() {
            KafkaDebeziumSinkRecord insert1 = mockKafkaRecord("key1");
            DebeziumSinkRecord truncate = mockTruncateRecord();
            KafkaDebeziumSinkRecord insert2 = mockKafkaRecord("key1");

            buffer.enqueue(collectionId, insert1);
            buffer.truncate(collectionId, truncate);
            buffer.enqueue(collectionId, insert2);

            List<Batch> batches = buffer.forcePoll();
            List<BatchRecord> allRecords = batches.stream()
                    .flatMap(Batch::stream)
                    .toList();
            assertThat(allRecords).hasSize(3);
            assertThat(allRecords.get(0).record()).isSameAs(insert1);
            assertThat(allRecords.get(1).record()).isSameAs(truncate);
            assertThat(allRecords.get(2).record()).isSameAs(insert2);
        }

        // --- helpers ---

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

        private DebeziumSinkRecord mockTruncateRecord() {
            DebeziumSinkRecord record = mock(DebeziumSinkRecord.class);
            when(record.isTruncate()).thenReturn(true);
            when(record.key()).thenReturn(null);
            return record;
        }
    }
}
