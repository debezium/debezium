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

class DeduplicatingBufferTest {

    @Nested
    class KeyedMode {

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
            buffer = new DeduplicatingBuffer(config, true);
            collectionId = new CollectionId("test_table");
        }

        @FixFor("debezium/dbz#1185")
        @Test
        void shouldDeduplicateRecordsWithSameKey() {
            KafkaDebeziumSinkRecord record1 = mockKafkaRecord("key1");
            KafkaDebeziumSinkRecord record2 = mockKafkaRecord("key1");

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
            KafkaDebeziumSinkRecord record1 = mockKafkaRecord("key1");
            KafkaDebeziumSinkRecord record2 = mockKafkaRecord("key2");

            buffer.enqueue(collectionId, record1);
            buffer.enqueue(collectionId, record2);

            assertThat(buffer.size()).isEqualTo(2);
        }

        @FixFor("debezium/dbz#1185")
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

        @FixFor("debezium/dbz#1185")
        @Test
        void truncateShouldRemoveRecordsAndAddTruncateEvent() {
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
            List<Batch> batches = buffer.forcePoll();
            assertThat(batches.get(0).get(0).record()).isSameAs(truncateRecord);
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

    @Nested
    class KeylessMode {

        private DeduplicatingBuffer buffer;
        private CollectionId collectionId;

        @BeforeEach
        void setUp() {
            SinkConnectorConfig config = mock(SinkConnectorConfig.class);
            when(config.getBatchSize()).thenReturn(500);
            buffer = new DeduplicatingBuffer(config, false);
            collectionId = new CollectionId("test_table");
        }

        @FixFor("debezium/dbz#1185")
        @Test
        void shouldPreserveAllRecords() {
            DebeziumSinkRecord record1 = mockRecord(null);
            DebeziumSinkRecord record2 = mockRecord(null);
            DebeziumSinkRecord record3 = mockRecord(null);

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
            DebeziumSinkRecord record1 = mockRecord(null);
            DebeziumSinkRecord record2 = mockRecord(null);
            DebeziumSinkRecord truncateRecord = mockRecord(null);
            when(truncateRecord.isTruncate()).thenReturn(true);

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
            DebeziumSinkRecord record1 = mockRecord(null);
            DebeziumSinkRecord record2 = mockRecord(null);
            DebeziumSinkRecord truncateRecord = mockRecord(null);
            when(truncateRecord.isTruncate()).thenReturn(true);

            buffer.enqueue(table1, record1);
            buffer.enqueue(table2, record2);

            buffer.truncate(table1, truncateRecord);

            assertThat(buffer.size()).isEqualTo(2);
            List<Batch> batches = buffer.forcePoll();
            assertThat(batches.get(0).get(0).collectionId()).isEqualTo(table2);
            assertThat(batches.get(0).get(1).collectionId()).isEqualTo(table1);
            assertThat(batches.get(0).get(1).record()).isSameAs(truncateRecord);
        }

        private DebeziumSinkRecord mockRecord(Object key) {
            DebeziumSinkRecord record = mock(DebeziumSinkRecord.class);
            when(record.key()).thenReturn(key);
            return record;
        }
    }
}
