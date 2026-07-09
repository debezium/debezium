/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.sink;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.data.Envelope;
import io.debezium.doc.FixFor;
import io.debezium.metadata.CollectionId;
import io.debezium.sink.batch.Batch;

class AbstractChangeEventSinkTest {

    private TestChangeEventSink sink;
    private TestSinkConnectorConfig config;

    @BeforeEach
    void setUp() {
        config = new TestSinkConnectorConfig();
        sink = new TestChangeEventSink(config);
    }

    @FixFor("debezium/dbz#1185")
    @Test
    void putShouldBufferRecords() {
        List<Batch> batches = sink.put(List.of(createFlatSinkRecord((byte) 1, "John")));

        assertThat(batches).isEmpty();
    }

    @FixFor("debezium/dbz#1185")
    @Test
    void forceFlushShouldDrainBufferedRecords() {
        sink.put(List.of(createFlatSinkRecord((byte) 1, "John")));
        sink.forceFlush();

        assertThat(sink.writtenBatches).hasSize(1);
        assertThat(sink.writtenBatches.get(0)).hasSize(1);
    }

    @FixFor("debezium/dbz#1185")
    @Test
    void forceFlushOnEmptyBufferShouldNotCallWriteBatch() {
        sink.forceFlush();

        assertThat(sink.writtenBatches).isEmpty();
    }

    @FixFor("debezium/dbz#1185")
    @Test
    void putShouldSkipSchemaChangeRecords() {
        Schema valueSchema = SchemaBuilder.struct()
                .name("io.debezium.connector.mysql.SchemaChangeValue")
                .field("dummy", Schema.STRING_SCHEMA)
                .build();
        Struct value = new Struct(valueSchema).put("dummy", "x");
        SinkRecord schemaChange = new SinkRecord("topic", 0, null, null, valueSchema, value, 0);

        sink.put(List.of(schemaChange));
        sink.forceFlush();

        assertThat(sink.writtenBatches).isEmpty();
    }

    @FixFor("debezium/dbz#1185")
    @Test
    void putShouldSkipDeleteWhenDeletesDisabled() {
        sink = new TestChangeEventSink(config.withDeleteEnabled(false));

        SinkRecord deleteRecord = new SinkRecord("topic", 0,
                Schema.INT8_SCHEMA, (byte) 1,
                SchemaBuilder.struct().field("id", Schema.INT8_SCHEMA).build(), null,
                0);

        sink.put(List.of(deleteRecord));
        sink.forceFlush();

        assertThat(sink.writtenBatches).isEmpty();
    }

    @FixFor("debezium/dbz#1185")
    @Test
    void putShouldIncludeDeleteWhenDeletesEnabled() {
        sink = new TestChangeEventSink(config.withDeleteEnabled(true));

        SinkRecord deleteRecord = new SinkRecord("topic", 0,
                Schema.INT8_SCHEMA, (byte) 1,
                SchemaBuilder.struct().field("id", Schema.INT8_SCHEMA).build(), null,
                0);

        sink.put(List.of(deleteRecord));
        sink.forceFlush();

        assertThat(sink.writtenBatches).hasSize(1);
        assertThat(sink.writtenBatches.get(0).get(0).record().isDelete()).isTrue();
    }

    @FixFor("debezium/dbz#1185")
    @Test
    void putShouldSkipTruncateWhenTruncateDisabled() {
        sink = new TestChangeEventSink(config.withTruncateEnabled(false));

        sink.put(List.of(createTruncateSinkRecord()));
        sink.forceFlush();

        assertThat(sink.writtenBatches).isEmpty();
    }

    @FixFor("debezium/dbz#1185")
    @Test
    void putShouldIncludeTruncateWhenTruncateEnabled() {
        sink = new TestChangeEventSink(config.withTruncateEnabled(true));

        sink.put(List.of(createTruncateSinkRecord()));
        sink.forceFlush();

        assertThat(sink.writtenBatches).hasSize(1);
        assertThat(sink.writtenBatches.get(0).get(0).record().isTruncate()).isTrue();
    }

    @FixFor("debezium/dbz#1185")
    @Test
    void putShouldBufferMultipleRecords() {
        sink.put(List.of(
                createFlatSinkRecord((byte) 1, "John"),
                createFlatSinkRecord((byte) 2, "Jane")));
        sink.forceFlush();

        assertThat(sink.writtenBatches).hasSize(1);
        assertThat(sink.writtenBatches.get(0)).hasSize(2);
    }

    @FixFor("debezium/dbz#1185")
    @Test
    void forcePollShouldReturnAllBuffered() {
        sink.put(List.of(createFlatSinkRecord((byte) 1, "John")));

        List<Batch> batches = sink.forcePoll();
        assertThat(batches).hasSize(1);
        assertThat(batches.get(0)).hasSize(1);
    }

    // --- Buffer mode tests ---

    @FixFor("debezium/dbz#1185")
    @Test
    void keylessPassthroughShouldPreserveAllEvents() {
        sink = new TestChangeEventSink(config
                .withPrimaryKeyMode(SinkConnectorConfig.PrimaryKeyMode.NONE));

        consumeAndFlush(List.of(
                createFlatSinkRecord((byte) 1, "John"),
                createFlatSinkRecord((byte) 1, "John Updated"),
                createFlatSinkRecord((byte) 1, "John Updated Again")));

        int totalRecords = sink.writtenBatches.stream().mapToInt(Batch::size).sum();
        assertThat(totalRecords).isEqualTo(3);
    }

    @FixFor("debezium/dbz#1185")
    @Test
    void deduplicatingBufferShouldCollapseByKey() {
        sink = new TestChangeEventSink(config
                .withPrimaryKeyMode(SinkConnectorConfig.PrimaryKeyMode.RECORD_KEY)
                .withKeyedMessageBatchMode(SinkConnectorConfig.KeyedMessageBatchMode.DEDUPLICATION));

        consumeAndFlush(List.of(
                createKeyedSinkRecord((byte) 1, "John"),
                createKeyedSinkRecord((byte) 2, "Jane"),
                createKeyedSinkRecord((byte) 1, "John Updated")));

        int totalRecords = sink.writtenBatches.stream().mapToInt(Batch::size).sum();
        assertThat(totalRecords).isEqualTo(2);
    }

    @FixFor("debezium/dbz#1185")
    @Test
    void keyedPassthroughShouldPreserveAllEvents() {
        sink = new TestChangeEventSink(config
                .withPrimaryKeyMode(SinkConnectorConfig.PrimaryKeyMode.RECORD_KEY)
                .withKeyedMessageBatchMode(SinkConnectorConfig.KeyedMessageBatchMode.PASSTHROUGH));

        consumeAndFlush(List.of(
                createKeyedSinkRecord((byte) 1, "John"),
                createKeyedSinkRecord((byte) 2, "Jane"),
                createKeyedSinkRecord((byte) 1, "John Updated")));

        int totalRecords = sink.writtenBatches.stream().mapToInt(Batch::size).sum();
        assertThat(totalRecords).isEqualTo(3);
    }

    @FixFor("debezium/dbz#1185")
    @Test
    void keyedPassthroughShouldSplitBatchesAtDuplicateKey() {
        sink = new TestChangeEventSink(config
                .withPrimaryKeyMode(SinkConnectorConfig.PrimaryKeyMode.RECORD_KEY)
                .withKeyedMessageBatchMode(SinkConnectorConfig.KeyedMessageBatchMode.PASSTHROUGH));

        consumeAndFlush(List.of(
                createKeyedSinkRecord((byte) 1, "John"),
                createKeyedSinkRecord((byte) 2, "Jane"),
                createKeyedSinkRecord((byte) 1, "John Updated")));

        assertThat(sink.writtenBatches).hasSize(2);
        assertThat(sink.writtenBatches.get(0)).hasSize(2);
        assertThat(sink.writtenBatches.get(1)).hasSize(1);
    }

    @FixFor("debezium/dbz#1185")
    @Test
    void keyedPassthroughMultipleDuplicatesShouldProduceMultipleBatches() {
        sink = new TestChangeEventSink(config
                .withPrimaryKeyMode(SinkConnectorConfig.PrimaryKeyMode.RECORD_KEY)
                .withKeyedMessageBatchMode(SinkConnectorConfig.KeyedMessageBatchMode.PASSTHROUGH));

        consumeAndFlush(List.of(
                createKeyedSinkRecord((byte) 1, "v1"),
                createKeyedSinkRecord((byte) 1, "v2"),
                createKeyedSinkRecord((byte) 1, "v3")));

        int totalRecords = sink.writtenBatches.stream().mapToInt(Batch::size).sum();
        assertThat(totalRecords).isEqualTo(3);
        assertThat(sink.writtenBatches).hasSize(3);
    }

    @FixFor("debezium/dbz#1185")
    @Test
    void deduplicatingBufferMultipleDuplicatesShouldCollapse() {
        sink = new TestChangeEventSink(config
                .withPrimaryKeyMode(SinkConnectorConfig.PrimaryKeyMode.RECORD_KEY)
                .withKeyedMessageBatchMode(SinkConnectorConfig.KeyedMessageBatchMode.DEDUPLICATION));

        consumeAndFlush(List.of(
                createKeyedSinkRecord((byte) 1, "v1"),
                createKeyedSinkRecord((byte) 1, "v2"),
                createKeyedSinkRecord((byte) 1, "v3")));

        int totalRecords = sink.writtenBatches.stream().mapToInt(Batch::size).sum();
        assertThat(totalRecords).isEqualTo(1);
    }

    @FixFor("debezium/dbz#1185")
    @Test
    void getCollectionIdFromRecordShouldResolveViaConfig() {
        SinkRecord kafkaRecord = createFlatSinkRecord((byte) 1, "John");
        sink.put(List.of(kafkaRecord));
        sink.forceFlush();

        assertThat(sink.writtenBatches.get(0).get(0).collectionId().name()).contains("topic");
    }

    private void consumeAndFlush(List<SinkRecord> records) {
        for (Batch batch : sink.put(records)) {
            sink.writeBatch(batch);
        }
        sink.forceFlush();
    }

    // --- Concrete test implementation ---

    static class TestChangeEventSink extends AbstractChangeEventSink {

        final List<Batch> writtenBatches = new ArrayList<>();

        TestChangeEventSink(SinkConnectorConfig config) {
            super(config);
        }

        @Override
        protected void doWriteBatch(Batch batch) {
            writtenBatches.add(batch);
        }

        @Override
        public CollectionId getCollectionId(String collectionName) {
            return new CollectionId(collectionName);
        }

        @Override
        public void close() {
        }
    }

    // --- Helper methods ---

    private static Schema createFlatSchema() {
        return SchemaBuilder.struct()
                .field("id", Schema.INT8_SCHEMA)
                .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                .build();
    }

    private static SinkRecord createFlatSinkRecord(byte id, String name) {
        Schema schema = createFlatSchema();
        Struct value = new Struct(schema).put("id", id).put("name", name);
        return new SinkRecord("topic", 0, Schema.INT8_SCHEMA, id, schema, value, 0);
    }

    private static Schema createKeySchema() {
        return SchemaBuilder.struct()
                .field("id", Schema.INT8_SCHEMA)
                .build();
    }

    private static SinkRecord createKeyedSinkRecord(byte id, String name) {
        Schema keySchema = createKeySchema();
        Struct key = new Struct(keySchema).put("id", id);
        Schema valueSchema = createFlatSchema();
        Struct value = new Struct(valueSchema).put("id", id).put("name", name);
        return new SinkRecord("topic", 0, keySchema, key, valueSchema, value, 0);
    }

    private static SinkRecord createTruncateSinkRecord() {
        Schema sourceSchema = SchemaBuilder.struct().field("ts_ms", Schema.INT64_SCHEMA).build();
        Schema envelopeSchema = SchemaBuilder.struct()
                .name("server1.schema.table.Envelope")
                .field(Envelope.FieldName.SOURCE, sourceSchema)
                .field(Envelope.FieldName.OPERATION, Schema.STRING_SCHEMA)
                .field(Envelope.FieldName.TIMESTAMP, Schema.OPTIONAL_INT64_SCHEMA)
                .build();

        Struct envelope = new Struct(envelopeSchema);
        envelope.put(Envelope.FieldName.SOURCE, new Struct(sourceSchema).put("ts_ms", Instant.now().toEpochMilli()));
        envelope.put(Envelope.FieldName.OPERATION, Envelope.Operation.TRUNCATE.code());
        envelope.put(Envelope.FieldName.TIMESTAMP, Instant.now().toEpochMilli());

        return new SinkRecord("topic", 0, null, null, envelopeSchema, envelope, 0);
    }
}
