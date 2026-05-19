/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.sink;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.data.Envelope;
import io.debezium.metadata.CollectionId;
import io.debezium.sink.batch.Batch;
import io.debezium.sink.naming.CollectionNamingStrategy;

class AbstractChangeEventSinkTest {

    private TestChangeEventSink sink;
    private SinkConnectorConfig config;

    @BeforeEach
    void setUp() {
        config = mock(SinkConnectorConfig.class);
        when(config.getBatchSize()).thenReturn(500);
        when(config.getPrimaryKeyMode()).thenReturn(SinkConnectorConfig.PrimaryKeyMode.NONE);
        when(config.getPrimaryKeyFields()).thenReturn(Set.of());
        when(config.fieldFilter()).thenReturn(null);
        when(config.isTruncateEnabled()).thenReturn(false);
        when(config.isDeleteEnabled()).thenReturn(false);
        when(config.getCollectionNameFormat()).thenReturn("${topic}");
        when(config.cloudEventsSchemaNamePattern()).thenReturn(".*CloudEvents\\.Envelope$");

        CollectionNamingStrategy namingStrategy = mock(CollectionNamingStrategy.class);
        when(namingStrategy.resolveCollectionName(org.mockito.ArgumentMatchers.any(), anyString())).thenAnswer(invocation -> {
            DebeziumSinkRecord record = invocation.getArgument(0);
            return record.topicName().replace(".", "_");
        });
        when(config.getCollectionNamingStrategy()).thenReturn(namingStrategy);

        sink = new TestChangeEventSink(config);
    }

    @Test
    void putShouldBufferRecords() {
        Batch batch = sink.put(List.of(createFlatSinkRecord((byte) 1, "John")));

        assertThat(batch).isEmpty();
    }

    @Test
    void flushShouldDrainBufferedRecords() {
        sink.put(List.of(createFlatSinkRecord((byte) 1, "John")));
        sink.flush();

        assertThat(sink.writtenBatches).hasSize(1);
        assertThat(sink.writtenBatches.get(0)).hasSize(1);
    }

    @Test
    void flushOnEmptyBufferShouldNotCallWriteBatch() {
        sink.flush();

        assertThat(sink.writtenBatches).isEmpty();
    }

    @Test
    void putShouldSkipSchemaChangeRecords() {
        Schema valueSchema = SchemaBuilder.struct()
                .name("io.debezium.connector.mysql.SchemaChangeValue")
                .field("dummy", Schema.STRING_SCHEMA)
                .build();
        Struct value = new Struct(valueSchema).put("dummy", "x");
        SinkRecord schemaChange = new SinkRecord("topic", 0, null, null, valueSchema, value, 0);

        sink.put(List.of(schemaChange));
        sink.flush();

        assertThat(sink.writtenBatches).isEmpty();
    }

    @Test
    void putShouldSkipDeleteWhenDeletesDisabled() {
        when(config.isDeleteEnabled()).thenReturn(false);
        sink = new TestChangeEventSink(config);

        SinkRecord deleteRecord = new SinkRecord("topic", 0,
                Schema.INT8_SCHEMA, (byte) 1,
                SchemaBuilder.struct().field("id", Schema.INT8_SCHEMA).build(), null,
                0);

        sink.put(List.of(deleteRecord));
        sink.flush();

        assertThat(sink.writtenBatches).isEmpty();
    }

    @Test
    void putShouldIncludeDeleteWhenDeletesEnabled() {
        when(config.isDeleteEnabled()).thenReturn(true);
        sink = new TestChangeEventSink(config);

        SinkRecord deleteRecord = new SinkRecord("topic", 0,
                Schema.INT8_SCHEMA, (byte) 1,
                SchemaBuilder.struct().field("id", Schema.INT8_SCHEMA).build(), null,
                0);

        sink.put(List.of(deleteRecord));
        sink.flush();

        assertThat(sink.writtenBatches).hasSize(1);
        assertThat(sink.writtenBatches.get(0).get(0).record().isDelete()).isTrue();
    }

    @Test
    void putShouldSkipTruncateWhenTruncateDisabled() {
        when(config.isTruncateEnabled()).thenReturn(false);
        sink = new TestChangeEventSink(config);

        sink.put(List.of(createTruncateSinkRecord()));
        sink.flush();

        assertThat(sink.writtenBatches).isEmpty();
    }

    @Test
    void putShouldIncludeTruncateWhenTruncateEnabled() {
        when(config.isTruncateEnabled()).thenReturn(true);
        sink = new TestChangeEventSink(config);

        sink.put(List.of(createTruncateSinkRecord()));
        sink.flush();

        assertThat(sink.writtenBatches).hasSize(1);
        assertThat(sink.writtenBatches.get(0).get(0).record().isTruncate()).isTrue();
    }

    @Test
    void putShouldBufferMultipleRecords() {
        sink.put(List.of(
                createFlatSinkRecord((byte) 1, "John"),
                createFlatSinkRecord((byte) 2, "Jane")));
        sink.flush();

        assertThat(sink.writtenBatches).hasSize(1);
        assertThat(sink.writtenBatches.get(0)).hasSize(2);
    }

    @Test
    void forcePollShouldReturnAllBuffered() {
        sink.put(List.of(createFlatSinkRecord((byte) 1, "John")));

        Batch batch = sink.forcePoll();
        assertThat(batch).hasSize(1);
    }

    @Test
    void getCollectionIdFromRecordShouldResolveViaConfig() {
        SinkRecord kafkaRecord = createFlatSinkRecord((byte) 1, "John");
        sink.put(List.of(kafkaRecord));
        sink.flush();

        assertThat(sink.writtenBatches.get(0).get(0).collectionId().name()).contains("topic");
    }

    // --- Concrete test implementation ---

    static class TestChangeEventSink extends AbstractChangeEventSink {

        final List<Batch> writtenBatches = new ArrayList<>();

        TestChangeEventSink(SinkConnectorConfig config) {
            super(config);
        }

        @Override
        protected void writeBatch(Batch batch) {
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
