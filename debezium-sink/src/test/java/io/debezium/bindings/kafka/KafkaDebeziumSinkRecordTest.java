/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.bindings.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.Set;

import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import io.debezium.data.Envelope;
import io.debezium.doc.FixFor;
import io.debezium.sink.SinkConnectorConfig.PrimaryKeyMode;

class KafkaDebeziumSinkRecordTest {

    private static final String TOPIC = "server1.schema.table";
    private static final String CE_PATTERN = ".*CloudEvents\\.Envelope$";

    private static Schema simpleSchema() {
        return SchemaBuilder.struct()
                .field("id", Schema.INT8_SCHEMA)
                .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                .build();
    }

    private static Struct simpleStruct(byte id, String name) {
        return new Struct(simpleSchema()).put("id", id).put("name", name);
    }

    private static SinkRecord simpleRecord(byte id, String name) {
        return new SinkRecord(TOPIC, 0,
                Schema.INT8_SCHEMA, id,
                simpleSchema(), simpleStruct(id, name),
                42);
    }

    private static Schema sourceSchema() {
        return SchemaBuilder.struct()
                .field("ts_ms", Schema.INT64_SCHEMA)
                .build();
    }

    private static Schema envelopeSchema() {
        Schema recordSchema = simpleSchema();
        Schema sourceSchema = sourceSchema();
        return SchemaBuilder.struct()
                .name("server1.schema.table.Envelope")
                .field(Envelope.FieldName.BEFORE, recordSchema)
                .field(Envelope.FieldName.AFTER, recordSchema)
                .field(Envelope.FieldName.SOURCE, sourceSchema)
                .field(Envelope.FieldName.OPERATION, Schema.STRING_SCHEMA)
                .field(Envelope.FieldName.TIMESTAMP, Schema.OPTIONAL_INT64_SCHEMA)
                .build();
    }

    private static SinkRecord createEnvelopeRecord(Envelope.Operation op, byte id, String afterName, String beforeName) {
        Schema envelopeSchema = envelopeSchema();
        Schema recordSchema = simpleSchema();
        Schema sourceSchema = sourceSchema();
        Schema keySchema = SchemaBuilder.struct().field("id", Schema.INT8_SCHEMA).build();
        Struct key = new Struct(keySchema).put("id", id);

        Struct envelope = new Struct(envelopeSchema);
        if (afterName != null) {
            envelope.put(Envelope.FieldName.AFTER, new Struct(recordSchema).put("id", id).put("name", afterName));
        }
        if (beforeName != null) {
            envelope.put(Envelope.FieldName.BEFORE, new Struct(recordSchema).put("id", id).put("name", beforeName));
        }
        envelope.put(Envelope.FieldName.SOURCE, new Struct(sourceSchema).put("ts_ms", Instant.now().toEpochMilli()));
        envelope.put(Envelope.FieldName.OPERATION, op.code());
        envelope.put(Envelope.FieldName.TIMESTAMP, Instant.now().toEpochMilli());

        return new SinkRecord(TOPIC, 0, keySchema, key, envelopeSchema, envelope, 42);
    }

    private static SinkRecord truncateRecord() {
        Schema sourceSchema = sourceSchema();
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

        return new SinkRecord(TOPIC, 0, null, null, envelopeSchema, envelope, 0);
    }

    @FixFor("debezium/dbz#1185")
    @Test
    void shouldReturnTopicPartitionAndOffset() {
        SinkRecord kafkaRecord = simpleRecord((byte) 1, "John");
        KafkaDebeziumSinkRecord record = new KafkaDebeziumSinkRecord(kafkaRecord, CE_PATTERN);

        assertThat(record.topicName()).isEqualTo(TOPIC);
        assertThat(record.partition()).isEqualTo(0);
        assertThat(record.offset()).isEqualTo(42L);
    }

    @FixFor("debezium/dbz#1185")
    @Test
    void shouldDetectTombstone() {
        SinkRecord kafkaRecord = new SinkRecord(TOPIC, 0, Schema.INT8_SCHEMA, (byte) 1, null, null, 0);
        KafkaDebeziumSinkRecord record = new KafkaDebeziumSinkRecord(kafkaRecord, CE_PATTERN);

        assertThat(record.isTombstone()).isTrue();
        assertThat(record.isDelete()).isTrue();
    }

    @FixFor("debezium/dbz#1185")
    @Test
    void shouldDetectFlattenedCreate() {
        SinkRecord kafkaRecord = simpleRecord((byte) 1, "John");
        KafkaDebeziumSinkRecord record = new KafkaDebeziumSinkRecord(kafkaRecord, CE_PATTERN);

        assertThat(record.isFlattened()).isTrue();
        assertThat(record.isDebeziumMessage()).isFalse();
        assertThat(record.isDelete()).isFalse();
        assertThat(record.isTruncate()).isFalse();
        assertThat(record.isTombstone()).isFalse();
    }

    @FixFor("debezium/dbz#1185")
    @Test
    void shouldDetectFlattenedDelete() {
        SinkRecord kafkaRecord = new SinkRecord(TOPIC, 0,
                Schema.INT8_SCHEMA, (byte) 1,
                SchemaBuilder.struct().field("id", Schema.INT8_SCHEMA).build(), null,
                0);
        KafkaDebeziumSinkRecord record = new KafkaDebeziumSinkRecord(kafkaRecord, CE_PATTERN);

        assertThat(record.isDelete()).isTrue();
    }

    @FixFor("debezium/dbz#1185")
    @Test
    void shouldDetectDebeziumEnvelope() {
        SinkRecord kafkaRecord = createEnvelopeRecord(Envelope.Operation.CREATE, (byte) 1, "John", null);
        KafkaDebeziumSinkRecord record = new KafkaDebeziumSinkRecord(kafkaRecord, CE_PATTERN);

        assertThat(record.isDebeziumMessage()).isTrue();
        assertThat(record.isFlattened()).isFalse();
        assertThat(record.isDelete()).isFalse();
        assertThat(record.isTruncate()).isFalse();
    }

    @FixFor("debezium/dbz#1185")
    @Test
    void shouldDetectDebeziumDelete() {
        SinkRecord kafkaRecord = createEnvelopeRecord(Envelope.Operation.DELETE, (byte) 1, null, "John");
        KafkaDebeziumSinkRecord record = new KafkaDebeziumSinkRecord(kafkaRecord, CE_PATTERN);

        assertThat(record.isDebeziumMessage()).isTrue();
        assertThat(record.isDelete()).isTrue();
    }

    @FixFor("debezium/dbz#1185")
    @Test
    void shouldDetectDebeziumTruncate() {
        SinkRecord kafkaRecord = truncateRecord();
        KafkaDebeziumSinkRecord record = new KafkaDebeziumSinkRecord(kafkaRecord, CE_PATTERN);

        assertThat(record.isDebeziumMessage()).isTrue();
        assertThat(record.isTruncate()).isTrue();
        assertThat(record.isDelete()).isFalse();
    }

    @FixFor("debezium/dbz#1185")
    @Test
    void shouldReturnPayloadForFlattenedRecord() {
        SinkRecord kafkaRecord = simpleRecord((byte) 1, "John");
        KafkaDebeziumSinkRecord record = new KafkaDebeziumSinkRecord(kafkaRecord, CE_PATTERN);

        Struct payload = record.getPayload();
        assertThat(payload).isNotNull();
        assertThat(payload.get("id")).isEqualTo((byte) 1);
        assertThat(payload.get("name")).isEqualTo("John");
    }

    @FixFor("debezium/dbz#1185")
    @Test
    void shouldReturnAfterPayloadForDebeziumCreate() {
        SinkRecord kafkaRecord = createEnvelopeRecord(Envelope.Operation.CREATE, (byte) 1, "John", null);
        KafkaDebeziumSinkRecord record = new KafkaDebeziumSinkRecord(kafkaRecord, CE_PATTERN);

        Struct payload = record.getPayload();
        assertThat(payload).isNotNull();
        assertThat(payload.get("id")).isEqualTo((byte) 1);
        assertThat(payload.get("name")).isEqualTo("John");
    }

    @FixFor("debezium/dbz#1185")
    @Test
    void shouldReturnBeforePayloadForDebeziumDelete() {
        SinkRecord kafkaRecord = createEnvelopeRecord(Envelope.Operation.DELETE, (byte) 1, null, "OldName");
        KafkaDebeziumSinkRecord record = new KafkaDebeziumSinkRecord(kafkaRecord, CE_PATTERN);

        Struct payload = record.getPayload();
        assertThat(payload).isNotNull();
        assertThat(payload.get("name")).isEqualTo("OldName");
    }

    @FixFor("debezium/dbz#1185")
    @Test
    void shouldReturnKafkaCoordinates() {
        SinkRecord kafkaRecord = simpleRecord((byte) 1, "John");
        KafkaDebeziumSinkRecord record = new KafkaDebeziumSinkRecord(kafkaRecord, CE_PATTERN);

        Struct coords = record.kafkaCoordinates();
        assertThat(coords.get("__connect_topic")).isEqualTo(TOPIC);
        assertThat(coords.get("__connect_partition")).isEqualTo(0);
        assertThat(coords.get("__connect_offset")).isEqualTo(42L);
    }

    @FixFor("debezium/dbz#1185")
    @Test
    void shouldReturnKafkaFields() {
        SinkRecord kafkaRecord = simpleRecord((byte) 1, "John");
        KafkaDebeziumSinkRecord record = new KafkaDebeziumSinkRecord(kafkaRecord, CE_PATTERN);

        var fields = record.kafkaFields();
        assertThat(fields).containsKeys("__connect_topic", "__connect_partition", "__connect_offset");
        assertThat(fields.get("__connect_topic").isKey()).isTrue();
    }

    @FixFor("debezium/dbz#1185")
    @Test
    void shouldReturnOriginalKafkaRecord() {
        SinkRecord kafkaRecord = simpleRecord((byte) 1, "John");
        KafkaDebeziumSinkRecord record = new KafkaDebeziumSinkRecord(kafkaRecord, CE_PATTERN);

        assertThat(record.getOriginalKafkaRecord()).isSameAs(kafkaRecord);
    }

    @FixFor("debezium/dbz#1185")
    @Test
    void getFilteredKeyShouldReturnNullForModeNone() {
        SinkRecord kafkaRecord = simpleRecord((byte) 1, "John");
        KafkaDebeziumSinkRecord record = new KafkaDebeziumSinkRecord(kafkaRecord, CE_PATTERN);

        assertThat(record.getFilteredKey(PrimaryKeyMode.NONE, Set.of(), null)).isNull();
    }

    @FixFor("debezium/dbz#1185")
    @Test
    void getFilteredKeyShouldReturnKeyForModeRecordKey() {
        Schema keySchema = SchemaBuilder.struct().field("id", Schema.INT8_SCHEMA).build();
        Struct key = new Struct(keySchema).put("id", (byte) 1);
        SinkRecord kafkaRecord = new SinkRecord(TOPIC, 0,
                keySchema, key,
                simpleSchema(), simpleStruct((byte) 1, "John"),
                42);
        KafkaDebeziumSinkRecord record = new KafkaDebeziumSinkRecord(kafkaRecord, CE_PATTERN);

        Struct filteredKey = record.getFilteredKey(PrimaryKeyMode.RECORD_KEY, Set.of(), null);
        assertThat(filteredKey).isNotNull();
        assertThat(filteredKey.get("id")).isEqualTo((byte) 1);
    }

    @FixFor("debezium/dbz#1185")
    @Test
    void getFilteredKeyShouldReturnKafkaCoordinatesForModeKafka() {
        SinkRecord kafkaRecord = simpleRecord((byte) 1, "John");
        KafkaDebeziumSinkRecord record = new KafkaDebeziumSinkRecord(kafkaRecord, CE_PATTERN);

        Struct filteredKey = record.getFilteredKey(PrimaryKeyMode.KAFKA, Set.of(), null);
        assertThat(filteredKey).isNotNull();
        assertThat(filteredKey.get("__connect_topic")).isEqualTo(TOPIC);
    }

    @FixFor("debezium/dbz#1185")
    @Test
    void getFilteredKeyShouldThrowForRecordKeyWithNullSchema() {
        SinkRecord kafkaRecord = new SinkRecord(TOPIC, 0, null, null,
                simpleSchema(), simpleStruct((byte) 1, "John"), 0);
        KafkaDebeziumSinkRecord record = new KafkaDebeziumSinkRecord(kafkaRecord, CE_PATTERN);

        assertThatThrownBy(() -> record.getFilteredKey(PrimaryKeyMode.RECORD_KEY, Set.of(), null))
                .hasMessageContaining("cannot have null schema");
    }

    @FixFor("debezium/dbz#1185")
    @Test
    void getFilteredKeyShouldReturnHeadersForModeRecordHeader() {
        ConnectHeaders headers = new ConnectHeaders();
        headers.addString("h1", "val1");
        SinkRecord kafkaRecord = new SinkRecord(TOPIC, 0,
                null, null,
                simpleSchema(), simpleStruct((byte) 1, "John"),
                0, null, TimestampType.NO_TIMESTAMP_TYPE, headers);
        KafkaDebeziumSinkRecord record = new KafkaDebeziumSinkRecord(kafkaRecord, CE_PATTERN);

        Struct filteredKey = record.getFilteredKey(PrimaryKeyMode.RECORD_HEADER, Set.of(), null);
        assertThat(filteredKey).isNotNull();
        assertThat(filteredKey.get("h1")).isEqualTo("val1");
    }

    @FixFor("debezium/dbz#1185")
    @Test
    void shouldDetectSchemaChangeRecord() {
        Schema valueSchema = SchemaBuilder.struct()
                .name("io.debezium.connector.mysql.SchemaChangeValue")
                .field("dummy", Schema.STRING_SCHEMA)
                .build();
        Struct value = new Struct(valueSchema).put("dummy", "x");
        SinkRecord kafkaRecord = new SinkRecord(TOPIC, 0, null, null, valueSchema, value, 0);
        KafkaDebeziumSinkRecord record = new KafkaDebeziumSinkRecord(kafkaRecord, CE_PATTERN);

        assertThat(record.isSchemaChange()).isTrue();
    }

    @FixFor("debezium/dbz#1185")
    @Test
    void shouldFilterPayloadFields() {
        SinkRecord kafkaRecord = simpleRecord((byte) 1, "John");
        KafkaDebeziumSinkRecord record = new KafkaDebeziumSinkRecord(kafkaRecord, CE_PATTERN);

        Struct filtered = record.getFilteredPayload((topic, field) -> field.equals("id"));
        assertThat(filtered.schema().fields()).hasSize(1);
        assertThat(filtered.get("id")).isEqualTo((byte) 1);
    }
}
