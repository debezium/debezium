/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.chroniclequeue;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.debezium.doc.FixFor;

import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.wire.DocumentContext;

/**
 * Unit tests for the ChronicleQueue {@link SourceRecordJsonSerializer}.
 *
 * @author Chris Cranford
 */
class SourceRecordSerializerTest {

    @TempDir
    Path tempDir;

    private ChronicleQueue queue;
    private ExcerptAppender appender;
    private ExcerptTailer tailer;
    private SourceRecordJsonSerializer serializer;

    @BeforeEach
    void setUp() {
        queue = ChronicleQueue.singleBuilder(tempDir).build();
        appender = queue.createAppender();
        tailer = queue.createTailer();
        serializer = new SourceRecordJsonSerializer();
    }

    @AfterEach
    void tearDown() {
        if (queue != null) {
            queue.close();
        }
    }

    @Test
    @FixFor("dbz#1938")
    void shouldRoundTripRecordWithStructKeyAndValue() {
        Schema keySchema = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .build();
        Struct key = new Struct(keySchema).put("id", 42);

        Schema valueSchema = SchemaBuilder.struct()
                .field("name", Schema.STRING_SCHEMA)
                .field("active", Schema.BOOLEAN_SCHEMA)
                .field("score", Schema.FLOAT64_SCHEMA)
                .build();
        Struct value = new Struct(valueSchema)
                .put("name", "test-record")
                .put("active", true)
                .put("score", 95.5);

        Map<String, Object> sourcePartition = Map.of("server", "db1");
        Map<String, Object> sourceOffset = Map.of("pos", 12345, "file", "binlog.001");

        SourceRecord original = new SourceRecord(
                sourcePartition, sourceOffset,
                "test-topic", 0,
                keySchema, key,
                valueSchema, value,
                1000L,
                new ConnectHeaders());

        SourceRecord result = writeAndRead(original);

        assertThat(result.topic()).isEqualTo("test-topic");
        assertThat(result.kafkaPartition()).isEqualTo(0);
        assertThat(result.timestamp()).isEqualTo(1000L);
        assertThat((Map<String, Object>) result.sourcePartition()).containsEntry("server", "db1");
        assertThat((Map<String, Object>) result.sourceOffset()).containsEntry("file", "binlog.001");

        Struct resultKey = (Struct) result.key();
        assertThat(resultKey.getInt32("id")).isEqualTo(42);

        Struct resultValue = (Struct) result.value();
        assertThat(resultValue.getString("name")).isEqualTo("test-record");
        assertThat(resultValue.getBoolean("active")).isTrue();
        assertThat(resultValue.getFloat64("score")).isEqualTo(95.5);
    }

    @Test
    @FixFor("dbz#1938")
    void shouldRoundTripRecordWithNullKey() {
        Schema valueSchema = SchemaBuilder.struct()
                .field("data", Schema.STRING_SCHEMA)
                .build();
        Struct value = new Struct(valueSchema).put("data", "hello");

        SourceRecord original = new SourceRecord(
                Map.of("server", "db1"), Map.of("pos", 1),
                "test-topic", null,
                null, null,
                valueSchema, value,
                null,
                new ConnectHeaders());

        SourceRecord result = writeAndRead(original);

        assertThat(result.topic()).isEqualTo("test-topic");
        assertThat(result.kafkaPartition()).isNull();
        assertThat(result.timestamp()).isNull();
        assertThat(result.key()).isNull();
        assertThat(result.keySchema()).isNull();
        assertThat(((Struct) result.value()).getString("data")).isEqualTo("hello");
    }

    @Test
    @FixFor("dbz#1938")
    void shouldRoundTripRecordWithHeaders() {
        Schema valueSchema = Schema.STRING_SCHEMA;

        ConnectHeaders headers = new ConnectHeaders();
        headers.addString("header1", "value1");
        headers.addInt("header2", 42);

        SourceRecord original = new SourceRecord(
                Map.of("s", "p"), Map.of("o", 1),
                "topic", null,
                null, null,
                valueSchema, "test-value",
                null,
                headers);

        SourceRecord result = writeAndRead(original);

        assertThat(result.headers()).hasSize(2);
    }

    @Test
    @FixFor("dbz#1938")
    void shouldRoundTripRecordWithEmptyHeaders() {
        SourceRecord original = new SourceRecord(
                Map.of("s", "p"), Map.of("o", 1),
                "topic", null,
                null, null,
                Schema.STRING_SCHEMA, "value",
                null,
                new ConnectHeaders());

        SourceRecord result = writeAndRead(original);

        assertThat(result.headers()).isEmpty();
    }

    @Test
    @FixFor("dbz#1938")
    void shouldRoundTripRecordWithNestedStruct() {
        Schema addressSchema = SchemaBuilder.struct()
                .field("street", Schema.STRING_SCHEMA)
                .field("zip", Schema.STRING_SCHEMA)
                .build();

        Schema valueSchema = SchemaBuilder.struct()
                .field("name", Schema.STRING_SCHEMA)
                .field("address", addressSchema)
                .build();

        Struct address = new Struct(addressSchema)
                .put("street", "123 Main St")
                .put("zip", "12345");
        Struct value = new Struct(valueSchema)
                .put("name", "Alice")
                .put("address", address);

        SourceRecord original = new SourceRecord(
                Map.of("s", "p"), Map.of("o", 1),
                "topic", null,
                null, null,
                valueSchema, value,
                System.currentTimeMillis(),
                new ConnectHeaders());

        SourceRecord result = writeAndRead(original);

        Struct resultValue = (Struct) result.value();
        assertThat(resultValue.getString("name")).isEqualTo("Alice");
        Struct resultAddress = resultValue.getStruct("address");
        assertThat(resultAddress.getString("street")).isEqualTo("123 Main St");
        assertThat(resultAddress.getString("zip")).isEqualTo("12345");
    }

    @Test
    @FixFor("dbz#1938")
    void shouldPreserveSchemaFieldTypes() {
        Schema valueSchema = SchemaBuilder.struct()
                .field("int32", Schema.INT32_SCHEMA)
                .field("int64", Schema.INT64_SCHEMA)
                .field("float32", Schema.FLOAT32_SCHEMA)
                .field("float64", Schema.FLOAT64_SCHEMA)
                .field("bool", Schema.BOOLEAN_SCHEMA)
                .field("str", Schema.STRING_SCHEMA)
                .field("bytes", Schema.BYTES_SCHEMA)
                .build();

        Struct value = new Struct(valueSchema)
                .put("int32", 1)
                .put("int64", 2L)
                .put("float32", 3.0f)
                .put("float64", 4.0)
                .put("bool", true)
                .put("str", "test")
                .put("bytes", new byte[]{ 1, 2, 3 });

        SourceRecord original = new SourceRecord(
                Map.of("s", "p"), Map.of("o", 1),
                "topic", null,
                null, null,
                valueSchema, value,
                null,
                new ConnectHeaders());

        SourceRecord result = writeAndRead(original);

        Schema resultSchema = result.valueSchema();
        assertThat(resultSchema.field("int32").schema().type()).isEqualTo(Schema.Type.INT32);
        assertThat(resultSchema.field("int64").schema().type()).isEqualTo(Schema.Type.INT64);
        assertThat(resultSchema.field("float32").schema().type()).isEqualTo(Schema.Type.FLOAT32);
        assertThat(resultSchema.field("float64").schema().type()).isEqualTo(Schema.Type.FLOAT64);
        assertThat(resultSchema.field("bool").schema().type()).isEqualTo(Schema.Type.BOOLEAN);
        assertThat(resultSchema.field("str").schema().type()).isEqualTo(Schema.Type.STRING);
        assertThat(resultSchema.field("bytes").schema().type()).isEqualTo(Schema.Type.BYTES);
    }

    private SourceRecord writeAndRead(SourceRecord record) {
        try (DocumentContext dc = appender.writingDocument()) {
            serializer.write(record, dc.wire());
        }
        try (DocumentContext dc = tailer.readingDocument()) {
            assertThat(dc.isPresent()).isTrue();
            return serializer.read(dc.wire());
        }
    }
}
