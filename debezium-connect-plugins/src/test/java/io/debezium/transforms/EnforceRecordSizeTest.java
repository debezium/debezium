/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.util.ApproximateStructSizeCalculator;

public class EnforceRecordSizeTest {

    private EnforceRecordSize<SourceRecord> transform;

    @BeforeEach
    public void setup() {
        transform = new EnforceRecordSize<>();
    }

    @AfterEach
    public void teardown() {
        transform.close();
    }

    @Test
    public void shouldNotTruncateWhenMessageSizeUnderLimit() {
        Map<String, String> config = new HashMap<>();
        config.put(EnforceRecordSize.MAX_BYTES_CONF, "10000");
        transform.configure(config);

        SourceRecord record = createRecordWithStringValue("short");
        SourceRecord result = transform.apply(record);

        Struct after = getAfterStruct(result);
        assertThat(after.getString("text_col")).isEqualTo("short");
    }

    @Test
    public void shouldTruncateStringColumnWhenMessageExceedsLimit() {
        int maxBytes = 600;
        Map<String, String> config = new HashMap<>();
        config.put(EnforceRecordSize.MAX_BYTES_CONF, String.valueOf(maxBytes));
        config.put(EnforceRecordSize.MIN_FIELD_SIZE_CONF, "0");
        transform.configure(config);

        String largeValue = "a".repeat(5000);
        SourceRecord record = createRecordWithStringValue(largeValue);
        SourceRecord result = transform.apply(record);

        Struct after = getAfterStruct(result);
        String truncatedValue = after.getString("text_col");
        assertThat(truncatedValue.length()).isLessThan(largeValue.length());
        long resultSize = ApproximateStructSizeCalculator.getApproximateRecordSize(result);
        assertThat(resultSize).isLessThanOrEqualTo(maxBytes);
    }

    @Test
    public void shouldTruncateBytesColumnWhenMessageExceedsLimit() {
        int maxBytes = 600;
        Map<String, String> config = new HashMap<>();
        config.put(EnforceRecordSize.MAX_BYTES_CONF, String.valueOf(maxBytes));
        config.put(EnforceRecordSize.MIN_FIELD_SIZE_CONF, "0");
        transform.configure(config);

        byte[] largeValue = new byte[5000];
        SourceRecord record = createRecordWithBytesValue(largeValue);
        SourceRecord result = transform.apply(record);

        Struct after = getAfterStruct(result);
        byte[] truncatedValue = after.getBytes("blob_col");
        assertThat(truncatedValue.length).isLessThan(largeValue.length);
        long resultSize = ApproximateStructSizeCalculator.getApproximateRecordSize(result);
        assertThat(resultSize).isLessThanOrEqualTo(maxBytes);
    }

    @Test
    public void shouldTruncateProportionally() {
        int maxBytes = 600;
        Map<String, String> config = new HashMap<>();
        config.put(EnforceRecordSize.MAX_BYTES_CONF, String.valueOf(maxBytes));
        config.put(EnforceRecordSize.MIN_FIELD_SIZE_CONF, "0");
        transform.configure(config);

        String col1Value = "a".repeat(2000);
        String col2Value = "b".repeat(4000);
        SourceRecord record = createRecordWithTwoStringColumns(col1Value, col2Value);
        SourceRecord result = transform.apply(record);

        Struct after = getAfterStruct(result);
        String col1Result = after.getString("small_col");
        String col2Result = after.getString("large_col");
        assertThat(col1Result.length()).isLessThan(col1Value.length());
        assertThat(col2Result.length()).isLessThan(col2Value.length());
        long resultSize = ApproximateStructSizeCalculator.getApproximateRecordSize(result);
        assertThat(resultSize).isLessThanOrEqualTo(maxBytes);
    }

    @Test
    public void shouldNotTruncateNonStringColumns() {
        int maxBytes = 600;
        Map<String, String> config = new HashMap<>();
        config.put(EnforceRecordSize.MAX_BYTES_CONF, String.valueOf(maxBytes));
        config.put(EnforceRecordSize.MIN_FIELD_SIZE_CONF, "0");
        transform.configure(config);

        SourceRecord record = createRecordWithIntAndString(42, "x".repeat(5000));
        SourceRecord result = transform.apply(record);

        Struct after = getAfterStruct(result);
        assertThat(after.getInt32("int_col")).isEqualTo(42);
        String truncatedStr = after.getString("text_col");
        assertThat(truncatedStr.length()).isLessThan(5000);
        long resultSize = ApproximateStructSizeCalculator.getApproximateRecordSize(result);
        assertThat(resultSize).isLessThanOrEqualTo(maxBytes);
    }

    @Test
    public void shouldHandleNullValues() {
        Map<String, String> config = new HashMap<>();
        config.put(EnforceRecordSize.MAX_BYTES_CONF, "100");
        transform.configure(config);

        SourceRecord record = createRecordWithNullColumn();
        SourceRecord result = transform.apply(record);

        Struct after = getAfterStruct(result);
        assertThat(after.getString("text_col")).isNull();
        assertThat(after.getInt32("int_col")).isEqualTo(1);
    }

    @Test
    public void shouldHandleNullRecord() {
        Map<String, String> config = new HashMap<>();
        config.put(EnforceRecordSize.MAX_BYTES_CONF, "200");
        transform.configure(config);

        SourceRecord result = transform.apply(null);
        assertThat(result).isNull();
    }

    @Test
    public void shouldNotOverTruncateUpdateEvents() {
        int maxBytes = 2000;
        Map<String, String> config = new HashMap<>();
        config.put(EnforceRecordSize.MAX_BYTES_CONF, String.valueOf(maxBytes));
        config.put(EnforceRecordSize.MIN_FIELD_SIZE_CONF, "0");
        transform.configure(config);

        SourceRecord record = createUpdateRecord("x".repeat(5000), "y".repeat(5000));
        long originalSize = ApproximateStructSizeCalculator.getApproximateRecordSize(record);
        SourceRecord result = transform.apply(record);

        Struct value = (Struct) result.value();
        Struct before = value.getStruct("before");
        Struct after = value.getStruct("after");
        assertThat(before.getString("text_col").length()).isLessThan(5000);
        assertThat(after.getString("text_col").length()).isLessThan(5000);
        long resultSize = ApproximateStructSizeCalculator.getApproximateRecordSize(result);
        assertThat(resultSize).isLessThanOrEqualTo(maxBytes);
        assertThat(resultSize).isGreaterThan((long) (maxBytes * 0.7));
    }

    @Test
    public void shouldRejectInvalidMaxMessageSize() {
        Map<String, String> config = new HashMap<>();
        config.put(EnforceRecordSize.MAX_BYTES_CONF, "0");

        assertThrows(ConfigException.class, () -> transform.configure(config));
    }

    @Test
    public void shouldRejectNegativeMaxMessageSize() {
        Map<String, String> config = new HashMap<>();
        config.put(EnforceRecordSize.MAX_BYTES_CONF, "-1");

        assertThrows(ConfigException.class, () -> transform.configure(config));
    }

    @Test
    public void shouldPassThroughNonStructRecords() {
        Map<String, String> config = new HashMap<>();
        config.put(EnforceRecordSize.MAX_BYTES_CONF, "200");
        transform.configure(config);

        SourceRecord record = new SourceRecord(null, null, "topic", 0, Schema.STRING_SCHEMA, "plaintext");
        SourceRecord result = transform.apply(record);
        assertThat(result.value()).isEqualTo("plaintext");
    }

    @Test
    public void shouldApplyCompressionRatioReducesEffectiveSize() {
        Map<String, String> config = new HashMap<>();
        config.put(EnforceRecordSize.MAX_BYTES_CONF, "600");
        config.put(EnforceRecordSize.COMPRESSION_RATIO_CONF, "0.1");
        transform.configure(config);

        String largeValue = "a".repeat(1000);
        SourceRecord record = createRecordWithStringValue(largeValue);
        SourceRecord result = transform.apply(record);

        Struct after = getAfterStruct(result);
        assertThat(after.getString("text_col")).isEqualTo(largeValue);
    }

    @Test
    public void shouldApplyCompressionRatioIncreasesEffectiveSize() {
        int maxBytes = 5000;
        Map<String, String> config = new HashMap<>();
        config.put(EnforceRecordSize.MAX_BYTES_CONF, String.valueOf(maxBytes));
        config.put(EnforceRecordSize.COMPRESSION_RATIO_CONF, "2.0");
        config.put(EnforceRecordSize.MIN_FIELD_SIZE_CONF, "0");
        transform.configure(config);

        String largeValue = "a".repeat(30000);
        SourceRecord record = createRecordWithStringValue(largeValue);
        SourceRecord result = transform.apply(record);

        Struct after = getAfterStruct(result);
        assertThat(after.getString("text_col").length()).isLessThan(30000);
        long resultSize = ApproximateStructSizeCalculator.getApproximateRecordSize(result);
        assertThat(resultSize).isLessThanOrEqualTo(maxBytes);
    }

    @Test
    public void shouldRejectInvalidCompressionRatio() {
        Map<String, String> config = new HashMap<>();
        config.put(EnforceRecordSize.MAX_BYTES_CONF, "200");
        config.put(EnforceRecordSize.COMPRESSION_RATIO_CONF, "0");

        assertThrows(ConfigException.class, () -> transform.configure(config));
    }

    @Test
    public void shouldRejectNegativeMinFieldSize() {
        Map<String, String> config = new HashMap<>();
        config.put(EnforceRecordSize.MAX_BYTES_CONF, "200");
        config.put(EnforceRecordSize.MIN_FIELD_SIZE_CONF, "-1");

        assertThrows(ConfigException.class, () -> transform.configure(config));
    }

    @Test
    public void shouldHandleMultiByteCharacters() {
        int maxBytes = 600;
        Map<String, String> config = new HashMap<>();
        config.put(EnforceRecordSize.MAX_BYTES_CONF, String.valueOf(maxBytes));
        config.put(EnforceRecordSize.MIN_FIELD_SIZE_CONF, "0");
        transform.configure(config);

        String multiByteValue = "中".repeat(5000);
        SourceRecord record = createRecordWithStringValue(multiByteValue);
        SourceRecord result = transform.apply(record);

        Struct after = getAfterStruct(result);
        String truncatedValue = after.getString("text_col");
        assertThat(truncatedValue.length()).isLessThan(multiByteValue.length());
        long resultSize = ApproximateStructSizeCalculator.getApproximateRecordSize(result);
        assertThat(resultSize).isLessThanOrEqualTo(maxBytes);
    }

    @Test
    public void shouldNotTruncateFieldsBelowMinFieldSize() {
        Map<String, String> config = new HashMap<>();
        config.put(EnforceRecordSize.MAX_BYTES_CONF, "2000");
        config.put(EnforceRecordSize.MIN_FIELD_SIZE_CONF, "20");
        transform.configure(config);

        // 9 small fields (10 chars each) + 1 large field (5000 chars)
        // Total ~5090 bytes, limit 2000. With min.field.size=20, the small fields
        // should not be truncated since they are already below the threshold.
        Schema recordSchema = SchemaBuilder.struct()
                .field("small_1", Schema.OPTIONAL_STRING_SCHEMA)
                .field("small_2", Schema.OPTIONAL_STRING_SCHEMA)
                .field("small_3", Schema.OPTIONAL_STRING_SCHEMA)
                .field("small_4", Schema.OPTIONAL_STRING_SCHEMA)
                .field("small_5", Schema.OPTIONAL_STRING_SCHEMA)
                .field("small_6", Schema.OPTIONAL_STRING_SCHEMA)
                .field("small_7", Schema.OPTIONAL_STRING_SCHEMA)
                .field("small_8", Schema.OPTIONAL_STRING_SCHEMA)
                .field("small_9", Schema.OPTIONAL_STRING_SCHEMA)
                .field("large", Schema.OPTIONAL_STRING_SCHEMA)
                .build();
        Schema sourceSchema = SchemaBuilder.struct().field("db", Schema.STRING_SCHEMA).build();
        Schema envelopeSchema = SchemaBuilder.struct()
                .field("before", recordSchema)
                .field("after", recordSchema)
                .field("op", Schema.STRING_SCHEMA)
                .field("source", sourceSchema)
                .build();

        Struct afterStruct = new Struct(recordSchema);
        for (int i = 1; i <= 9; i++) {
            afterStruct.put("small_" + i, "x".repeat(10));
        }
        afterStruct.put("large", "y".repeat(5000));

        Struct sourceStruct = new Struct(sourceSchema).put("db", "test");
        Struct envelope = new Struct(envelopeSchema)
                .put("after", afterStruct)
                .put("op", "c")
                .put("source", sourceStruct);

        SourceRecord record = new SourceRecord(Map.of(), Map.of(), "topic", 0, envelopeSchema, envelope);
        SourceRecord result = transform.apply(record);

        Struct after = getAfterStruct(result);
        // All small fields should remain untouched
        for (int i = 1; i <= 9; i++) {
            assertThat(after.getString("small_" + i)).isEqualTo("x".repeat(10));
        }
        // Large field should be truncated
        assertThat(after.getString("large").length()).isLessThan(5000);
    }

    @Test
    public void shouldOnlyTruncateFieldsAboveMinFieldSize() {
        Map<String, String> config = new HashMap<>();
        config.put(EnforceRecordSize.MAX_BYTES_CONF, "500");
        config.put(EnforceRecordSize.MIN_FIELD_SIZE_CONF, "50");
        transform.configure(config);

        // Two fields: one at 30 chars (below min), one at 2000 chars (above min)
        // Only the large one should be truncated
        Schema recordSchema = SchemaBuilder.struct()
                .field("small_field", Schema.OPTIONAL_STRING_SCHEMA)
                .field("large_field", Schema.OPTIONAL_STRING_SCHEMA)
                .build();
        Schema sourceSchema = SchemaBuilder.struct().field("db", Schema.STRING_SCHEMA).build();
        Schema envelopeSchema = SchemaBuilder.struct()
                .field("before", recordSchema)
                .field("after", recordSchema)
                .field("op", Schema.STRING_SCHEMA)
                .field("source", sourceSchema)
                .build();

        Struct afterStruct = new Struct(recordSchema)
                .put("small_field", "a".repeat(30))
                .put("large_field", "b".repeat(2000));
        Struct sourceStruct = new Struct(sourceSchema).put("db", "test");
        Struct envelope = new Struct(envelopeSchema)
                .put("after", afterStruct)
                .put("op", "c")
                .put("source", sourceStruct);

        SourceRecord record = new SourceRecord(Map.of(), Map.of(), "topic", 0, envelopeSchema, envelope);
        SourceRecord result = transform.apply(record);

        Struct after = getAfterStruct(result);
        assertThat(after.getString("small_field")).isEqualTo("a".repeat(30));
        assertThat(after.getString("large_field").length()).isLessThan(2000);
    }

    // Helper methods

    private static final Schema SOURCE_SCHEMA = SchemaBuilder.struct().field("db", Schema.STRING_SCHEMA).build();

    private Struct getAfterStruct(SourceRecord record) {
        Struct value = (Struct) record.value();
        return value.getStruct("after");
    }

    private SourceRecord createRecord(Schema recordSchema, Struct afterStruct) {
        Schema envelopeSchema = SchemaBuilder.struct()
                .field("before", recordSchema)
                .field("after", recordSchema)
                .field("op", Schema.STRING_SCHEMA)
                .field("source", SOURCE_SCHEMA)
                .build();

        Struct sourceStruct = new Struct(SOURCE_SCHEMA).put("db", "test");
        Struct envelope = new Struct(envelopeSchema)
                .put("after", afterStruct)
                .put("op", "c")
                .put("source", sourceStruct);

        return new SourceRecord(Map.of(), Map.of(), "topic", 0, envelopeSchema, envelope);
    }

    private SourceRecord createRecordWithStringValue(String textValue) {
        Schema recordSchema = SchemaBuilder.struct()
                .field("text_col", Schema.OPTIONAL_STRING_SCHEMA)
                .build();
        return createRecord(recordSchema, new Struct(recordSchema).put("text_col", textValue));
    }

    private SourceRecord createRecordWithBytesValue(byte[] blobValue) {
        Schema recordSchema = SchemaBuilder.struct()
                .field("blob_col", Schema.OPTIONAL_BYTES_SCHEMA)
                .build();
        return createRecord(recordSchema, new Struct(recordSchema).put("blob_col", ByteBuffer.wrap(blobValue)));
    }

    private SourceRecord createRecordWithTwoStringColumns(String smallValue, String largeValue) {
        Schema recordSchema = SchemaBuilder.struct()
                .field("small_col", Schema.OPTIONAL_STRING_SCHEMA)
                .field("large_col", Schema.OPTIONAL_STRING_SCHEMA)
                .build();
        Struct afterStruct = new Struct(recordSchema)
                .put("small_col", smallValue)
                .put("large_col", largeValue);
        return createRecord(recordSchema, afterStruct);
    }

    private SourceRecord createRecordWithIntAndString(int intValue, String textValue) {
        Schema recordSchema = SchemaBuilder.struct()
                .field("int_col", Schema.OPTIONAL_INT32_SCHEMA)
                .field("text_col", Schema.OPTIONAL_STRING_SCHEMA)
                .build();
        Struct afterStruct = new Struct(recordSchema)
                .put("int_col", intValue)
                .put("text_col", textValue);
        return createRecord(recordSchema, afterStruct);
    }

    private SourceRecord createRecordWithNullColumn() {
        Schema recordSchema = SchemaBuilder.struct()
                .field("int_col", Schema.OPTIONAL_INT32_SCHEMA)
                .field("text_col", Schema.OPTIONAL_STRING_SCHEMA)
                .build();
        Struct afterStruct = new Struct(recordSchema)
                .put("int_col", 1)
                .put("text_col", null);
        return createRecord(recordSchema, afterStruct);
    }

    private SourceRecord createUpdateRecord(String beforeValue, String afterValue) {
        Schema recordSchema = SchemaBuilder.struct()
                .field("text_col", Schema.OPTIONAL_STRING_SCHEMA)
                .build();
        Schema envelopeSchema = SchemaBuilder.struct()
                .field("before", recordSchema)
                .field("after", recordSchema)
                .field("op", Schema.STRING_SCHEMA)
                .field("source", SOURCE_SCHEMA)
                .build();

        Struct beforeStruct = new Struct(recordSchema).put("text_col", beforeValue);
        Struct afterStruct = new Struct(recordSchema).put("text_col", afterValue);
        Struct sourceStruct = new Struct(SOURCE_SCHEMA).put("db", "test");
        Struct envelope = new Struct(envelopeSchema)
                .put("before", beforeStruct)
                .put("after", afterStruct)
                .put("op", "u")
                .put("source", sourceStruct);

        return new SourceRecord(Map.of(), Map.of(), "topic", 0, envelopeSchema, envelope);
    }
}
