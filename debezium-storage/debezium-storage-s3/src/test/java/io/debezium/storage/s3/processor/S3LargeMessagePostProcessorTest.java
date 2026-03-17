/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.s3.processor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.DebeziumException;
import io.debezium.data.Envelope;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

public class S3LargeMessagePostProcessorTest {

    private static final String BUCKET = "test-bucket";
    private static final String REGION = "us-east-1";

    private static final int SMALL_THRESHOLD = 10;

    private S3Client mockS3Client;
    private S3LargeMessagePostProcessor processor;

    @BeforeEach
    void setUp() {
        mockS3Client = mock(S3Client.class);
        when(mockS3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
                .thenReturn(PutObjectResponse.builder().build());

        processor = new S3LargeMessagePostProcessor();
        processor.s3ClientOverride = mockS3Client;
    }

    @AfterEach
    void tearDown() {
        processor.close();
    }

    @Test
    void testConfigureValidConfig() {
        processor.configure(baseConfig());
    }

    @Test
    void testConfigureMissingBucket() {

        Map<String, Object> config = baseConfig();
        config.remove(S3LargeMessagePostProcessor.BUCKET_NAME_CONFIG);

        assertThrows(DebeziumException.class, () -> processor.configure(config));
    }

    @Test
    void testConfigureMissingRegion() {

        Map<String, Object> config = baseConfig();
        config.remove(S3LargeMessagePostProcessor.REGION_NAME_CONFIG);

        assertThrows(DebeziumException.class, () -> processor.configure(config));
    }

    @Test
    void testConfigureInvalidThreshold() {

        Map<String, Object> config = baseConfig();
        config.put(S3LargeMessagePostProcessor.THRESHOLD_BYTES_CONFIG, "0");

        assertThrows(DebeziumException.class, () -> processor.configure(config));
    }

    @Test
    void testDefaultThreshold() {

        processor.configure(Map.of(
                S3LargeMessagePostProcessor.BUCKET_NAME_CONFIG, BUCKET,
                S3LargeMessagePostProcessor.REGION_NAME_CONFIG, REGION));
        Schema schema = SchemaBuilder.struct()
                .field("payload", Schema.STRING_SCHEMA)
                .build();
        Struct after = new Struct(schema);
        after.put("payload", "small");

        Struct envelope = buildSimpleEnvelope(null, after);

        processor.apply(null, envelope);

        verify(mockS3Client, never()).putObject(any(PutObjectRequest.class), any(RequestBody.class));
    }

    @Test
    void testApplyNullValue() {
        processor.configure(baseConfig());

        processor.apply(null, null);

        verify(mockS3Client, never()).putObject(any(PutObjectRequest.class), any(RequestBody.class));
    }

    @Test
    void testApplyBytesFieldBelowThreshold() {
        processor.configure(baseConfig());

        Schema schema = SchemaBuilder.struct()
                .field("data", Schema.BYTES_SCHEMA)
                .build();
        Struct after = new Struct(schema);
        after.put("data", new byte[SMALL_THRESHOLD - 1]);

        Struct envelope = buildSimpleEnvelope(null, after);
        processor.apply(null, envelope);

        verify(mockS3Client, never()).putObject(any(PutObjectRequest.class), any(RequestBody.class));

        Struct resultAfter = envelope.getStruct(Envelope.FieldName.AFTER);

        assertNotNull(resultAfter);
        assertInstanceOf(byte[].class, resultAfter.get("data"));
    }

    @Test
    void testApplyStringFieldAboveThreshold() {

        processor.configure(baseConfig());
        String largeString = "x".repeat(SMALL_THRESHOLD + 1);
        Schema schema = SchemaBuilder.struct()
                .field("content", Schema.STRING_SCHEMA)
                .build();
        Struct after = new Struct(schema);
        after.put("content", largeString);
        Struct envelope = buildSimpleEnvelope(buildSourceStruct(), after);

        processor.apply(null, envelope);

        verify(mockS3Client, times(1)).putObject(any(PutObjectRequest.class), any(RequestBody.class));

        Struct resultAfter = envelope.getStruct(Envelope.FieldName.AFTER);
        Object replacedValue = resultAfter.get("content");
        assertInstanceOf(Struct.class, replacedValue);

        Struct ref = (Struct) replacedValue;
        assertEquals(BUCKET, ref.getString("bucket"));

        String objectId = ref.getString("objectId");
        assertNotNull(objectId);
        assertTrue(objectId.contains("content"));
        assertTrue(objectId.endsWith("/after"));
    }

    @Test
    void testBeforeFieldProcessedOnUpdateEvent() {

        processor.configure(baseConfig());
        String largeString = "y".repeat(SMALL_THRESHOLD + 5);
        Schema schema = SchemaBuilder.struct()
                .field("notes", Schema.STRING_SCHEMA)
                .build();
        Struct before = new Struct(schema);
        before.put("notes", largeString);
        Struct after = new Struct(schema);
        after.put("notes", largeString);
        Struct envelope = buildUpdateEnvelope(buildSourceStruct(), before, after);

        processor.apply(null, envelope);

        verify(mockS3Client, times(2)).putObject(any(PutObjectRequest.class), any(RequestBody.class));

        Struct resultBefore = envelope.getStruct(Envelope.FieldName.BEFORE);
        Struct resultAfter = envelope.getStruct(Envelope.FieldName.AFTER);

        assertInstanceOf(Struct.class, resultBefore.get("notes"));
        assertInstanceOf(Struct.class, resultAfter.get("notes"));

        String beforeObjectId = ((Struct) resultBefore.get("notes")).getString("objectId");
        String afterObjectId = ((Struct) resultAfter.get("notes")).getString("objectId");
        assertTrue(beforeObjectId.endsWith("/before"));
        assertTrue(afterObjectId.endsWith("/after"));
    }

    @Test
    void testReferenceStructContainsCorrectBucket() {

        processor.configure(baseConfig());

        String largeString = "Z".repeat(SMALL_THRESHOLD + 1);
        Schema schema = SchemaBuilder.struct()
                .field("payload", Schema.STRING_SCHEMA)
                .build();
        Struct after = new Struct(schema);
        after.put("payload", largeString);

        Struct envelope = buildSimpleEnvelope(buildSourceStruct(), after);

        processor.apply(null, envelope);

        Struct ref = (Struct) envelope.getStruct(Envelope.FieldName.AFTER).get("payload");
        assertEquals(BUCKET, ref.getString("bucket"), "Bucket in reference must match configured bucket");
    }

    @Test
    void testNullSourceDoesNotThrow() {

        processor.configure(baseConfig());

        String largeString = "X".repeat(SMALL_THRESHOLD + 1);
        Schema schema = SchemaBuilder.struct()
                .field("payload", Schema.STRING_SCHEMA)
                .build();
        Struct after = new Struct(schema);
        after.put("payload", largeString);

        Struct envelope = buildSimpleEnvelope(null, after);

        processor.apply(null, envelope);

        verify(mockS3Client, times(1)).putObject(any(PutObjectRequest.class), any(RequestBody.class));

        Struct ref = (Struct) envelope.getStruct(Envelope.FieldName.AFTER).get("payload");
        assertNotNull(ref.getString("objectId"));
    }

    private Map<String, Object> baseConfig() {
        HashMap<String, Object> map = new HashMap<>();
        map.put(S3LargeMessagePostProcessor.BUCKET_NAME_CONFIG, BUCKET);
        map.put(S3LargeMessagePostProcessor.REGION_NAME_CONFIG, REGION);
        map.put(S3LargeMessagePostProcessor.THRESHOLD_BYTES_CONFIG, Integer.toString(SMALL_THRESHOLD));
        return map;
    }

    private Struct buildSourceStruct() {
        Schema sourceSchema = SchemaBuilder.struct()
                .name("io.debezium.connector.mysql.Source")
                .field("db", Schema.STRING_SCHEMA)
                .field("table", Schema.STRING_SCHEMA)
                .field("file", Schema.STRING_SCHEMA)
                .field("pos", Schema.INT64_SCHEMA)
                .field("ts_ms", Schema.INT64_SCHEMA)
                .build();

        Struct source = new Struct(sourceSchema);
        source.put("db", "testdb");
        source.put("table", "orders");
        source.put("file", "binlog.000001");
        source.put("pos", 1234L);
        source.put("ts_ms", 1700000000000L);
        return source;
    }

    private Struct buildSimpleEnvelope(Struct source, Struct after) {
        Schema afterSchema = after.schema();
        SchemaBuilder envelopeBuilder = SchemaBuilder.struct()
                .name("TestEnvelope")
                .field(Envelope.FieldName.OPERATION, Schema.STRING_SCHEMA)
                .field(Envelope.FieldName.AFTER, afterSchema);

        if (source != null) {
            envelopeBuilder.field(Envelope.FieldName.SOURCE, source.schema());
        }

        Schema envelopeSchema = envelopeBuilder.build();
        Struct envelope = new Struct(envelopeSchema);
        envelope.put(Envelope.FieldName.OPERATION, "c");
        envelope.put(Envelope.FieldName.AFTER, after);
        if (source != null) {
            envelope.put(Envelope.FieldName.SOURCE, source);
        }
        return envelope;
    }

    private Struct buildUpdateEnvelope(Struct source, Struct before, Struct after) {
        Schema beforeSchema = before.schema();
        Schema afterSchema = after.schema();
        SchemaBuilder envelopeBuilder = SchemaBuilder.struct()
                .name("TestEnvelope")
                .field(Envelope.FieldName.OPERATION, Schema.STRING_SCHEMA)
                .field(Envelope.FieldName.BEFORE, beforeSchema)
                .field(Envelope.FieldName.AFTER, afterSchema);

        if (source != null) {
            envelopeBuilder.field(Envelope.FieldName.SOURCE, source.schema());
        }

        Schema envelopeSchema = envelopeBuilder.build();
        Struct envelope = new Struct(envelopeSchema);
        envelope.put(Envelope.FieldName.OPERATION, "u");
        envelope.put(Envelope.FieldName.BEFORE, before);
        envelope.put(Envelope.FieldName.AFTER, after);
        if (source != null) {
            envelope.put(Envelope.FieldName.SOURCE, source);
        }
        return envelope;
    }
}