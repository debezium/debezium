/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.config.Configuration;
import io.debezium.spi.storage.OversizedRecord;
import io.debezium.spi.storage.OversizedRecordReference;
import io.debezium.spi.storage.OversizedRecordStorage;

class EnforceRecordSizeClaimCheckTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final Schema SOURCE_SCHEMA = SchemaBuilder.struct()
            .field("db", Schema.STRING_SCHEMA)
            .build();
    private static final Schema KEY_SCHEMA = SchemaBuilder.struct()
            .field("id", Schema.INT32_SCHEMA)
            .build();

    private EnforceRecordSize<SourceRecord> transform;

    @BeforeEach
    void setUp() {
        RecordingStorage.reset();
        transform = new EnforceRecordSize<>();
    }

    @AfterEach
    void tearDown() {
        transform.close();
    }

    @Test
    void shouldStoreCompleteRecordBeforeReplacingConfiguredColumn() throws Exception {
        transform.configure(claimCheckConfig("inventory.customers.payload"));
        String payload = "complete-value-" + "x".repeat(5_000);
        SourceRecord sourceRecord = createStringRecord(payload, sourceOffset(100L));

        SourceRecord result = transform.apply(sourceRecord);

        assertThat(RecordingStorage.records).hasSize(1);
        OversizedRecord stored = RecordingStorage.records.get(0);
        JsonNode storedJson = OBJECT_MAPPER.readTree(stored.payload());
        assertThat(storedJson.path("version").asInt()).isEqualTo(1);
        assertThat(storedJson.path("topic").asText()).isEqualTo("inventory.customers");
        assertThat(storedJson.at("/sourcePartition/server").asText()).isEqualTo("inventory");
        assertThat(storedJson.at("/sourceOffset/lsn").asLong()).isEqualTo(100L);
        assertThat(storedJson.at("/key/id").asInt()).isEqualTo(1);
        assertThat(storedJson.at("/value/after/payload").asText()).isEqualTo(payload);
        assertThat(stored.contentType()).isEqualTo("application/json");

        JsonNode marker = marker(result, "payload");
        assertThat(marker.path("__debezium_claim_check").asBoolean()).isTrue();
        assertThat(marker.path("version").asInt()).isEqualTo(1);
        assertThat(marker.path("storage").asText()).isEqualTo("test");
        assertThat(marker.path("uri").asText()).isEqualTo("test://claim-check/" + stored.key());
        assertThat(marker.path("column").asText()).isEqualTo("payload");
        assertThat(marker.path("size_bytes").asLong()).isEqualTo(stored.payload().length);
        assertThat(marker.path("sha256").asText()).isEqualTo(ClaimCheckRecordSerializer.sha256Hex(stored.payload()));

        assertThat(after(sourceRecord).getString("payload")).isEqualTo(payload);
        assertThat(RecordingStorage.configuredBasePath).isEqualTo("test://claim-check");
    }

    @Test
    void shouldSerializeHeadersInOrder() throws Exception {
        Map<String, Object> config = claimCheckConfig("payload");
        config.put(EnforceRecordSize.MAX_BYTES_CONF, "2000");
        transform.configure(config);
        SourceRecord sourceRecord = createStringRecord("x".repeat(5_000), sourceOffset(100L));
        sourceRecord.headers().addString("trace-id", "first");
        sourceRecord.headers().addBytes("trace-id", new byte[]{ 1, 2, 3 });

        transform.apply(sourceRecord);

        JsonNode headers = OBJECT_MAPPER.readTree(RecordingStorage.records.get(0).payload()).path("headers");
        assertThat(headers.size()).isEqualTo(2);
        assertThat(headers.get(0).path("key").asText()).isEqualTo("trace-id");
        assertThat(headers.get(0).path("value").asText()).isEqualTo("first");
        assertThat(headers.get(1).path("key").asText()).isEqualTo("trace-id");
        assertThat(headers.get(1).path("value").asText()).isEqualTo("AQID");
    }

    @Test
    void shouldUseTheSameKeyForAnIdenticalRetry() {
        transform.configure(claimCheckConfig("payload"));
        SourceRecord sourceRecord = createStringRecord("x".repeat(5_000), sourceOffset(100L));

        transform.apply(sourceRecord);
        transform.apply(sourceRecord);

        assertThat(RecordingStorage.records).hasSize(2);
        assertThat(RecordingStorage.records.get(0).key()).isEqualTo(RecordingStorage.records.get(1).key());
        assertThat(RecordingStorage.records.get(0).payload()).isEqualTo(RecordingStorage.records.get(1).payload());
    }

    @Test
    void shouldUseDifferentKeysForDifferentSourceOffsets() {
        transform.configure(claimCheckConfig("payload"));

        transform.apply(createStringRecord("x".repeat(5_000), sourceOffset(100L)));
        transform.apply(createStringRecord("x".repeat(5_000), sourceOffset(101L)));

        assertThat(RecordingStorage.records).hasSize(2);
        assertThat(RecordingStorage.records.get(0).key()).isNotEqualTo(RecordingStorage.records.get(1).key());
    }

    @Test
    void shouldCanonicalizeSourcePositionWhenBuildingTheKey() {
        transform.configure(claimCheckConfig("payload"));
        Map<String, Object> firstOffset = new LinkedHashMap<>();
        firstOffset.put("lsn", 100L);
        firstOffset.put("tx", 7L);
        Map<String, Object> secondOffset = new LinkedHashMap<>();
        secondOffset.put("tx", 7L);
        secondOffset.put("lsn", 100L);

        transform.apply(createStringRecord("x".repeat(5_000), firstOffset));
        transform.apply(createStringRecord("x".repeat(5_000), secondOffset));

        assertThat(RecordingStorage.records.get(0).key()).isEqualTo(RecordingStorage.records.get(1).key());
    }

    @Test
    void shouldFailClosedWithoutMutatingTheOriginalRecord() {
        transform.configure(claimCheckConfig("payload"));
        RecordingStorage.failWrites = true;
        String payload = "x".repeat(5_000);
        SourceRecord sourceRecord = createStringRecord(payload, sourceOffset(100L));

        assertThatThrownBy(() -> transform.apply(sourceRecord))
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("Failed to store oversized record");

        assertThat(after(sourceRecord).getString("payload")).isEqualTo(payload);
    }

    @Test
    void shouldValidateConfiguredColumnsBeforeWriting() {
        transform.configure(claimCheckConfig("missing_column"));
        SourceRecord sourceRecord = createStringRecord("x".repeat(5_000), sourceOffset(100L));

        assertThatThrownBy(() -> transform.apply(sourceRecord))
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("missing_column")
                .hasMessageContaining("not found");
        assertThat(RecordingStorage.records).isEmpty();
    }

    @Test
    void shouldRejectUnsupportedColumnTypesBeforeWriting() {
        transform.configure(claimCheckConfig("payload"));
        Schema recordSchema = SchemaBuilder.struct()
                .field("payload", Schema.OPTIONAL_INT32_SCHEMA)
                .field("large", Schema.OPTIONAL_STRING_SCHEMA)
                .optional()
                .build();
        Struct after = new Struct(recordSchema)
                .put("payload", 42)
                .put("large", "x".repeat(5_000));
        SourceRecord sourceRecord = createRecord(recordSchema, after, sourceOffset(100L));

        assertThatThrownBy(() -> transform.apply(sourceRecord))
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("payload")
                .hasMessageContaining("only STRING and unnamed BYTES");
        assertThat(RecordingStorage.records).isEmpty();
    }

    @Test
    void shouldRejectNamedBytesColumnsBeforeWriting() {
        transform.configure(claimCheckConfig("amount"));
        Schema recordSchema = SchemaBuilder.struct()
                .field("amount", Decimal.builder(2).optional().build())
                .field("large", Schema.OPTIONAL_STRING_SCHEMA)
                .optional()
                .build();
        Struct after = new Struct(recordSchema)
                .put("amount", new BigDecimal("42.00"))
                .put("large", "x".repeat(5_000));
        SourceRecord sourceRecord = createRecord(recordSchema, after, sourceOffset(100L));

        assertThatThrownBy(() -> transform.apply(sourceRecord))
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("amount")
                .hasMessageContaining("unnamed BYTES");
        assertThat(RecordingStorage.records).isEmpty();
    }

    @Test
    void shouldFailClosedWhenStorageReturnsNoReference() {
        transform.configure(claimCheckConfig("payload"));
        RecordingStorage.returnNullReference = true;
        String payload = "x".repeat(5_000);
        SourceRecord sourceRecord = createStringRecord(payload, sourceOffset(100L));

        assertThatThrownBy(() -> transform.apply(sourceRecord))
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("returned no reference");
        assertThat(after(sourceRecord).getString("payload")).isEqualTo(payload);
    }

    @Test
    void shouldNotWriteRecordsWithinTheConfiguredLimit() {
        Map<String, Object> config = claimCheckConfig("payload");
        config.put(EnforceRecordSize.MAX_BYTES_CONF, "10000");
        transform.configure(config);
        SourceRecord sourceRecord = createStringRecord("small", sourceOffset(100L));

        SourceRecord result = transform.apply(sourceRecord);

        assertThat(result).isSameAs(sourceRecord);
        assertThat(RecordingStorage.records).isEmpty();
    }

    @Test
    void shouldEmitABytesMarkerForBytesColumns() throws Exception {
        transform.configure(claimCheckConfig("payload"));
        SourceRecord sourceRecord = createBytesRecord(new byte[5_000], sourceOffset(100L));

        SourceRecord result = transform.apply(sourceRecord);

        byte[] markerBytes = after(result).getBytes("payload");
        JsonNode marker = OBJECT_MAPPER.readTree(new String(markerBytes, StandardCharsets.UTF_8));
        assertThat(marker.path("__debezium_claim_check").asBoolean()).isTrue();
        assertThat(OBJECT_MAPPER.readTree(RecordingStorage.records.get(0).payload())
                .at("/value/after/payload").asText()).isNotBlank();
    }

    @Test
    void shouldCloseConfiguredStorage() {
        transform.configure(claimCheckConfig("payload"));

        transform.close();

        assertThat(RecordingStorage.closed).isTrue();
    }

    private static Map<String, Object> claimCheckConfig(String columns) {
        Map<String, Object> config = new LinkedHashMap<>();
        config.put(EnforceRecordSize.MAX_BYTES_CONF, "1200");
        config.put(EnforceRecordSize.STRATEGY_CONF, "claim_check");
        config.put(EnforceRecordSize.CLAIM_CHECK_STORAGE_CLASS_CONF, RecordingStorage.class.getName());
        config.put(EnforceRecordSize.CLAIM_CHECK_COLUMNS_CONF, columns);
        config.put(EnforceRecordSize.CLAIM_CHECK_STORAGE_CONFIG_PREFIX + "base.path", "test://claim-check");
        return config;
    }

    private static SourceRecord createStringRecord(String value, Map<String, ?> offset) {
        Schema recordSchema = SchemaBuilder.struct()
                .field("payload", Schema.OPTIONAL_STRING_SCHEMA)
                .field("id", Schema.INT32_SCHEMA)
                .optional()
                .build();
        Struct after = new Struct(recordSchema)
                .put("payload", value)
                .put("id", 1);
        return createRecord(recordSchema, after, offset);
    }

    private static SourceRecord createBytesRecord(byte[] value, Map<String, ?> offset) {
        Schema recordSchema = SchemaBuilder.struct()
                .field("payload", Schema.OPTIONAL_BYTES_SCHEMA)
                .field("id", Schema.INT32_SCHEMA)
                .optional()
                .build();
        Struct after = new Struct(recordSchema)
                .put("payload", ByteBuffer.wrap(value))
                .put("id", 1);
        return createRecord(recordSchema, after, offset);
    }

    private static SourceRecord createRecord(Schema recordSchema, Struct after, Map<String, ?> offset) {
        Schema envelopeSchema = SchemaBuilder.struct()
                .field("before", recordSchema)
                .field("after", recordSchema)
                .field("op", Schema.STRING_SCHEMA)
                .field("source", SOURCE_SCHEMA)
                .build();
        Struct envelope = new Struct(envelopeSchema)
                .put("after", after)
                .put("op", "c")
                .put("source", new Struct(SOURCE_SCHEMA).put("db", "inventory"));
        return new SourceRecord(
                Map.of("server", "inventory"),
                offset,
                "inventory.customers",
                0,
                KEY_SCHEMA,
                new Struct(KEY_SCHEMA).put("id", 1),
                envelopeSchema,
                envelope);
    }

    private static Map<String, Object> sourceOffset(long lsn) {
        return Map.of("lsn", lsn);
    }

    private static Struct after(SourceRecord record) {
        return ((Struct) record.value()).getStruct("after");
    }

    private static JsonNode marker(SourceRecord record, String field) throws Exception {
        return OBJECT_MAPPER.readTree(after(record).getString(field));
    }

    public static class RecordingStorage implements OversizedRecordStorage {

        private static final List<OversizedRecord> records = new ArrayList<>();
        private static boolean failWrites;
        private static boolean returnNullReference;
        private static boolean closed;
        private static String configuredBasePath;

        static void reset() {
            records.clear();
            failWrites = false;
            returnNullReference = false;
            closed = false;
            configuredBasePath = null;
        }

        @Override
        public void configure(Configuration config) {
            configuredBasePath = config.getString("base.path");
        }

        @Override
        public OversizedRecordReference write(OversizedRecord record) {
            if (failWrites) {
                throw new IllegalStateException("storage unavailable");
            }
            records.add(record);
            if (returnNullReference) {
                return null;
            }
            return new OversizedRecordReference(
                    "test",
                    URI.create("test://claim-check/" + record.key()),
                    record.payload().length);
        }

        @Override
        public void close() {
            closed = true;
        }
    }
}
