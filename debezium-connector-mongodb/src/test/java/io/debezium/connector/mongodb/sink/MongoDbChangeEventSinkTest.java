/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.sink;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.bson.BsonDocument;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.WriteModel;

import io.debezium.config.Configuration;
import io.debezium.connector.mongodb.connection.client.MongoDbClient;
import io.debezium.data.Envelope;
import io.debezium.dlq.ErrorReporter;
import io.debezium.openlineage.ConnectorContext;
import io.debezium.sink.DebeziumSinkRecord;
import io.debezium.sink.SinkConnectorConfig;

/**
 * Unit tests for {@link MongoDbChangeEventSink} error-reporter wiring and DLQ behaviour.
 */
@ExtendWith(MockitoExtension.class)
class MongoDbChangeEventSinkTest {

    private static final String TOPIC = "server1.inventory.customers";

    // MongoDB driver interfaces — must be mocked (no embedded server available).
    @Mock
    private MongoClient mongoClient;
    @Mock
    private MongoDbClient mongoDbClient;
    @Mock
    private MongoDatabase mongoDatabase;
    @Mock
    @SuppressWarnings("unchecked")
    private MongoCollection<BsonDocument> mongoCollection;

    // Real capturing reporter — no mock needed.
    private final List<DebeziumSinkRecord> reportedRecords = new ArrayList<>();
    private final List<Exception> reportedErrors = new ArrayList<>();
    private ErrorReporter capturingReporter;

    // Real ConnectorContext with OpenLineage disabled so no HTTP calls are made.
    private ConnectorContext connectorContext;

    @BeforeEach
    void setUp() {
        when(mongoDbClient.getClient()).thenReturn(mongoClient);
        when(mongoClient.getDatabase(anyString())).thenReturn(mongoDatabase);
        when(mongoDatabase.getCollection(anyString(), eq(BsonDocument.class))).thenReturn(mongoCollection);

        capturingReporter = (record, error) -> {
            reportedRecords.add(record);
            reportedErrors.add(error);
        };

        connectorContext = new ConnectorContext(
                "test-connector", "MongoDbSinkConnector", "0", "test",
                UUID.randomUUID(), Map.of("openlineage.integration.enabled", "false"));
    }

    // ─── helpers ─────────────────────────────────────────────────────────────────

    /**
     * Builds a real {@link MongoDbSinkConnectorConfig}.
     * Sets {@code batch.size=1} by default so a single record is flushed immediately in the SCES
     * path; the default batch size of 500 would hold records in the buffer indefinitely.
     * Additional key-value pairs override any earlier setting.
     */
    private MongoDbSinkConnectorConfig createConfig(String... keyValues) {
        var builder = Configuration.create()
                .with(MongoDbSinkConnectorConfig.CONNECTION_STRING, "mongodb://localhost:27017")
                .with(MongoDbSinkConnectorConfig.SINK_DATABASE, "testdb")
                .with(SinkConnectorConfig.BATCH_SIZE_FIELD, "1");
        for (int i = 0; i < keyValues.length; i += 2) {
            builder = builder.with(keyValues[i], keyValues[i + 1]);
        }
        return new MongoDbSinkConnectorConfig(builder.build());
    }

    private MongoDbChangeEventSink createSink(MongoDbSinkConnectorConfig config) {
        return new MongoDbChangeEventSink(config, mongoDbClient, capturingReporter, connectorContext);
    }

    /**
     * Returns a real {@link BulkWriteResult} for the success path.
     * The production code only logs the result via {@code toString()}, so the
     * unacknowledged variant is sufficient and avoids driver internal-API ambiguity.
     */
    private static BulkWriteResult successResult() {
        return BulkWriteResult.unacknowledged();
    }

    /** Produces a valid Debezium CREATE-envelope {@link SinkRecord} that converts cleanly to BSON. */
    private static SinkRecord createEnvelopeRecord(String topic, int id, String name) {
        Schema sourceSchema = SchemaBuilder.struct()
                .field("ts_ms", Schema.INT64_SCHEMA)
                .build();
        Schema recordSchema = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                .build();
        Schema envelopeSchema = SchemaBuilder.struct()
                .name(topic + ".Envelope")
                .field(Envelope.FieldName.BEFORE, recordSchema)
                .field(Envelope.FieldName.AFTER, recordSchema)
                .field(Envelope.FieldName.SOURCE, sourceSchema)
                .field(Envelope.FieldName.OPERATION, Schema.STRING_SCHEMA)
                .field(Envelope.FieldName.TIMESTAMP, Schema.OPTIONAL_INT64_SCHEMA)
                .build();

        Struct source = new Struct(sourceSchema).put("ts_ms", Instant.now().toEpochMilli());
        Struct after = new Struct(recordSchema).put("id", id).put("name", name);
        Struct envelope = new Struct(envelopeSchema)
                .put(Envelope.FieldName.AFTER, after)
                .put(Envelope.FieldName.SOURCE, source)
                .put(Envelope.FieldName.OPERATION, Envelope.Operation.CREATE.code())
                .put(Envelope.FieldName.TIMESTAMP, Instant.now().toEpochMilli());

        Schema keySchema = SchemaBuilder.struct().field("id", Schema.INT32_SCHEMA).build();
        Struct key = new Struct(keySchema).put("id", id);

        return new SinkRecord(topic, 0, keySchema, key, envelopeSchema, envelope, 0);
    }

    /**
     * Produces a minimal CREATE-envelope {@link SinkRecord} with only {@code op}/{@code after} —
     * no {@code before}/{@code source}/{@code ts_ms} — matching the shape produced by a bare,
     * hand-written JSON envelope (as used by {@code SinkConnectorIT}'s synthetic-record scenarios),
     * as opposed to {@link #createEnvelopeRecord} which mirrors a full Debezium source envelope.
     */
    private static SinkRecord createMinimalEnvelopeRecord(String topic, int id, String name, String secret) {
        Schema afterSchema = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                .field("secret", Schema.OPTIONAL_STRING_SCHEMA)
                .build();
        Schema envelopeSchema = SchemaBuilder.struct()
                .field("op", Schema.STRING_SCHEMA)
                .field("after", afterSchema)
                .build();
        Struct after = new Struct(afterSchema).put("id", id).put("name", name).put("secret", secret);
        Struct envelope = new Struct(envelopeSchema).put("op", "c").put("after", after);

        Schema keySchema = SchemaBuilder.struct().field("id", Schema.INT32_SCHEMA).build();
        Struct key = new Struct(keySchema).put("id", id);

        return new SinkRecord(topic, 0, keySchema, key, envelopeSchema, envelope, 0);
    }

    // ─── Parameterised: same expected outcome in both legacy and SCES mode ───────

    @Test
    void successfulWriteShouldNotReportErrors() {
        when(mongoCollection.bulkWrite(anyList(), any(BulkWriteOptions.class)))
                .thenReturn(successResult());

        createSink(createConfig()).execute(List.of(createEnvelopeRecord(TOPIC, 1, "John")));

        assertThat(reportedRecords).isEmpty();
        verify(mongoCollection, times(1)).bulkWrite(anyList(), any(BulkWriteOptions.class));
    }

    @Test
    void nonBulkWriteExceptionShouldBeRoutedToErrorReporter() {
        when(mongoCollection.bulkWrite(anyList(), any(BulkWriteOptions.class)))
                .thenThrow(new RuntimeException("network error"));

        createSink(createConfig()).execute(List.of(createEnvelopeRecord(TOPIC, 1, "John")));

        assertThat(reportedRecords).hasSize(1);
        assertThat(reportedErrors.get(0))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("network error");
    }

    // ─── SCES-only: MongoBulkWriteException is routed to the DLQ via framework isolation ──

    @Test
    void mongoBulkWriteExceptionShouldBeRoutedToErrorReporterViaFrameworkIsolation() {
        // MongoBulkWriteException causes handleTolerableWriteException() to throw DataException,
        // which escapes doWriteBatch(). AbstractChangeEventSink.writeBatch() intercepts it and,
        // because the real errorReporter is now wired via super(sinkConfig, errorReporter),
        // calls reportFailedRecords(). For a single-record batch, reportRecord() is invoked
        // directly (no individual retry) — so bulkWrite is called exactly once.
        var bulkWriteEx = mock(MongoBulkWriteException.class);
        when(mongoCollection.bulkWrite(anyList(), any(BulkWriteOptions.class))).thenThrow(bulkWriteEx);

        createSink(createConfig()).execute(List.of(createEnvelopeRecord(TOPIC, 1, "John")));

        assertThat(reportedRecords).hasSize(1);
        assertThat(reportedErrors.get(0)).isInstanceOf(MongoBulkWriteException.class);
        // Single-record batch: framework skips the retry loop and calls reportRecord() directly.
        verify(mongoCollection, times(1)).bulkWrite(anyList(), any(BulkWriteOptions.class));
    }

    @Test
    void mongoBulkWriteExceptionWithMultipleRecordsShouldIsolateFailuresViaRetry() {
        // For a batch of N records, the framework retries each record individually to find the
        // bad ones (1 initial batch call + N individual retry calls = N+1 total).
        var bulkWriteEx = mock(MongoBulkWriteException.class);
        when(mongoCollection.bulkWrite(anyList(), any(BulkWriteOptions.class))).thenThrow(bulkWriteEx);

        createSink(createConfig(
                SinkConnectorConfig.BATCH_SIZE_FIELD.name(), "2"))
                .execute(List.of(
                        createEnvelopeRecord(TOPIC, 1, "John"),
                        createEnvelopeRecord(TOPIC, 2, "Jane")));

        // Both records are reported after their individual retries fail.
        assertThat(reportedRecords).hasSize(2);
        // 1 initial batch + 2 individual retries = 3 total bulkWrite calls.
        verify(mongoCollection, times(3)).bulkWrite(anyList(), any(BulkWriteOptions.class));
    }

    @Test
    void onlyBadRecordShouldBeReportedWhenOthersSucceed() {
        // Initial batch of 2 fails; individual retries: record-1 succeeds, record-2 fails.
        var bulkWriteEx = mock(MongoBulkWriteException.class);
        when(mongoCollection.bulkWrite(anyList(), any(BulkWriteOptions.class)))
                .thenThrow(bulkWriteEx) // initial batch → triggers individual retry
                .thenReturn(successResult()) // record-1 retry → success
                .thenThrow(bulkWriteEx); // record-2 retry → reported to DLQ

        createSink(createConfig(
                SinkConnectorConfig.BATCH_SIZE_FIELD.name(), "2"))
                .execute(List.of(
                        createEnvelopeRecord(TOPIC, 1, "John"),
                        createEnvelopeRecord(TOPIC, 2, "Jane")));

        assertThat(reportedRecords).hasSize(1);
        verify(mongoCollection, times(3)).bulkWrite(anyList(), any(BulkWriteOptions.class));
    }

    // ─── SCES-only: field filtering through the full execute()->buffer->doWriteBatch() pipeline ──

    @Test
    @SuppressWarnings("unchecked")
    void fieldExcludeListShouldFilterFieldThroughFullPipeline() {
        // Unlike FilteringSinkDocumentResolverTest/FieldFilterTest (which construct
        // MongoDbSinkRecord directly), this drives field filtering through the actual
        // execute() -> put() -> AbstractChangeEventSink buffer -> writeBatch() -> doWriteBatch()
        // chain, with a minimal (op/after-only) envelope matching a hand-written JSON record.
        when(mongoCollection.bulkWrite(anyList(), any(BulkWriteOptions.class))).thenReturn(successResult());

        var config = createConfig(
                "field.exclude.list", "secret");

        try (var sink = createSink(config)) {
            sink.execute(List.of(createMinimalEnvelopeRecord(TOPIC, 1, "Alice", "topsecret")));
        }

        ArgumentCaptor<List<WriteModel<BsonDocument>>> captor = ArgumentCaptor.forClass(List.class);
        verify(mongoCollection).bulkWrite(captor.capture(), any(BulkWriteOptions.class));
        BsonDocument replacement = ((ReplaceOneModel<BsonDocument>) captor.getValue().get(0)).getReplacement();

        assertThat(replacement.containsKey("name")).isTrue();
        assertThat(replacement.containsKey("secret")).isFalse();
    }

    // ─── Retriable vs non-retriable exception classification ──

    @Test
    void transientMongoExceptionShouldPropagateNotRoute() {
        // A MongoSocketException (network error) is transient and should be propagated
        // to Kafka Connect for retry, not routed to the error reporter.
        when(mongoCollection.bulkWrite(anyList(), any(BulkWriteOptions.class)))
                .thenThrow(new com.mongodb.MongoSocketException("connection reset", new com.mongodb.ServerAddress()));

        var sink = createSink(createConfig());
        org.assertj.core.api.Assertions.assertThatThrownBy(
                () -> sink.execute(List.of(createEnvelopeRecord(TOPIC, 1, "John"))))
                .isInstanceOf(com.mongodb.MongoSocketException.class);
        assertThat(reportedRecords).isEmpty();
    }

    @Test
    void mongoBulkWriteExceptionShouldRouteNotPropagate() {
        // MongoBulkWriteException (permanent per-document errors) should be routed to
        // the error reporter, not propagated — so the task stays alive.
        var bulkWriteEx = mock(MongoBulkWriteException.class);
        when(mongoCollection.bulkWrite(anyList(), any(BulkWriteOptions.class))).thenThrow(bulkWriteEx);

        createSink(createConfig()).execute(List.of(createEnvelopeRecord(TOPIC, 1, "John")));

        assertThat(reportedRecords).hasSize(1);
    }

    // ─── Truncate with truncate.enabled=true ──

    @Test
    void truncateEnabledShouldCallDeleteMany() {
        when(mongoCollection.bulkWrite(anyList(), any(BulkWriteOptions.class))).thenReturn(successResult());
        when(mongoCollection.deleteMany(any(BsonDocument.class)))
                .thenReturn(com.mongodb.client.result.DeleteResult.acknowledged(5));

        var config = createConfig("truncate.enabled", "true");

        try (var sink = createSink(config)) {
            sink.execute(List.of(createEnvelopeRecord(TOPIC, 1, "John")));
            sink.execute(List.of(createTruncateRecord(TOPIC)));
        }

        verify(mongoCollection, times(1)).bulkWrite(anyList(), any(BulkWriteOptions.class));
        verify(mongoCollection, times(1)).deleteMany(any(BsonDocument.class));
    }

    private static SinkRecord createTruncateRecord(String topic) {
        Schema sourceSchema = SchemaBuilder.struct().field("ts_ms", Schema.INT64_SCHEMA).build();
        Schema envelopeSchema = SchemaBuilder.struct()
                .name(topic + ".Envelope")
                .field(Envelope.FieldName.SOURCE, sourceSchema)
                .field(Envelope.FieldName.OPERATION, Schema.STRING_SCHEMA)
                .field(Envelope.FieldName.TIMESTAMP, Schema.OPTIONAL_INT64_SCHEMA)
                .build();

        Struct source = new Struct(sourceSchema).put("ts_ms", java.time.Instant.now().toEpochMilli());
        Struct envelope = new Struct(envelopeSchema)
                .put(Envelope.FieldName.SOURCE, source)
                .put(Envelope.FieldName.OPERATION, Envelope.Operation.TRUNCATE.code())
                .put(Envelope.FieldName.TIMESTAMP, java.time.Instant.now().toEpochMilli());

        return new SinkRecord(topic, 0, null, null, envelopeSchema, envelope, 0);
    }

}
