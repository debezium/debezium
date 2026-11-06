/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import io.debezium.DebeziumException;
import io.debezium.data.Envelope;
import io.debezium.data.VerifyRecord;
import io.debezium.transforms.tracing.ActivateTracingSpan;
import io.opentelemetry.javaagent.testing.common.AgentTestingExporterAccess;
import io.opentelemetry.sdk.trace.data.SpanData;

import scala.collection.mutable.StringBuilder;

public class ActivateTracingSpanTest {
    private final ActivateTracingSpan<SourceRecord> transform = new ActivateTracingSpan<>();
    private final String tracingSpanContextFieldName = "tracingspancontext";
    protected final Schema sourceSchema = SchemaBuilder.struct().optional()
            .field("table", Schema.STRING_SCHEMA)
            .field("lsn", Schema.INT32_SCHEMA)
            .field("ts_ms", Schema.OPTIONAL_INT64_SCHEMA)
            .build();
    protected final SchemaBuilder recordSchemaBuilder = SchemaBuilder.struct().optional()
            .field("id", Schema.INT8_SCHEMA)
            .field("name", Schema.STRING_SCHEMA);
    protected final Schema recordSchema = recordSchemaBuilder.build();
    protected final Schema recordSchemaWithTracingSpanContextField = recordSchemaBuilder.field(tracingSpanContextFieldName, Schema.STRING_SCHEMA).build();

    @Test
    public void whenPropagationContextIsProvidedATraceIsCreated() {
        AgentTestingExporterAccess.reset();
        // valid traceparent header from https://www.w3.org/TR/trace-context/#examples-of-http-traceparent-headers
        final String propagatedTraceParent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"; // version-traceid-parentid-flags
        final String propagatedParentId = "00f067aa0ba902b7"; // second to last part of propagatedTraceParent
        final String propagatedTraceId = "4bf92f3577b34da6a3ce929d0e0e4736"; // second part of propagatedTraceParent

        final Map<String, String> props = new HashMap<>();
        props.put(ActivateTracingSpan.TRACING_SPAN_CONTEXT_FIELD.toString(), tracingSpanContextFieldName);
        transform.configure(props);

        final Schema myRecordSchema = SchemaBuilder.struct().optional()
                .field("id", Schema.INT8_SCHEMA)
                .field("name", Schema.STRING_SCHEMA)
                .field(tracingSpanContextFieldName, Schema.STRING_SCHEMA)
                .build();
        final Struct after = new Struct(myRecordSchema);
        final Struct source = new Struct(sourceSchema);
        source.put("table", "mytable");
        source.put("lsn", 1);
        source.put("ts_ms", 1588252618953L);

        after.put("id", (byte) 1);
        after.put("name", "test");
        // build the "tracingcontext" field which is expected to be a serialized representation
        // of a java.util.Properties object (e.g. "prop1=val1\nprop2=val2\n")
        final Map<String, String> tracingContext = new HashMap<>();
        // example valid traceparent value taken from https://www.w3.org/TR/trace-context/#relationship-between-the-headers
        tracingContext.put("traceparent", propagatedTraceParent);
        final String propagatedBaggage = "testprop=testvalue";
        // example baggage value taken from https://www.w3.org/TR/baggage/#header-name
        tracingContext.put("baggage", propagatedBaggage);
        final StringBuilder tracingContextSb = new StringBuilder();
        for (var prop : tracingContext.entrySet()) {
            tracingContextSb.append(prop.getKey())
                    .append("=")
                    .append(prop.getValue())
                    .append("\n");
        }
        after.put(tracingSpanContextFieldName, tracingContextSb.toString());

        final Envelope envelope = Envelope.defineSchema()
                .withName("dummy.Envelope")
                .withRecord(myRecordSchema)
                .withSource(sourceSchema)
                .build();

        final Struct payload = envelope.create(after, source, Instant.now());

        SourceRecord record = new SourceRecord(
                new HashMap<>(),
                new HashMap<>(),
                "db.server1.table1",
                envelope.schema(),
                payload);

        VerifyRecord.isValid(record, true);
        final SourceRecord transformedRecord = transform.apply(record);
        VerifyRecord.isValid(transformedRecord, true);

        final List<SpanData> spans = AgentTestingExporterAccess.getExportedSpans();
        assertThat(spans.size()).isEqualTo(2);
        assertThat(spans.get(0).getName()).isEqualTo("debezium-read");
        assertThat(spans.get(1).getName()).isEqualTo("db-log-write");
        // The "db-log-write" span should have the same parentId as the propagated trace
        assertThat(spans.get(1).getParentSpanId()).isEqualTo(propagatedParentId);
        // Spans should share the same trace id as the propagated trace
        assertThat(spans.get(0).getTraceId()).isEqualTo(propagatedTraceId);
        assertThat(spans.get(1).getTraceId()).isEqualTo(propagatedTraceId);

        final Headers headers = transformedRecord.headers();
        assertThat(headers).isNotEmpty();
        // the produced Kafka record should have a traceparent header holding the propagated trace Id
        assertThat(headers.lastWithName("traceparent").value().toString()).contains(propagatedTraceId);
        // the produced Kafka record should have a traceparent header holding the "db-log-write" span Id as a parent
        assertThat(headers.lastWithName("traceparent").value().toString()).contains(spans.get(1).getSpanId());
        // the produced Kafka record should have a baggage header holding the propagated baggage value
        assertThat(headers.lastWithName("baggage").value().toString()).isEqualTo(propagatedBaggage);
    }

    @Test
    public void whenNanosecondTimestampsAreAvailableSpansStartWithNanosecondPrecision() {
        final Schema schema = SchemaBuilder.struct().optional()
                .field("table", Schema.STRING_SCHEMA)
                .field("ts_ms", Schema.OPTIONAL_INT64_SCHEMA)
                .field("ts_us", Schema.OPTIONAL_INT64_SCHEMA)
                .field("ts_ns", Schema.OPTIONAL_INT64_SCHEMA)
                .build();
        final Struct source = new Struct(schema);
        source.put("table", "mytable");
        source.put("ts_ms", 1588252618953L);
        source.put("ts_us", 1588252618953123L);
        source.put("ts_ns", 1588252618953123456L);

        final Instant processingTime = Instant.ofEpochSecond(1588252619L, 123456789L);
        final List<SpanData> spans = traceRecordWithSource(source, processingTime);

        assertThat(spans.get(0).getStartEpochNanos()).isEqualTo(1588252619123456789L);
        assertThat(spans.get(1).getStartEpochNanos()).isEqualTo(1588252618953123456L);
    }

    @Test
    public void whenMicrosecondTimestampIsTheMostPreciseAvailableSpanStartsWithMicrosecondPrecision() {
        final Schema schema = SchemaBuilder.struct().optional()
                .field("table", Schema.STRING_SCHEMA)
                .field("ts_ms", Schema.OPTIONAL_INT64_SCHEMA)
                .field("ts_us", Schema.OPTIONAL_INT64_SCHEMA)
                .field("ts_ns", Schema.OPTIONAL_INT64_SCHEMA)
                .build();
        final Struct source = new Struct(schema);
        source.put("table", "mytable");
        source.put("ts_ms", 1588252618953L);
        source.put("ts_us", 1588252618953123L);

        final List<SpanData> spans = traceRecordWithSource(source, Instant.now());

        assertThat(spans.get(1).getStartEpochNanos()).isEqualTo(1588252618953123000L);
    }

    @Test
    public void whenOnlyMillisecondTimestampIsAvailableSpanStartsWithMillisecondPrecision() {
        final Struct source = new Struct(sourceSchema);
        source.put("table", "mytable");
        source.put("lsn", 1);
        source.put("ts_ms", 1588252618953L);

        final List<SpanData> spans = traceRecordWithSource(source, Instant.now());

        assertThat(spans.get(1).getStartEpochNanos()).isEqualTo(1588252618953000000L);
    }

    private List<SpanData> traceRecordWithSource(Struct source, Instant processingTime) {
        AgentTestingExporterAccess.reset();
        transform.configure(new HashMap<>());

        final Struct after = new Struct(recordSchema);
        after.put("id", (byte) 1);
        after.put("name", "test");

        final Envelope envelope = Envelope.defineSchema()
                .withName("dummy.Envelope")
                .withRecord(recordSchema)
                .withSource(source.schema())
                .build();

        final SourceRecord record = new SourceRecord(
                new HashMap<>(),
                new HashMap<>(),
                "db.server1.table1",
                envelope.schema(),
                envelope.create(after, source, processingTime));

        transform.apply(record);

        final List<SpanData> spans = AgentTestingExporterAccess.getExportedSpans();
        assertThat(spans).hasSize(2);
        assertThat(spans.get(0).getName()).isEqualTo("debezium-read");
        assertThat(spans.get(1).getName()).isEqualTo("db-log-write");
        return spans;
    }

    @Test
    public void whenPropagationContextIsNotProvidedAndContextIsNotRequiredATraceIsCreated() {
        AgentTestingExporterAccess.reset();
        final Map<String, String> props = new HashMap<>();
        transform.configure(props);

        final Struct after = new Struct(recordSchema);
        final Struct source = new Struct(sourceSchema);
        source.put("table", "mytable");
        source.put("lsn", 1);
        source.put("ts_ms", 1588252618953L);

        after.put("id", (byte) 1);
        after.put("name", "test");

        final Envelope envelope = Envelope.defineSchema()
                .withName("dummy.Envelope")
                .withRecord(recordSchema)
                .withSource(sourceSchema)
                .build();

        final Struct payload = envelope.create(after, source, Instant.now());

        SourceRecord record = new SourceRecord(
                new HashMap<>(),
                new HashMap<>(),
                "db.server1.table1",
                envelope.schema(),
                payload);

        VerifyRecord.isValid(record, true);
        final SourceRecord transformedRecord = transform.apply(record);
        VerifyRecord.isValid(transformedRecord, true);

        final List<SpanData> spans = AgentTestingExporterAccess.getExportedSpans();
        assertThat(spans.size()).isEqualTo(2);
        assertThat(spans.get(0).getName()).isEqualTo("debezium-read");
        assertThat(spans.get(1).getName()).isEqualTo("db-log-write");

        final Headers headers = transformedRecord.headers();
        assertThat(headers).isNotEmpty();
        // the produced Kafka record should have a traceparent header holding the newly created trace Id
        assertThat(headers.lastWithName("traceparent").value().toString()).contains(spans.get(1).getTraceId());
        // the produced Kafka record should have a traceparent header holding the "db-log-write" span Id as a parent
        assertThat(headers.lastWithName("traceparent").value().toString()).contains(spans.get(1).getSpanId());
    }

    @Test
    public void whenPropagationContextIsNotProvidedAndContextIsRequiredNoTraceIsCreated() {
        AgentTestingExporterAccess.reset();
        final Map<String, String> props = new HashMap<>();
        props.put(ActivateTracingSpan.TRACING_CONTEXT_FIELD_REQUIRED.toString(), "true");
        transform.configure(props);

        final Struct after = new Struct(recordSchema);
        final Struct source = new Struct(sourceSchema);
        source.put("table", "mytable");
        source.put("lsn", 1);
        source.put("ts_ms", 1588252618953L);

        after.put("id", (byte) 1);
        after.put("name", "test");

        final Envelope envelope = Envelope.defineSchema()
                .withName("dummy.Envelope")
                .withRecord(recordSchema)
                .withSource(sourceSchema)
                .build();

        final Struct payload = envelope.create(after, source, Instant.now());

        SourceRecord record = new SourceRecord(
                new HashMap<>(),
                new HashMap<>(),
                "db.server1.table1",
                envelope.schema(),
                payload);

        VerifyRecord.isValid(record, true);
        final SourceRecord transformedRecord = transform.apply(record);
        VerifyRecord.isValid(transformedRecord, true);

        final List<SpanData> spans = AgentTestingExporterAccess.getExportedSpans();
        assertThat(spans.size()).isEqualTo(0);

        final Headers headers = transformedRecord.headers();
        assertThat(headers).isEmpty();
    }

    @Test
    public void whenJsonPropagationContextIsProvidedATraceIsCreated() {
        AgentTestingExporterAccess.reset();
        final String propagatedTraceParent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
        final String propagatedParentId = "00f067aa0ba902b7";
        final String propagatedTraceId = "4bf92f3577b34da6a3ce929d0e0e4736";
        final String propagatedBaggage = "testprop=testvalue";

        final Map<String, String> props = new HashMap<>();
        props.put(ActivateTracingSpan.TRACING_SPAN_CONTEXT_FORMAT.name(), "json");
        transform.configure(props);

        final String tracingContext = "{\"traceparent\":\"" + propagatedTraceParent + "\",\"baggage\":\"" + propagatedBaggage + "\","
                + "\"origin\":\"test\",\"attempt\":3,\"labels\":{\"team\":\"cdc\"}}";
        final SourceRecord transformedRecord = transform.apply(recordWithTracingSpanContext(tracingContext));
        VerifyRecord.isValid(transformedRecord, true);

        final List<SpanData> spans = AgentTestingExporterAccess.getExportedSpans();
        assertThat(spans.size()).isEqualTo(2);
        assertThat(spans.get(0).getName()).isEqualTo("debezium-read");
        assertThat(spans.get(1).getName()).isEqualTo("db-log-write");
        assertThat(spans.get(1).getParentSpanId()).isEqualTo(propagatedParentId);
        assertThat(spans.get(0).getTraceId()).isEqualTo(propagatedTraceId);
        assertThat(spans.get(1).getTraceId()).isEqualTo(propagatedTraceId);
        assertThat(spans.get(1).getSpanContext().isSampled()).isTrue();

        final Headers headers = transformedRecord.headers();
        assertThat(headers.lastWithName("traceparent").value().toString()).contains(propagatedTraceId);
        assertThat(headers.lastWithName("traceparent").value().toString()).contains(spans.get(1).getSpanId());
        assertThat(headers.lastWithName("baggage").value().toString()).isEqualTo(propagatedBaggage);
        assertThat(headers.lastWithName("origin")).isNull();
        assertThat(headers.lastWithName("attempt")).isNull();
    }

    @Test
    public void whenJsonPropagationContextIsMalformedAnExceptionIsThrown() {
        AgentTestingExporterAccess.reset();
        final Map<String, String> props = new HashMap<>();
        props.put(ActivateTracingSpan.TRACING_SPAN_CONTEXT_FORMAT.name(), "json");
        transform.configure(props);

        assertThatThrownBy(() -> transform.apply(recordWithTracingSpanContext("traceparent=00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")))
                .isInstanceOf(DebeziumException.class);
        assertThatThrownBy(() -> transform.apply(recordWithTracingSpanContext("[\"traceparent\"]")))
                .isInstanceOf(DebeziumException.class);
    }

    @Test
    public void whenSpanContextFormatIsInvalidConfigurationFails() {
        final Map<String, String> props = new HashMap<>();
        props.put(ActivateTracingSpan.TRACING_SPAN_CONTEXT_FORMAT.name(), "yaml");

        assertThatThrownBy(() -> transform.configure(props)).isInstanceOf(ConnectException.class);
    }

    private SourceRecord recordWithTracingSpanContext(String tracingContext) {
        final Struct after = new Struct(recordSchemaWithTracingSpanContextField);
        final Struct source = new Struct(sourceSchema);
        source.put("table", "mytable");
        source.put("lsn", 1);
        source.put("ts_ms", 1588252618953L);

        after.put("id", (byte) 1);
        after.put("name", "test");
        after.put(tracingSpanContextFieldName, tracingContext);

        final Envelope envelope = Envelope.defineSchema()
                .withName("dummy.Envelope")
                .withRecord(recordSchemaWithTracingSpanContextField)
                .withSource(sourceSchema)
                .build();

        return new SourceRecord(
                new HashMap<>(),
                new HashMap<>(),
                "db.server1.table1",
                envelope.schema(),
                envelope.create(after, source, Instant.now()));
    }
}
