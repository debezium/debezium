/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;

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
}
