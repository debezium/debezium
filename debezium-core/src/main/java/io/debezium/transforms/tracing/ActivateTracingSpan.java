/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms.tracing;

import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.AbstractSourceInfo;
import io.debezium.data.Envelope;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.transforms.SmtManager;
import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.Tracer.SpanBuilder;
import io.opentracing.propagation.Format;
import io.opentracing.tag.Tags;
import io.opentracing.util.GlobalTracer;

/**
 * This SMT enables integration with a tracing system.
 * The SMT creates a tracing span and enriches it with metadata from envelope and source info block.<br/>
 * It is possible to connect the span to a parent span created by a business application.
 * The application then needs to export its tracing active span context into a database field.
 * The SMT looks for a predefined field name in the {@code after} block
 * and when found it extracts the parent span from it.
 * 
 * @see {@link EventDispatcher} for example of such implementation
 * 
 *
 * @param <R> the subtype of {@link ConnectRecord} on which this transformation will operate
 * @author Jiri Pechanec
 */
public class ActivateTracingSpan<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final String DB_FIELDS_PREFIX = "db.";

    private static final Logger LOGGER = LoggerFactory.getLogger(ActivateTracingSpan.class);

    private static final String DEFAULT_TRACING_SPAN_CONTEXT_FIELD = "tracingspancontext";
    private static final String DEFAULT_TRACING_OPERATION_NAME = "debezium-read";

    private static final String TRACING_COMPONENT = "debezium";
    private static final String TX_LOG_WRITE_OPERATION_NAME = "db-log-write";

    private static final boolean OPEN_TRACING_AVAILABLE = resolveOpenTracingApiAvailable();

    public static final Field TRACING_SPAN_CONTEXT_FIELD = Field.create("tracing.span.context.field")
            .withDisplayName("Serialized tracing span context field")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDefault(DEFAULT_TRACING_SPAN_CONTEXT_FIELD)
            .withDescription("The name of the field containing java.util.Properties representation of serialized span context. Defaults to '"
                    + DEFAULT_TRACING_SPAN_CONTEXT_FIELD + "'");

    public static final Field TRACING_OPERATION_NAME = Field.create("tracing.operation.name")
            .withDisplayName("Tracing operation name")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDefault(DEFAULT_TRACING_OPERATION_NAME)
            .withDescription("The operation name representing Debezium processing span. Default is '" + DEFAULT_TRACING_OPERATION_NAME + "'");

    public static final Field TRACING_CONTEXT_FIELD_REQUIRED = Field.create("tracing.with.context.field.only")
            .withDisplayName("Trace only events with context field present")
            .withType(ConfigDef.Type.BOOLEAN)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDefault(false)
            .withDescription("Set to `true` when only events that have serialized context field should be traced.");

    private String spanContextField;
    private String operationName;
    private boolean requireContextField;

    private SmtManager<R> smtManager;

    @Override
    public void configure(Map<String, ?> props) {
        Configuration config = Configuration.from(props);
        final Field.Set configFields = Field.setOf(TRACING_SPAN_CONTEXT_FIELD, TRACING_OPERATION_NAME);

        if (!config.validateAndRecord(configFields, LOGGER::error)) {
            throw new ConnectException("Unable to validate config.");
        }

        spanContextField = config.getString(TRACING_SPAN_CONTEXT_FIELD);
        operationName = config.getString(TRACING_OPERATION_NAME);
        requireContextField = config.getBoolean(TRACING_CONTEXT_FIELD_REQUIRED);

        smtManager = new SmtManager<>(config);
    }

    public void setRequireContextField(boolean requireContextField) {
        this.requireContextField = requireContextField;
    }

    @Override
    public R apply(R record) {
        // In case of tombstones or non-CDC events (heartbeats, schema change events),
        // leave the value as-is
        if (record.value() == null || !smtManager.isValidEnvelope(record)) {
            return record;
        }

        final Struct envelope = (Struct) record.value();

        final Struct after = (envelope.schema().field(Envelope.FieldName.AFTER) != null) ? envelope.getStruct(Envelope.FieldName.AFTER) : null;
        final Struct source = (envelope.schema().field(Envelope.FieldName.SOURCE) != null) ? envelope.getStruct(Envelope.FieldName.SOURCE) : null;
        String propagatedSpanContext = null;

        if (after != null) {
            if (after.schema().field(spanContextField) != null) {
                propagatedSpanContext = after.getString(spanContextField);
            }
        }

        if (propagatedSpanContext == null && requireContextField) {
            return record;
        }

        try {
            return traceRecord(record, envelope, source, after, propagatedSpanContext);
        }
        catch (NoClassDefFoundError e) {
            throw new DebeziumException("Failed to record tracing information, tracing libraries not available", e);
        }
    }

    private R traceRecord(R record, Struct envelope, Struct source, Struct after, String propagatedSpanContext) {
        final Tracer tracer = GlobalTracer.get();
        if (tracer == null) {
            return record;
        }

        final SpanBuilder txLogSpanBuilder = tracer.buildSpan(TX_LOG_WRITE_OPERATION_NAME);

        final SpanBuilder debeziumSpanBuilder = tracer.buildSpan(operationName);
        addFieldToSpan(debeziumSpanBuilder, envelope, Envelope.FieldName.OPERATION, "");
        addFieldToSpan(debeziumSpanBuilder, envelope, Envelope.FieldName.TIMESTAMP, "");

        final Long processingTimestamp = envelope.getInt64(Envelope.FieldName.TIMESTAMP);
        if (processingTimestamp != null) {
            debeziumSpanBuilder.withStartTimestamp(processingTimestamp * 1_000);
        }

        Long eventTimestamp = null;
        if (source != null) {
            for (org.apache.kafka.connect.data.Field field : source.schema().fields()) {
                addFieldToSpan(txLogSpanBuilder, source, field.name(), DB_FIELDS_PREFIX);
            }
            eventTimestamp = source.getInt64(AbstractSourceInfo.TIMESTAMP_KEY);
            if (eventTimestamp != null) {
                txLogSpanBuilder.withStartTimestamp(eventTimestamp * 1_000);
            }
        }

        if (propagatedSpanContext != null) {
            final DebeziumTextMap parentSpanContextMap = new DebeziumTextMap(propagatedSpanContext);
            final SpanContext parentSpanContext = tracer.extract(Format.Builtin.TEXT_MAP, parentSpanContextMap);
            txLogSpanBuilder.asChildOf(parentSpanContext);
        }

        final Span txLogSpan = txLogSpanBuilder.start();
        debeziumSpanBuilder.asChildOf(txLogSpan);
        final Span debeziumSpan = debeziumSpanBuilder.start();
        try (final Scope debeziumScope = tracer.scopeManager().activate(debeziumSpan)) {
            Tags.COMPONENT.set(txLogSpan, TRACING_COMPONENT);
            Tags.COMPONENT.set(debeziumSpan, TRACING_COMPONENT);
            if (eventTimestamp != null) {
                txLogSpan.finish(eventTimestamp * 1_000);
            }
            else {
                txLogSpan.finish();
            }
            debeziumSpan.finish();
            final DebeziumTextMap activeTextMap = new DebeziumTextMap();
            tracer.inject(debeziumSpan.context(), Format.Builtin.TEXT_MAP, activeTextMap);
            activeTextMap.forEach(e -> record.headers().add(e.getKey(), e.getValue(), Schema.STRING_SCHEMA));
        }

        return record;
    }

    @Override
    public void close() {
    }

    @Override
    public ConfigDef config() {
        ConfigDef config = new ConfigDef();
        Field.group(
                config,
                null,
                TRACING_SPAN_CONTEXT_FIELD,
                TRACING_OPERATION_NAME,
                TRACING_CONTEXT_FIELD_REQUIRED);
        return config;
    }

    private void addFieldToSpan(SpanBuilder span, Struct struct, String field, String prefix) {
        final Object fieldValue = struct.get(field);
        if (fieldValue != null) {
            String targetFieldName = prefix + field;
            if (DB_FIELDS_PREFIX.equals(prefix)) {
                if ("db".equals(field)) {
                    targetFieldName = prefix + "instance";
                }
                else if ("connector".equals(field)) {
                    targetFieldName = prefix + "type";
                }
                else if ("name".equals(field)) {
                    targetFieldName = prefix + "cdc-name";
                }
            }
            span.withTag(targetFieldName, fieldValue.toString());
        }
    }

    public static boolean isOpenTracingAvailable() {
        return OPEN_TRACING_AVAILABLE;
    }

    private static boolean resolveOpenTracingApiAvailable() {
        try {
            GlobalTracer.get();
            return true;
        }
        catch (NoClassDefFoundError e) {
            // ignored
        }
        return false;
    }
}
