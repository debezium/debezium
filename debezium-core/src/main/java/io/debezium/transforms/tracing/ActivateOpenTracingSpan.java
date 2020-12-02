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

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.data.Envelope;
import io.debezium.transforms.SmtManager;
import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.Tracer.SpanBuilder;
import io.opentracing.propagation.Format;
import io.opentracing.util.GlobalTracer;

/**
 * This SMT enables integration with an OpenTracing system.
 * The SMT creates a OpenTracing span and enriches it with metadata from envelope and source info block.<br/>
 * It is possible to connect the span to a parent span created by a business application.
 * The application then needs to export its OpenTracing active span into a database field.
 * The SMT looks for a predefined field name in the {@code after} block
 * and when found it extracts the parent span from it.<br>
 * 
 * See {@link EventDispatcher} for example of such implementation.
 * 
 *
 * @param <R> the subtype of {@link ConnectRecord} on which this transformation will operate
 * @author Jiri Pechanec
 */
public class ActivateOpenTracingSpan<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ActivateOpenTracingSpan.class);

    private static final String DEFAULT_OPENTRACING_SPAN_VALUE = "opentracingspan";
    private static final String DEFAULT_OPENTRACING_OPERATION_NAME = "debezium";

    private static final String TAG_PREFIX = "debezium.";

    private static final Field OPENTRACING_SPAN_FIELD = Field.create("opentracing.span.field")
            .withDisplayName("OpenTracing Span Field")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDefault(DEFAULT_OPENTRACING_SPAN_VALUE)
            .withDescription("The name of the field notaining java.util.Properties representation of OpenTracing span.");

    private static final Field OPENTRACING_OOPERATION_NAME = Field.create("opentracing.operation.name")
            .withDisplayName("OpenTracing Operation Name")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDefault(DEFAULT_OPENTRACING_OPERATION_NAME)
            .withDescription("The name of the field notaining java.util.Properties representation of OpenTracing span.");

    private String spanField;
    private String operationName;

    private SmtManager<R> smtManager;

    @Override
    public void configure(Map<String, ?> props) {
        Configuration config = Configuration.from(props);
        final Field.Set configFields = Field.setOf(OPENTRACING_SPAN_FIELD, OPENTRACING_OOPERATION_NAME);

        if (!config.validateAndRecord(configFields, LOGGER::error)) {
            throw new ConnectException("Unable to validate config.");
        }

        spanField = config.getString(OPENTRACING_SPAN_FIELD);
        operationName = config.getString(OPENTRACING_OOPERATION_NAME);

        smtManager = new SmtManager<>(config);
    }

    @Override
    public R apply(R record) {
        final Tracer tracer = GlobalTracer.get();
        if (tracer == null) {
            return record;
        }

        // In case of tombstones or non-CDC events (heartbeats, schema change events),
        // leave the value as-is
        if (record.value() == null || !smtManager.isValidEnvelope(record)) {
            return record;
        }

        final Struct envelope = (Struct) record.value();
        final Struct after = envelope.getStruct(Envelope.FieldName.AFTER);
        final Struct source = envelope.getStruct(Envelope.FieldName.SOURCE);

        final SpanBuilder spanBuilder = tracer.buildSpan(operationName);
        addFieldToSpan(spanBuilder, envelope, Envelope.FieldName.OPERATION, TAG_PREFIX);
        addFieldToSpan(spanBuilder, envelope, Envelope.FieldName.TIMESTAMP, TAG_PREFIX);

        if (source != null) {
            final String sourcePrefix = TAG_PREFIX + "source.";
            for (org.apache.kafka.connect.data.Field field : source.schema().fields()) {
                addFieldToSpan(spanBuilder, source, field.name(), sourcePrefix);
            }
        }

        if (after != null) {
            if (after.schema().field(spanField) != null) {
                final String propagatedSpan = after.getString(spanField);
                if (propagatedSpan != null) {
                    final DebeziumTextMap parentSpanMap = new DebeziumTextMap(propagatedSpan);
                    spanBuilder.asChildOf(tracer.extract(Format.Builtin.TEXT_MAP, parentSpanMap));
                }
            }
        }

        final Span span = spanBuilder.start();
        try (final Scope scope = tracer.scopeManager().activate(span)) {
            final DebeziumTextMap activeTextMap = new DebeziumTextMap();
            tracer.inject(span.context(), Format.Builtin.TEXT_MAP, activeTextMap);
            activeTextMap.forEach(e -> record.headers().add(e.getKey(), e.getValue(), Schema.STRING_SCHEMA));
            span.finish();
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
                OPENTRACING_SPAN_FIELD,
                OPENTRACING_OOPERATION_NAME);
        return config;
    }

    private void addFieldToSpan(SpanBuilder span, Struct struct, String field, String prefix) {
        final Object fieldValue = struct.get(field);
        if (fieldValue != null) {
            span.withTag(prefix + field, fieldValue.toString());
        }
    }
}
