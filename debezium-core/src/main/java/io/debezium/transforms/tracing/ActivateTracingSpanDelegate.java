/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms.tracing;

import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.data.Envelope;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.internal.ConfigUtil;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.context.propagation.TextMapSetter;

public class ActivateTracingSpanDelegate {

    private static final Logger LOGGER = LoggerFactory.getLogger(ActivateTracingSpanDelegate.class);

    private static final String DB_FIELDS_PREFIX = "db.";
    private static final String TX_LOG_WRITE_OPERATION_NAME = "db-log-write";
    public static final String ARG_OTEL_JAVAAGENT_ENABLED = "otel.javaagent.enabled";
    public static final String ARG_OTEL_INSTRUMENTATION_OPENTELEMETRY_API_ENABLED = "otel.instrumentation.opentelemetry-api.enabled";

    private static final String TRACING_COMPONENT = ActivateTracingSpanDelegate.class.getName();
    private static final OpenTelemetry openTelemetry = GlobalOpenTelemetry.get();
    private static final Tracer tracer = openTelemetry.getTracer(TRACING_COMPONENT);
    private static final TextMapPropagator TEXT_MAP_PROPAGATOR = openTelemetry.getPropagators().getTextMapPropagator();
    private static final TextMapSetter<Headers> SETTER = KafkaConnectHeadersSetter.INSTANCE;
    private static final TextMapGetter<Properties> GETTER = PropertiesGetter.INSTANCE;

    private ActivateTracingSpanDelegate() {

    }

    public <R extends ConnectRecord<R>> R traceRecord(R connectRecord, Struct envelope, Struct source, String propagatedSpanContext, String operationName) {
        if (!is0penTelemetryJavaagentEnable()) {
            LOGGER.debug(
                    "OpenTelemetry javaagent is disabled. To enable, run your JVM with -D{}=true\"",
                    ARG_OTEL_JAVAAGENT_ENABLED);
        }
        if (!isOpenTelemetryApiEnable()) {
            LOGGER.debug(
                    "OpenTelemetry API is disabled. To enable, run your JVM with -D{}=true\"",
                    ARG_OTEL_INSTRUMENTATION_OPENTELEMETRY_API_ENABLED);
        }
        if (propagatedSpanContext != null) {

            Properties props = PropertiesGetter.extract(propagatedSpanContext);

            Context parentSpanContext = openTelemetry.getPropagators().getTextMapPropagator()
                    .extract(Context.current(), props, GETTER);

            SpanBuilder txLogSpanBuilder = tracer.spanBuilder(TX_LOG_WRITE_OPERATION_NAME)
                    .setSpanKind(SpanKind.INTERNAL)
                    .setParent(parentSpanContext);

            if (source != null) {
                Long eventTimestamp = source.getInt64(AbstractSourceInfo.TIMESTAMP_KEY);
                if (Objects.nonNull(eventTimestamp)) {
                    txLogSpanBuilder.setStartTimestamp(eventTimestamp, TimeUnit.MILLISECONDS);
                }
            }

            Span txLogSpan = txLogSpanBuilder.startSpan();

            try (Scope ignored = txLogSpan.makeCurrent()) {
                if (source != null) {
                    for (org.apache.kafka.connect.data.Field field : source.schema().fields()) {
                        addFieldToSpan(txLogSpan, source, field.name(), DB_FIELDS_PREFIX);
                    }
                }
                debeziumSpan(envelope, operationName);

                TEXT_MAP_PROPAGATOR.inject(Context.current(), connectRecord.headers(), SETTER);
            }
            finally {
                txLogSpan.end();
            }
        }

        return connectRecord;
    }

    private void debeziumSpan(Struct envelope, String operationName) {
        final Long processingTimestamp = envelope.getInt64(Envelope.FieldName.TIMESTAMP);
        Span debeziumSpan = ActivateTracingSpanDelegate.tracer.spanBuilder(operationName)
                .setStartTimestamp(processingTimestamp, TimeUnit.MILLISECONDS)
                .startSpan();

        try (Scope ignored = debeziumSpan.makeCurrent()) {
            addFieldToSpan(debeziumSpan, envelope, Envelope.FieldName.OPERATION, "");
            addFieldToSpan(debeziumSpan, envelope, Envelope.FieldName.TIMESTAMP, "");
        }
        finally {
            debeziumSpan.end();
        }
    }

    private void addFieldToSpan(Span span, Struct struct, String field, String prefix) {
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
            span.setAttribute(targetFieldName, fieldValue.toString());
        }
    }

    public static class Builder {

        private Builder() {

        }

        public static ActivateTracingSpanDelegate build() {
            return new ActivateTracingSpanDelegate();
        }
    }

    private static boolean is0penTelemetryJavaagentEnable() {
        return Boolean.parseBoolean(ConfigUtil.getString(ARG_OTEL_JAVAAGENT_ENABLED, "true"));
    }

    private static boolean isOpenTelemetryApiEnable() {
        return Boolean.parseBoolean(ConfigUtil.getString(ARG_OTEL_INSTRUMENTATION_OPENTELEMETRY_API_ENABLED, "true"));
    }
}
