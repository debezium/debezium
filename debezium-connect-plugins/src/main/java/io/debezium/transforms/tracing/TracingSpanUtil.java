/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms.tracing;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.data.Envelope;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapPropagator;

public class TracingSpanUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(TracingSpanUtil.class);

    private static final String DB_FIELDS_PREFIX = "db.";
    private static final String TX_LOG_WRITE_OPERATION_NAME = "db-log-write";
    private static final String TRACING_COMPONENT = TracingSpanUtil.class.getName();
    private static final OpenTelemetry openTelemetry = GlobalOpenTelemetry.get();
    private static final Tracer tracer = openTelemetry.getTracer(TRACING_COMPONENT);

    private TracingSpanUtil() {

    }

    /**
     * Create tracing spans representing the write operation in the database (the span timestamp is taken
     * from the source event if available), as well as Debezium's processing operation (the span timestamp
     * is taken from the envelope's timestamp).
     * If a trace context is provided for propagation, it is set as the parent context of the
     * created spans to enable distributed tracing.
     * The resulting context is injected in the Kafka Connect Record headers for further propagation.
     *
     * @param <R> the subtype of {@link ConnectRecord} on which this transformation will operate
     * @param connectRecord the Connect record that is to be enriched with tracing information
     * @param envelope the envelope wrapped by the record
     * @param source the source field of the envelope, or {@code null}
     * @param propagatedSpanContext a String serialization of a {@link java.util.Properties} instance representing
     *   the parent span context that should be used for trace propagation, or {@code null}
     * @param operationName the operation name of the debezium processing span
     * @return the connect record with message headers augmented with tracing information
     */
    public static <R extends ConnectRecord<R>> R traceRecord(R connectRecord, Struct envelope, Struct source, String propagatedSpanContext, String operationName) {
        SpanBuilder txLogSpanBuilder = tracer.spanBuilder(TX_LOG_WRITE_OPERATION_NAME)
                .setSpanKind(SpanKind.INTERNAL);

        if (propagatedSpanContext != null) {
            Properties props = PropertiesGetter.extract(propagatedSpanContext);

            Context parentSpanContext = openTelemetry.getPropagators().getTextMapPropagator()
                    .extract(Context.current(), props, PropertiesGetter.INSTANCE);

            txLogSpanBuilder.setParent(parentSpanContext);
        }

        if (source != null) {
            Long eventTimestamp = source.getInt64(AbstractSourceInfo.TIMESTAMP_KEY);
            if (eventTimestamp != null) {
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

            TextMapPropagator textMapPropagator = openTelemetry.getPropagators().getTextMapPropagator();
            textMapPropagator.inject(Context.current(), connectRecord.headers(), KafkaConnectHeadersSetter.INSTANCE);
        }
        finally {
            txLogSpan.end();
        }

        return connectRecord;
    }

    private static void debeziumSpan(Struct envelope, String operationName) {
        final Long processingTimestamp = envelope.getInt64(Envelope.FieldName.TIMESTAMP);
        Span debeziumSpan = tracer.spanBuilder(operationName)
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

    private static void addFieldToSpan(Span span, Struct struct, String field, String prefix) {
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
}
