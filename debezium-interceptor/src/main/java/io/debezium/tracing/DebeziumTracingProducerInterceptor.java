/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.tracing;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.internal.ConfigUtil;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.instrumentation.kafkaclients.v2_6.TracingProducerInterceptor;

public class DebeziumTracingProducerInterceptor<K, V> extends TracingProducerInterceptor<K, V> {

    private static final Logger LOGGER = LoggerFactory.getLogger(DebeziumTracingProducerInterceptor.class);
    public static final String ARG_OTEL_INSTRUMENTATION_KAFKA_ENABLED = "otel.instrumentation.kafka.enabled";
    private static final OpenTelemetry openTelemetry = GlobalOpenTelemetry.get();
    private static final Tracer tracer = openTelemetry.getTracer(DebeziumTracingProducerInterceptor.class.getName());
    private static final TextMapPropagator TEXT_MAP_PROPAGATOR = openTelemetry.getPropagators().getTextMapPropagator();
    private static final TextMapGetter<ProducerRecord<?, ?>> GETTER = KafkaProducerRecordGetter.INSTANCE;

    @Override
    public ProducerRecord<K, V> onSend(ProducerRecord<K, V> producerRecord) {
        if (isInstrumentationKafkaEnabled()) {
            LOGGER.warn(
                    "To enable end-to-end traceability with Debezium you need to disable automatic Kafka instrumentation. To disable, run your JVM with -D{}=false\"",
                    ARG_OTEL_INSTRUMENTATION_KAFKA_ENABLED);
            return producerRecord;
        }

        Context parentContext = TEXT_MAP_PROPAGATOR.extract(Context.current(), producerRecord, GETTER);
        Span interceptorSpan = tracer.spanBuilder("onSend")
                .setSpanKind(SpanKind.INTERNAL)
                .setParent(parentContext)
                .startSpan();

        try (Scope ignored = interceptorSpan.makeCurrent()) {
            return super.onSend(producerRecord);
        }
        finally {
            interceptorSpan.end();
        }
    }

    private static boolean isInstrumentationKafkaEnabled() {
        return Boolean.parseBoolean(ConfigUtil.getString(ARG_OTEL_INSTRUMENTATION_KAFKA_ENABLED, "true"));
    }
}
