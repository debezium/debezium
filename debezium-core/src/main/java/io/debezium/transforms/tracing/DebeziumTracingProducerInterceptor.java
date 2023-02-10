/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms.tracing;

import org.apache.kafka.clients.producer.ProducerRecord;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.instrumentation.kafkaclients.TracingProducerInterceptor;

public class DebeziumTracingProducerInterceptor<K, V> extends TracingProducerInterceptor<K, V> {
    private static final OpenTelemetry openTelemetry = GlobalOpenTelemetry.get();
    private static final Tracer tracer = openTelemetry.getTracer(DebeziumTracingProducerInterceptor.class.getName());
    private static final TextMapPropagator TEXT_MAP_PROPAGATOR = openTelemetry.getPropagators().getTextMapPropagator();
    private static final TextMapGetter<ProducerRecord<?, ?>> GETTER = KafkaProducerRecordGetter.INSTANCE;

    @Override
    public ProducerRecord<K, V> onSend(ProducerRecord<K, V> producerRecord) {
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
}
