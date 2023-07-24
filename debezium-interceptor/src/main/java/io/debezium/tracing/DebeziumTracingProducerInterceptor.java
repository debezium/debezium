/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.tracing;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapPropagator;

public class DebeziumTracingProducerInterceptor<K, V> implements ProducerInterceptor<K, V> {

    private static final Logger LOGGER = LoggerFactory.getLogger(DebeziumTracingProducerInterceptor.class);
    private static final OpenTelemetry openTelemetry = GlobalOpenTelemetry.get();
    private static final Tracer tracer = openTelemetry.getTracer(DebeziumTracingProducerInterceptor.class.getName());
    private static final TextMapPropagator TEXT_MAP_PROPAGATOR = openTelemetry.getPropagators().getTextMapPropagator();
    private static final TextMapGetter<ProducerRecord<?, ?>> GETTER = KafkaProducerRecordGetter.INSTANCE;

    private Object interceptorInstance;
    private Method onSendMethod;

    public DebeziumTracingProducerInterceptor() {
        InterceptorVersion[] versions = {
                new InterceptorVersion("io.opentelemetry.instrumentation.kafkaclients.v2_6.TracingProducerInterceptor"),
                new InterceptorVersion("io.opentelemetry.instrumentation.kafkaclients.TracingProducerInterceptor"),
        };

        for (InterceptorVersion version : versions) {
            interceptorInstance = version.createInstance();
            if (interceptorInstance != null) {
                onSendMethod = version.getMethod("onSend", ProducerRecord.class);
                if (onSendMethod != null) {
                    break;
                }
            }
        }

        if (interceptorInstance == null || onSendMethod == null) {
            LOGGER.error("Unable to instantiate any known version of the interceptor");
            throw new IllegalStateException("Unable to instantiate interceptor");
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public ProducerRecord<K, V> onSend(ProducerRecord<K, V> producerRecord) {

        Context parentContext = TEXT_MAP_PROPAGATOR.extract(Context.current(), producerRecord, GETTER);
        Span interceptorSpan = tracer.spanBuilder("onSend")
                .setSpanKind(SpanKind.INTERNAL)
                .setParent(parentContext)
                .startSpan();

        try (Scope ignored = interceptorSpan.makeCurrent()) {
            try {
                return (ProducerRecord<K, V>) onSendMethod.invoke(interceptorInstance, producerRecord);
            }
            catch (IllegalAccessException | InvocationTargetException e) {
                LOGGER.error("Error invoking onSend method", e);
                throw new RuntimeException("Error invoking onSend method", e);
            }
        }
        finally {
            interceptorSpan.end();
        }
    }

    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
