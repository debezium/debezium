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

    /**
     * The constructor for the DebeziumTracingProducerInterceptor.
     * <p>
     * In this interceptor, we use a dynamic approach to handle the OpenTelemetry tracing interceptor due to versioning issues.
     * The problem arises because different versions of OpenTelemetry have their tracing interceptor in different packages.
     * For example, in versions before 1.23.0, the tracing interceptor is in the "io.opentelemetry.instrumentation.kafkaclients" package,
     * but from version 1.23.0 onwards, it is in the "io.opentelemetry.instrumentation.kafkaclients.v2_6" package.
     * <p>
     * The OpenTelemetry interceptor is also part of an alpha package, which means it's subject to change,
     * and there is no guarantee of backward compatibility. That's why a dynamic approach is used here.
     * We maintain an array of possible class names ({@link OpenTelemetryInterceptorVersion}) for the OpenTelemetry tracing interceptor,
     * and we attempt to instantiate one of them at runtime. We also use reflection to access the 'onSend' method from the interceptor.
     * <p>
     * This allows the Debezium Kafka Connector to work with different versions of OpenTelemetry.
     */
    public DebeziumTracingProducerInterceptor() {
        OpenTelemetryInterceptorVersion[] versions = {
                new OpenTelemetryInterceptorVersion("io.opentelemetry.instrumentation.kafkaclients.v2_6.TracingProducerInterceptor"),
                new OpenTelemetryInterceptorVersion("io.opentelemetry.instrumentation.kafkaclients.TracingProducerInterceptor"),
        };

        for (OpenTelemetryInterceptorVersion version : versions) {
            interceptorInstance = version.createInstance();
            if (interceptorInstance != null) {
                onSendMethod = version.getMethod("onSend", ProducerRecord.class);
                if (onSendMethod != null) {
                    break;
                }
            }
        }

        if (onSendMethod == null) {
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
