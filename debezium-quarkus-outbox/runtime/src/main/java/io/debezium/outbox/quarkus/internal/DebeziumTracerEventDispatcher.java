/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.outbox.quarkus.internal;

import java.util.Map;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.outbox.quarkus.ExportedEvent;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapPropagator;

/**
 * An application-scoped {@link EventDispatcher} implementation that is responsible not only
 * for observing {@link ExportedEvent} events but also generating an open tracing span that
 * is to be persisted with the event's data, allowing Debezium to capture and emit these as
 * change events.
 *
 */
@ApplicationScoped
public class DebeziumTracerEventDispatcher extends AbstractEventDispatcher {

    private static final String OPERATION_NAME = "outbox-write";
    private static final String TRACING_COMPONENT = "debezium";

    private static final Logger LOGGER = LoggerFactory.getLogger(DebeziumTracerEventDispatcher.class);

    @Inject
    OpenTelemetry openTelemetry;

    @Override
    public void onExportedEvent(@Observes ExportedEvent<?, ?> event) {
        LOGGER.debug("An exported event was found for type {}", event.getType());

        final Tracer tracer = openTelemetry.getTracer(TRACING_COMPONENT);
        final SpanBuilder spanBuilder = tracer.spanBuilder(OPERATION_NAME);
        final DataMapTracingSetter exportedSpanData = DataMapTracingSetter.create();

        final Span parentSpan = Span.current();
        if (parentSpan != null) {
            spanBuilder.setParent(Context.current().with(parentSpan));
        }
        spanBuilder.setAttribute(AGGREGATE_TYPE, event.getAggregateType())
                .setAttribute(AGGREGATE_ID, event.getAggregateId().toString())
                .setAttribute(TYPE, event.getAggregateType())
                .setAttribute(TIMESTAMP, event.getTimestamp().toString())
                .setSpanKind(SpanKind.INTERNAL);

        final Span activeSpan = spanBuilder.startSpan();
        try (Scope outboxSpanScope = activeSpan.makeCurrent()) {

            // Define the entity map-mode object using property names and values
            final Map<String, Object> dataMap = getDataMapFromEvent(event);
            TextMapPropagator textMapPropagator = openTelemetry.getPropagators().getTextMapPropagator();
            textMapPropagator.inject(Context.current(), dataMap, exportedSpanData);
            persist(dataMap);
        }
        finally {
            activeSpan.end();
        }
    }
}
