/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.outbox.quarkus.internal;

import java.util.Map;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.outbox.quarkus.DebeziumCustomCodec;
import io.debezium.outbox.quarkus.ExportedEvent;
import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.propagation.Format;
import io.opentracing.tag.Tags;
import io.quarkus.vertx.ConsumeEvent;
import io.smallrye.mutiny.Uni;

/**
 * An application-scoped {@link EventDispatcher} implementation that is responsible not only
 * for observing {@link ExportedEvent} events but also generating an open tracing span that
 * is to be persisted with the event's data, allowing Debezium to capture and emit these as
 * change events.
 *
 */
@ApplicationScoped
public class DebeziumTracerEventDispatcher extends AbstractEventDispatcher {

    private static final Logger LOGGER = LoggerFactory.getLogger(DebeziumTracerEventDispatcher.class);
    private static final String OPERATION_NAME = "outbox-write";
    private static final String TRACING_COMPONENT = "debezium";

    @Inject
    Tracer tracer;

    @Override
    @ConsumeEvent(value = "debezium-outbox", codec = DebeziumCustomCodec.class)
    public Uni<Void> onExportedEvent(Object incomingevent) {
        ExportedEvent<?, ?> event = (ExportedEvent<?, ?>) incomingevent;
        LOGGER.debug("An exported event was found for type {}" + event.getType());
        final Tracer.SpanBuilder spanBuilder = tracer.buildSpan(OPERATION_NAME);
        final DebeziumTextMap exportedSpanData = new DebeziumTextMap();

        final Span parentSpan = tracer.activeSpan();
        if (parentSpan != null) {
            spanBuilder.asChildOf(parentSpan);
        }
        spanBuilder.withTag(AGGREGATE_TYPE, event.getAggregateType())
                .withTag(AGGREGATE_ID, event.getAggregateId().toString())
                .withTag(TYPE, event.getAggregateType())
                .withTag(TIMESTAMP, event.getTimestamp().toString());

        try (final Scope outboxSpanScope = tracer.scopeManager().activate(spanBuilder.start())) {
            final Span activeSpan = tracer.scopeManager().activeSpan();

            Tags.COMPONENT.set(activeSpan, TRACING_COMPONENT);
            tracer.inject(activeSpan.context(), Format.Builtin.TEXT_MAP, exportedSpanData);

            // Define the entity map-mode object using property names and values
            final Map<String, Object> dataMap = getDataMapFromEvent((ExportedEvent<?, ?>) event);
            dataMap.put(OutboxConstants.TRACING_SPAN_CONTEXT, exportedSpanData.export());

            return persist(dataMap);
        }
    }
}
