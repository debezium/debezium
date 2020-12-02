/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.outbox.quarkus.internal;

import static io.debezium.outbox.quarkus.internal.OutboxConstants.OUTBOX_ENTITY_FULLNAME;

import java.util.HashMap;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;
import javax.persistence.EntityManager;

import org.hibernate.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.outbox.quarkus.ExportedEvent;
import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.Tracer.SpanBuilder;
import io.opentracing.propagation.Format;

/**
 * An application-scope component that is responsible for observing
 * {@link ExportedEvent} events and when detected, persists those events to the
 * underlying database allowing Debezium to then capture and emit those change
 * events.
 *
 * @author Chris Cranford
 */
@ApplicationScoped
public class EventDispatcher {

    private static final String OPENTRACING_OPERATION_NAME = "io.debezium.outbox.send";
    private static final String TIMESTAMP = "timestamp";
    private static final String PAYLOAD = "payload";
    private static final String TYPE = "type";
    private static final String AGGREGATE_ID = "aggregateId";
    private static final String AGGREGATE_TYPE = "aggregateType";
    public static final String OPENTRACING_SPAN = "openTracingSpan";

    private static final String TRACING_PREFIX = "outbox.";

    private static final Logger LOGGER = LoggerFactory.getLogger(EventDispatcher.class);

    @Inject
    EntityManager entityManager;

    /**
     * Debezium runtime configuration
     */
    @Inject
    DebeziumOutboxRuntimeConfig config;

    @Inject
    Tracer tracer;

    /**
     * An event handler for {@link ExportedEvent} events and will be called when
     * the event fires.
     *
     * @param event
     *            the exported event
     */
    public void onExportedEvent(@Observes ExportedEvent<?, ?> event) {
        LOGGER.debug("An exported event was found for type {}", event.getType());

        final SpanBuilder spanBuilder = tracer.buildSpan(OPENTRACING_OPERATION_NAME);
        final DebeziumTextMap exportedSpanData = new DebeziumTextMap();

        final Span parentSpan = tracer.activeSpan();
        if (parentSpan != null) {
            spanBuilder.asChildOf(parentSpan);
        }
        spanBuilder.withTag(TRACING_PREFIX + AGGREGATE_TYPE, event.getAggregateType())
                .withTag(TRACING_PREFIX + AGGREGATE_ID, event.getAggregateId().toString())
                .withTag(TRACING_PREFIX + TYPE, event.getAggregateType())
                .withTag(TRACING_PREFIX + TIMESTAMP, event.getTimestamp().toString());

        try (final Scope outboxSpanScope = spanBuilder.startActive(true)) {
            tracer.inject(outboxSpanScope.span().context(),
                    Format.Builtin.TEXT_MAP, exportedSpanData);

            // Define the entity map-mode object using property names and values
            final HashMap<String, Object> dataMap = new HashMap<>();
            dataMap.put(AGGREGATE_TYPE, event.getAggregateType());
            dataMap.put(AGGREGATE_ID, event.getAggregateId());
            dataMap.put(TYPE, event.getType());
            dataMap.put(PAYLOAD, event.getPayload());
            dataMap.put(TIMESTAMP, event.getTimestamp());
            dataMap.put(OPENTRACING_SPAN, exportedSpanData.export());

            // Unwrap to Hibernate session and save
            Session session = entityManager.unwrap(Session.class);
            session.save(OUTBOX_ENTITY_FULLNAME, dataMap);

            // Remove entity if the configuration deems doing so, leaving useful
            // for debugging
            if (config.removeAfterInsert) {
                session.delete(OUTBOX_ENTITY_FULLNAME, dataMap);
            }
        }
    }
}
