/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded;

import java.util.Map;

import org.slf4j.MDC;

import io.debezium.engine.source.EngineTaskId;

/**
 * Provides MDC logging context for the embedded engine without depending on Kafka's LoggingContext.
 * Sets the {@code connector.context} MDC key in the same format as Kafka Connect's LoggingContext
 * for compatibility with existing log patterns.
 *
 * @author Debezium Authors
 */
public final class EngineLoggingContext implements AutoCloseable {

    private static final String CONNECTOR_CONTEXT = "connector.context";

    private final Map<String, String> previous;

    private EngineLoggingContext() {
        previous = MDC.getCopyOfContextMap();
    }

    public static void clear() {
        MDC.clear();
    }

    public static EngineLoggingContext forTask(EngineTaskId taskId) {
        EngineLoggingContext context = new EngineLoggingContext();
        MDC.put(CONNECTOR_CONTEXT, "[" + taskId.connectorName() + "|task-" + taskId.taskId() + "] ");
        return context;
    }

    @Override
    public void close() {
        if (previous != null && previous.containsKey(CONNECTOR_CONTEXT)) {
            MDC.put(CONNECTOR_CONTEXT, previous.get(CONNECTOR_CONTEXT));
        }
        else {
            MDC.remove(CONNECTOR_CONTEXT);
        }
    }
}
