/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

import java.util.Collections;
import java.util.Map;

import org.slf4j.MDC;

/**
 * A utility that provides a consistent set of properties for the Mapped Diagnostic Context (MDC) properties used by Debezium
 * components.
 *
 * @author Randall Hauch
 * @since 0.2
 */
public class LoggingContext {

    /**
     * The key for the connector type MDC property.
     */
    public static final String CONNECTOR_TYPE = "dbz.connectorType";
    /**
     * The key for the connector logical name MDC property.
     */
    public static final String CONNECTOR_NAME = "dbz.connectorName";
    /**
     * The key for the connector context name MDC property.
     */
    public static final String CONNECTOR_CONTEXT = "dbz.connectorContext";

    private LoggingContext() {
    }

    /**
     * A snapshot of an MDC context that can be {@link #restore()}.
     */
    public static final class PreviousContext {
        private static final Map<String, String> EMPTY_CONTEXT = Collections.emptyMap();
        private final Map<String, String> context;

        protected PreviousContext() {
            Map<String, String> context = MDC.getCopyOfContextMap();
            this.context = context != null ? context : EMPTY_CONTEXT;
        }

        /**
         * Restore this logging context.
         */
        public void restore() {
            MDC.setContextMap(context);
        }
    }

    /**
     * Configure for a connector the logger's Mapped Diagnostic Context (MDC) properties for the thread making this call.
     *
     * @param connectorType the type of connector; may not be null
     * @param connectorName the name of the connector; may not be null
     * @param contextName the name of the context; may not be null
     * @return the previous MDC context; never null
     * @throws IllegalArgumentException if any of the parameters are null
     */
    public static PreviousContext forConnector(String connectorType, String connectorName, String contextName) {
        if (connectorType == null) {
            throw new IllegalArgumentException("The MDC value for the connector type may not be null");
        }
        if (connectorName == null) {
            throw new IllegalArgumentException("The MDC value for the connector name may not be null");
        }
        if (contextName == null) {
            throw new IllegalArgumentException("The MDC value for the connector context may not be null");
        }
        PreviousContext previous = new PreviousContext();
        MDC.put(CONNECTOR_TYPE, connectorType);
        MDC.put(CONNECTOR_NAME, connectorName);
        MDC.put(CONNECTOR_CONTEXT, contextName);
        return previous;
    }

    /**
     * Run the supplied function in the temporary connector MDC context, and when complete always return the MDC context to its
     * state before this method was called.
     *
     * @param connectorType the type of connector; may not be null
     * @param connectorName the logical name of the connector; may not be null
     * @param contextName the name of the context; may not be null
     * @param operation the function to run in the new MDC context; may not be null
     * @throws IllegalArgumentException if any of the parameters are null
     */
    public static void temporarilyForConnector(String connectorType, String connectorName, String contextName, Runnable operation) {
        if (connectorType == null) {
            throw new IllegalArgumentException("The MDC value for the connector type may not be null");
        }
        if (connectorName == null) {
            throw new IllegalArgumentException("The MDC value for the connector name may not be null");
        }
        if (contextName == null) {
            throw new IllegalArgumentException("The MDC value for the connector context may not be null");
        }
        if (operation == null) {
            throw new IllegalArgumentException("The operation may not be null");
        }
        PreviousContext previous = new PreviousContext();
        try {
            forConnector(connectorType, connectorName, contextName);
            operation.run();
        }
        finally {
            previous.restore();
        }
    }

}
