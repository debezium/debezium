/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

import static java.util.Collections.unmodifiableMap;
import static java.util.stream.Collectors.toUnmodifiableSet;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import org.slf4j.MDC;

import io.debezium.annotation.VisibleForTesting;
import io.debezium.pipeline.spi.Partition;

/**
 * A utility that provides a consistent set of properties for the Mapped Diagnostic Context (MDC) properties used by Debezium
 * components.
 *
 * @author Randall Hauch
 * @since 0.2
 */
public final class LoggingContext {

    /**
     * The key for the connector type MDC property.
     */
    public static final String CONNECTOR_TYPE = "dbz.connectorType";
    /**
     * The key for the connector logical name MDC property.
     */
    public static final String CONNECTOR_NAME = "dbz.connectorLogicalName";
    /**
     * The key for the connector context name MDC property.
     */
    public static final String CONNECTOR_CONTEXT = "dbz.connectorContext";
    /**
     * The key for the task id MDC property.
     */
    public static final String TASK_ID = "dbz.taskId";
    /**
     * The key for the database name MDC property.
     */
    public static final String DATABASE_NAME = "dbz.databaseName";

    private LoggingContext() {
    }

    /**
     * A snapshot of an MDC context that can be {@link #restore()}.
     */
    public static final class PreviousContext {
        private static final PreviousContext EMPTY_CONTEXT = new PreviousContext(Collections.emptyMap(), Collections.emptySet());
        private final Map<String, String> context;
        private final Set<String> rootContextKeys;

        private PreviousContext(final Map<String, String> context, final Set<String> rootContextKeys) {
            this.context = context;
            this.rootContextKeys = rootContextKeys;
        }

        /**
         * Loads snapshot of previous MDC context including copy of root context keys that needs to be removed on {@link PreviousContext#restore()}.
         *
         * @return {@link PreviousContext}
         */
        protected static PreviousContext load() {
            final var rootContext = RootLoggingContext.value();
            final var currentContext = MDC.getCopyOfContextMap();
            if (isNullOrEmpty(rootContext) && isNullOrEmpty(currentContext)) {
                return EMPTY_CONTEXT;
            }
            if (isNullOrEmpty(rootContext)) {
                return new PreviousContext(unmodifiableMap(currentContext), Collections.emptySet());
            }
            if (isNullOrEmpty(currentContext)) {
                return new PreviousContext(Collections.emptyMap(), rootContext.keySet());
            }
            final var rootContextKeys = rootContext.keySet().stream()
                    .filter(key -> !currentContext.containsKey(key))
                    .collect(toUnmodifiableSet());
            return new PreviousContext(unmodifiableMap(currentContext), rootContextKeys);
        }

        /**
         * Restore this logging context.
         */
        public void restore() {
            rootContextKeys.forEach(MDC::remove);
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
     * @throws NullPointerException if any of the parameters are null
     */
    public static PreviousContext forConnector(String connectorType, String connectorName, String contextName) {
        return forConnector(connectorType, connectorName, null, contextName, null);
    }

    /**
     * Configure for a connector the logger's Mapped Diagnostic Context (MDC) properties for the thread making this call.
     *
     * @param connectorType the type of connector; may not be null
     * @param connectorName the name of the connector; may not be null
     * @param taskId the task id; may be null
     * @param contextName the name of the context; may not be null
     * @param partition the partition; may be null
     * @return the previous MDC context; never null
     * @throws NullPointerException if connectorType, connectorLogicalName, or contextName parameters are null
     */
    public static PreviousContext forConnector(String connectorType, String connectorName, String taskId, String contextName, Partition partition) {
        Objects.requireNonNull(connectorType, "The MDC value for the connector type may not be null");
        Objects.requireNonNull(connectorName, "The MDC value for the connector name may not be null");
        Objects.requireNonNull(contextName, "The MDC value for the connector context may not be null");

        PreviousContext previous = PreviousContext.load();
        Optional.ofNullable(RootLoggingContext.value()).ifPresent(MDC::setContextMap);
        if (taskId != null) {
            MDC.put(TASK_ID, taskId);
        }
        if (partition != null && partition.getLoggingContext() != null) {
            partition.getLoggingContext().forEach((k, v) -> {
                if (k != null && v != null) {
                    MDC.put(k, v);
                }
            });
        }
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
     * @throws NullPointerException if any of the parameters are null
     */
    public static void temporarilyForConnector(String connectorType, String connectorName, String contextName, Runnable operation) {
        Objects.requireNonNull(connectorType, "The MDC value for the connector type may not be null");
        Objects.requireNonNull(connectorName, "The MDC value for the connector name may not be null");
        Objects.requireNonNull(contextName, "The MDC value for the connector context may not be null");
        Objects.requireNonNull(operation, "The operation may not be null");

        PreviousContext previous = PreviousContext.load();
        try {
            forConnector(connectorType, connectorName, contextName);
            operation.run();
        }
        finally {
            previous.restore();
        }
    }

    /**
     * Initialise root MDC context with current MDC tags.
     */
    public static RootLoggingContext initRootContext() {
        return RootLoggingContext.init();
    }

    private static <K, V> boolean isNullOrEmpty(final Map<K, V> map) {
        return map == null || map.isEmpty();
    }

    public static final class RootLoggingContext implements AutoCloseable {
        /**
         * Holder of root logging context.
         */
        private static final InheritableThreadLocal<Map<String, String>> CONTEXT_HOLDER = new InheritableThreadLocal<>();

        private static final RootLoggingContext EMPTY = new RootLoggingContext(Collections.emptySet());

        /**
         * Set of keys added to root context in this instance.
         */
        private final Set<String> contextKeys;

        private RootLoggingContext(final Set<String> contextKeys) {
            this.contextKeys = contextKeys;
        }

        /**
         * Initialise root logging context.
         *
         * @return {@link RootLoggingContext}
         */
        private static RootLoggingContext init() {
            final var context = MDC.getCopyOfContextMap();
            if (isNullOrEmpty(context)) {
                return EMPTY;
            }
            final var currentRootContext = value();
            if (currentRootContext == null) {
                CONTEXT_HOLDER.set(context);
                return new RootLoggingContext(context.keySet());
            }
            final var patchedRootContext = new HashMap<>(currentRootContext);
            patchedRootContext.putAll(context);
            CONTEXT_HOLDER.set(patchedRootContext);
            return new RootLoggingContext(context.keySet().stream().filter(it -> !currentRootContext.containsKey(it)).collect(toUnmodifiableSet()));
        }

        private static Map<String, String> value() {
            return CONTEXT_HOLDER.get();
        }

        @VisibleForTesting
        public static Map<String, String> valueCopy() {
            return Optional.ofNullable(value()).map(Map::copyOf).orElse(Collections.emptyMap());
        }

        @Override
        public void close() {
            if (contextKeys.isEmpty()) {
                return;
            }
            final var currentRootContext = value();
            if (currentRootContext != null) {
                final var newContext = new HashMap<>(currentRootContext);
                contextKeys.forEach(newContext::remove);
                if (newContext.isEmpty()) {
                    CONTEXT_HOLDER.remove();
                    return;
                }
                CONTEXT_HOLDER.set(newContext);
            }
        }
    }
}
