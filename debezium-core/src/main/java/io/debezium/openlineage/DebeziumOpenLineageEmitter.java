/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.openlineage;

import java.util.concurrent.atomic.AtomicReference;

import io.debezium.config.Configuration;
import io.debezium.connector.common.BaseSourceTask;
import io.debezium.relational.Table;
import io.openlineage.client.OpenLineage;

/**
 * A utility class for emitting OpenLineage events from Debezium connectors.
 * <p>
 * This class serves as a facade for the underlying OpenLineage integration, providing
 * static methods for initializing the emitter and emitting lineage events at various
 * points in the Debezium connector lifecycle. The implementation uses a thread-safe
 * approach to ensure proper initialization across multiple threads.
 * <p>
 * The emitter will only be active if OpenLineage integration is enabled in the configuration.
 * Otherwise, a no-operation implementation is used that performs no actual emission.
 *
 * @see LineageEmitter
 * @see OpenLineageEventEmitter
 * @see OpenLineageContext
 */
public class DebeziumOpenLineageEmitter {

    private static final String CONNECTOR_NAME_PROPERTY = "name";

    private static LineageEmitter lineageEmitter;
    private static final AtomicReference<OpenLineageContext> contextRef = new AtomicReference<>();

    /**
     * Initializes the lineage emitter with the given configuration.
     * <p>
     * This method must be called before any emission methods are used. It sets up the
     * OpenLineage context and emitter if OpenLineage integration is enabled in the configuration.
     * The initialization is thread-safe and ensures the context is only created once.
     *
     * @param configuration The Debezium connector configuration containing OpenLineage settings
     * @param connName The name of the connector used for event attribution
     */
    public static void init(Configuration configuration, String connName) {

        DebeziumOpenLineageConfiguration debeziumOpenLineageConfiguration = DebeziumOpenLineageConfiguration.from(configuration);

        if (debeziumOpenLineageConfiguration.enabled()) {
            OpenLineageEventEmitter emitter = new OpenLineageEventEmitter(debeziumOpenLineageConfiguration);

            if (contextRef.get() == null) {
                OpenLineageContext ctx = new OpenLineageContext(
                        new OpenLineage(emitter.getProducer()),
                        debeziumOpenLineageConfiguration,
                        new OpenLineageJobIdentifier(debeziumOpenLineageConfiguration.job().namespace(),
                                configuration.getString(CONNECTOR_NAME_PROPERTY)));
                contextRef.compareAndSet(null, ctx);
            }

            lineageEmitter = new OpenLineageEmitter(connName, configuration, contextRef.get(), emitter);
        }
        else {
            lineageEmitter = new NoOpLineageEmitter();
        }
    }

    private static void checkInitialized() {
        if (lineageEmitter == null) {
            throw new IllegalStateException("DebeziumOpenLineageEmitter not initialized. Call init() first.");
        }
    }

    /**
     * Emits a lineage event for the given source task state.
     *
     * @param state The current state of the source task
     * @throws IllegalStateException If the emitter has not been initialized
     */
    public static void emit(BaseSourceTask.State state) {

        checkInitialized();
        lineageEmitter.emit(state);
    }

    /**
     * Emits a lineage event for the given source task state and exception.
     * <p>
     * This method is typically used for error reporting.
     *
     * @param state The current state of the source task
     * @param t The exception that occurred during processing
     * @throws IllegalStateException If the emitter has not been initialized
     */
    public static void emit(BaseSourceTask.State state, Throwable t) {

        checkInitialized();
        lineageEmitter.emit(state, null, t);
    }

    /**
     * Emits a lineage event for the given source task state and table event.
     * <p>
     * This method is typically used for emitting input dataset lineage.
     *
     * @param state The current state of the source task
     * @param event The table event containing metadata for lineage
     * @throws IllegalStateException If the emitter has not been initialized
     */
    public static void emit(BaseSourceTask.State state, Table event) {

        checkInitialized();
        lineageEmitter.emit(state, event, null);
    }

    /**
     * Emits a lineage event for the given source task state, table event, and exception.
     * <p>
     * This method provides the most detailed lineage information, including both table
     * metadata and any exception that occurred during processing.
     *
     * @param state The current state of the source task
     * @param event The table event containing metadata for lineage, may be {@code null}
     * @param t The exception that occurred during processing, may be {@code null}
     * @throws IllegalStateException If the emitter has not been initialized
     */
    public static void emit(BaseSourceTask.State state, Table event, Throwable t) {

        checkInitialized();
        lineageEmitter.emit(state, event, t);
    }
}
