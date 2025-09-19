/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.openlineage;

import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.common.DebeziumTaskState;
import io.debezium.openlineage.dataset.DatasetMetadata;
import io.debezium.openlineage.emitter.LineageEmitter;
import io.debezium.openlineage.emitter.LineageEmitterFactory;
import io.debezium.openlineage.emitter.NoOpLineageEmitter;

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
 *
 * @author Mario Fiore Vitale
 */
public class DebeziumOpenLineageEmitter {

    private static final Logger LOGGER = LoggerFactory.getLogger(DebeziumOpenLineageEmitter.class);
    private static final ServiceLoader<LineageEmitterFactory> lineageEmitterFactory = ServiceLoader.load(LineageEmitterFactory.class);
    private static final Object SERVICE_LOADER_LOCK = new Object();

    // Thread-safe map to store emitters per connector
    private static final ConcurrentHashMap<String, LineageEmitter> emitters = new ConcurrentHashMap<>();

    /**
     * Initializes the lineage emitter with the given configuration for a specific connector.
     * <p>
     * This method must be called before any emission methods are used. It sets up the
     * OpenLineage context and emitter if OpenLineage integration is enabled in the configuration.
     * The initialization is thread-safe and ensures each connector has its own emitter instance.
     *
     * @param configuration The Debezium connector configuration containing OpenLineage settings
     * @param connectorTypeName The name of the connector used for event attribution
     */
    public static void init(Map<String, String> configuration, String connectorTypeName) {

        LOGGER.debug("Calling init for connector {} and config {}", connectorTypeName, configuration);

        ConnectorContext connectorContext = ConnectorContext.from(configuration, connectorTypeName);
        init(connectorContext);
    }

    /**
     * Initializes the lineage emitter with the given ConnectorContext.
     * <p>
     * This method must be called before any emission methods are used. It sets up the
     * emitter if OpenLineage integration is enabled in the configuration.
     * The initialization is thread-safe and ensures each connector has its own emitter instance.
     *
     * @param connectorContext The connector context
     */
    public static void init(ConnectorContext connectorContext) {

        LOGGER.debug("Calling init for connector with context {}", connectorContext);

        LineageEmitter emitter = emitters.computeIfAbsent(connectorContext.toEmitterKey(), key -> {
            LOGGER.debug("Creating new emitter for connector with name {}", key);
            /*
             * lineageEmitterFactory addresses native compilation issues with the Debezium Quarkus extension when OpenLineage
             * dependencies are scoped as 'provided'. In native builds, Quarkus performs comprehensive
             * classpath scanning and fails when referenced classes are unavailable at build time.
             * ServiceLoader registration signals to Quarkus that the implementation will be available
             * at runtime, allowing the native compilation to proceed successfully.
             */
            synchronized (SERVICE_LOADER_LOCK) { // This required for connectors with multiple tasks because the ServiceLoader is not thread-safe
                return lineageEmitterFactory
                        .stream()
                        .findFirst()
                        .map(ServiceLoader.Provider::get)
                        .orElse((ignore) -> new NoOpLineageEmitter())
                        .get(connectorContext);
            }
        });

        LOGGER.debug("Emitter instance for connector {}: {}", connectorContext.connectorName(), emitter);
    }

    /**
     * Removes the emitter for the specified connector.
     * Should be called when a connector is stopped or destroyed.
     *
     * @param connectorContext The context the connector
     */
    public static void cleanup(ConnectorContext connectorContext) {
        LineageEmitter removed = emitters.remove(connectorContext.toEmitterKey());
        if (removed != null) {
            LOGGER.debug("Cleaned up emitter for connector {}", connectorContext);
        }
    }

    public static ConnectorContext connectorContext(Map<String, String> config, String connectorName) {
        return ConnectorContext.from(config, connectorName);
    }

    /**
     * Emits a lineage event for the given source task state.
     *
     * @param connectorContext The connector context
     * @param state The current state of the source task
     * @throws IllegalStateException If the emitter has not been initialized for this connector
     */
    public static void emit(ConnectorContext connectorContext, DebeziumTaskState state) {
        getEmitter(connectorContext).emit(state);
    }

    /**
     * Emits a lineage event for the given source task state and exception.
     * <p>
     * This method is typically used for error reporting.
     *
     * @param connectorContext The connector context
     * @param state The current state of the source task
     * @param t The exception that occurred during processing
     * @throws IllegalStateException If the emitter has not been initialized for this connector
     */
    public static void emit(ConnectorContext connectorContext, DebeziumTaskState state, Throwable t) {
        getEmitter(connectorContext).emit(state, List.of(), t);
    }

    /**
     * Emits a lineage inputDatasetMetadata for the given source task state and table inputDatasetMetadata.
     * <p>
     * This method is typically used for emitting input dataset lineage.
     *
     * @param connectorContext The connector context
     * @param state The current state of the source task
     * @param datasetMetadata A list of input dataset metadata containing metadata for lineage
     * @throws IllegalStateException If the emitter has not been initialized for this connector
     */
    public static void emit(ConnectorContext connectorContext, DebeziumTaskState state, List<DatasetMetadata> datasetMetadata) {
        getEmitter(connectorContext).emit(state, datasetMetadata);
    }

    /**
     * Emits a lineage event for the given source task state, table event, and exception.
     * <p>
     * This method provides the most detailed lineage information, including both table
     * metadata and any exception that occurred during processing.
     *
     * @param connectorContext The connector context
     * @param state The current state of the source task
     * @param datasetMetadata A list of input dataset metadata containing metadata for lineage
     * @param t The exception that occurred during processing, may be {@code null}
     * @throws IllegalStateException If the emitter has not been initialized for this connector
     */
    public static void emit(ConnectorContext connectorContext, DebeziumTaskState state, List<DatasetMetadata> datasetMetadata, Throwable t) {
        getEmitter(connectorContext).emit(state, datasetMetadata, t);
    }

    private static LineageEmitter getEmitter(ConnectorContext connectorContext) {
        LineageEmitter emitter = emitters.get(connectorContext.toEmitterKey());
        LOGGER.debug("Available emitters {}", emitters);
        if (emitter == null) {
            throw new IllegalStateException("DebeziumOpenLineageEmitter not initialized for connector " + connectorContext + ". Call init() first.");
        }
        return emitter;
    }
}
