/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.openlineage;

import java.util.List;
import java.util.ServiceLoader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.connector.common.BaseSourceTask;
import io.debezium.openlineage.dataset.DatasetMetadata;
import io.debezium.openlineage.emitter.LineageEmitter;
import io.debezium.openlineage.emitter.LineageEmitterFactory;
import io.debezium.openlineage.emitter.NoOpLineageEmitter;
import io.debezium.openlineage.emitter.OpenLineageEventEmitter;

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

    private static final Logger LOGGER = LoggerFactory.getLogger(DebeziumOpenLineageEmitter.class);
    private static LineageEmitter lineageEmitter;
    private static final ServiceLoader<LineageEmitterFactory> lineageEmitterFactory = ServiceLoader.load(LineageEmitterFactory.class);

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

        LOGGER.debug("Calling init for connector {} and config {}", connName, configuration);
        /*
         * Addresses native compilation issues with the Debezium Quarkus extension when OpenLineage
         * dependencies are scoped as 'provided'. In native builds, Quarkus performs comprehensive
         * classpath scanning and fails when referenced classes are unavailable at build time.
         * ServiceLoader registration signals to Quarkus that the implementation will be available
         * at runtime, allowing the native compilation to proceed successfully.
         */
        lineageEmitter = lineageEmitterFactory
                .stream()
                .findFirst()
                .map(ServiceLoader.Provider::get)
                .orElse((ignore, ignore2) -> new NoOpLineageEmitter())
                .get(configuration, connName);

        LOGGER.debug("Emitter instance {}", lineageEmitter);
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
        lineageEmitter.emit(state, List.of(), t);
    }

    /**
     * Emits a lineage inputDatasetMetadata for the given source task state and table inputDatasetMetadata.
     * <p>
     * This method is typically used for emitting input dataset lineage.
     *
     * @param state The current state of the source task
     * @param datasetMetadata A list of input dataset metadata containing metadata for lineage
     * @throws IllegalStateException If the emitter has not been initialized
     */
    public static void emit(BaseSourceTask.State state, List<DatasetMetadata> datasetMetadata) {

        checkInitialized();
        lineageEmitter.emit(state, datasetMetadata);
    }

    /**
     * Emits a lineage event for the given source task state, table event, and exception.
     * <p>
     * This method provides the most detailed lineage information, including both table
     * metadata and any exception that occurred during processing.
     *
     * @param state The current state of the source task
     * @param datasetMetadata A list of input dataset metadata containing metadata for lineage
     * @param t The exception that occurred during processing, may be {@code null}
     * @throws IllegalStateException If the emitter has not been initialized
     */
    public static void emit(BaseSourceTask.State state, List<DatasetMetadata> datasetMetadata, Throwable t) {

        checkInitialized();
        lineageEmitter.emit(state, datasetMetadata, t);
    }
}
