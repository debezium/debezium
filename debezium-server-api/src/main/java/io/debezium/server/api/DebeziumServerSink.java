/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.api;

import java.util.Map;

import io.debezium.common.annotation.Incubating;
import io.debezium.metadata.ComponentMetadataProvider;
import io.debezium.metadata.ConfigDescriptor;
import io.debezium.server.ConnectionValidationResult;

/**
 * Common interface for all Debezium Server sink implementations.
 * <p>
 * This interface extends {@link ConfigDescriptor} to provide configuration metadata
 * and defines common lifecycle methods and operations for sink implementations.
 * <p>
 * The {@link DefaultChangeConsumer} wrapper manages the sink lifecycle in this order:
 * <ol>
 *   <li>{@link #validateConnection(Map)} - Validates sink connectivity (optional)</li>
 *   <li>{@link #configure(org.eclipse.microprofile.config.Config)} - Configures and initializes the sink</li>
 *   <li>Batch handling - Processes change events</li>
 *   <li>{@link #close()} - Cleans up resources on shutdown</li>
 * </ol>
 *
 */
@Incubating
public interface DebeziumServerSink extends ConfigDescriptor, ComponentMetadataProvider {

    /**
     * Configures the sink by loading configuration and setting up required resources.
     * This method is called by {@link DefaultChangeConsumer} after {@link #validateConnection(Map)}
     * and before any batch processing begins.
     * <p>
     * Implementations should:
     * <ul>
     *   <li>Load and validate configuration from the provided config</li>
     *   <li>Instantiate and configure the sink client</li>
     *   <li>Establish connections to the sink destination</li>
     *   <li>Initialize any required resources</li>
     * </ul>
     * After this method completes, the sink must be ready to handle batches.
     *
     * @param config the MicroProfile configuration
     * @throws Exception if configuration or initialization fails
     */
    default void configure(org.eclipse.microprofile.config.Config config) throws Exception {
        // Default implementation does nothing - sinks override this method
    }

    /**
     * Closes the sink and releases all resources.
     * This method is called by {@link DefaultChangeConsumer} during shutdown.
     * <p>
     * Implementations should:
     * <ul>
     *   <li>Close connections to the sink destination</li>
     *   <li>Release any allocated resources</li>
     *   <li>Clean up temporary state</li>
     * </ul>
     */
    default void close() {
        // Default implementation does nothing - sinks override this method
    }

    /**
     * Validates the connection to the sink destination.
     * This method is called by {@link DefaultChangeConsumer} before {@link #configure(org.eclipse.microprofile.config.Config)}
     * to verify that the sink is accessible and properly configured.
     * <p>
     * This should be a lightweight check that does not perform full initialization.
     * Implementations can verify connectivity, credentials, or other prerequisites
     * without instantiating the full sink client.
     *
     * @param config the configuration to validate
     * @return validation result indicating success or failure with details
     * @throws Exception if the connection validation fails
     */
    default ConnectionValidationResult validateConnection(Map<String, Object> config) throws Exception {
        // Default implementation does nothing - sinks can override if they support validation
        return ConnectionValidationResult.successful();
    }
}
