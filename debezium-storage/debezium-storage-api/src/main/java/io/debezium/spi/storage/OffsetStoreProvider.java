/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.spi.storage;

import java.util.Map;

import io.debezium.common.annotation.Incubating;
import io.debezium.config.Configuration;

/**
 * SPI for providing {@link OffsetStore} implementations.
 * <p>
 * Implementations are discovered via {@link java.util.ServiceLoader} and should be registered
 * in {@code META-INF/services/io.debezium.spi.storage.OffsetStoreProvider}.
 * <p>
 * This allows pluggable offset storage backends for the embedded engine, making Kafka Connect
 * dependencies optional where appropriate.
 *
 * @author Debezium Authors
 */
@Incubating
public interface OffsetStoreProvider {

    /**
     * Returns the provider name used for configuration and logging.
     * <p>
     * Examples: "file", "memory", "kafka", "jdbc", "redis"
     *
     * @return the provider name, never null
     */
    String getName();

    /**
     * Creates and configures an {@link OffsetStore} instance.
     * <p>
     * The provider is responsible for any special initialization required by the backing store,
     * such as creating converters or setting up connection pools.
     * <p>
     * Note: The caller is responsible for calling {@link OffsetStore#configure(Configuration)}
     * and {@link OffsetStore#start()} on the returned instance.
     *
     * @param config the configuration
     * @param connectorConfig the connector-specific configuration
     * @return a configured offset backing store instance, never null
     */
    OffsetStore create(Configuration config, Map<String, String> connectorConfig);

    /**
     * Returns the fully qualified class name of the {@link OffsetStore} implementation
     * this provider creates.
     * <p>
     * This is used for backward compatibility, allowing configuration by class name to still work.
     * <p>
     * Default implementation returns null, indicating no class name mapping.
     *
     * @return the offset store class name, or null if not applicable
     */
    default String getOffsetStoreClassName() {
        return null;
    }
}
