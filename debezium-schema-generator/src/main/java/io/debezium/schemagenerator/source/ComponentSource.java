/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.schemagenerator.source;

import java.util.List;

import io.debezium.metadata.ComponentMetadata;

/**
 * Strategy interface for discovering component metadata from different sources.
 *
 * <p>Implementations include:
 * <ul>
 *   <li>{@link DebeziumComponentSource} - Discovers Debezium components via ServiceLoader</li>
 *   <li>Future: KafkaConnectComponentSource - Discovers Kafka Connect components via Jandex</li>
 * </ul>
 *
 * <p>This interface follows the Strategy pattern, allowing the SchemaGenerator to work
 * with different component discovery mechanisms without coupling to specific implementations.
 *
 * @see DebeziumComponentSource
 */
public interface ComponentSource {

    /**
     * Discovers all component metadata from this source.
     *
     * @return list of discovered components, never null (may be empty)
     */
    List<ComponentMetadata> discoverComponents();

    /**
     * Returns a human-readable name for this source, used in logging.
     *
     * @return the source name
     */
    String getName();
}
