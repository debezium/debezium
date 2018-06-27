/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.schema;

/**
 * Implementations return names for Kafka topics (data and meta-data).
 *
 * @author Randal Hauch
 * @author Gunnar Morling
 *
 * @param <I>
 *            The type of {@link DataCollectionId} used by a given implementation
 */
// TODO: further unify; do we actually need distinct implementations per backend?
public interface TopicSelector<I extends DataCollectionId> {

    /**
     * Returns the name of the Kafka topic for a given data collection identifier
     *
     * @param id the data collection identifier, never {@code null}
     * @return the name of the Kafka topic, never {@code null}
     */
    String topicNameFor(I id);
}
