/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.schema;

import io.debezium.config.CommonConnectorConfig;

/**
 * Implementations return names for Kafka topics (data and meta-data).
 *
 * @author Randal Hauch
 * @author Gunnar Morling
 *
 * @param <I>
 *            The type of {@link DataCollectionId} used by a given implementation
 */
public class TopicSelector<I extends DataCollectionId> {

    private final String prefix;
    private final String heartbeatPrefix;
    private final String delimiter;
    private final DataCollectionTopicNamer<I> dataCollectionTopicNamer;

    private TopicSelector(String prefix, String heartbeatPrefix, String delimiter, DataCollectionTopicNamer<I> dataCollectionTopicNamer) {
        this.prefix = prefix;
        this.heartbeatPrefix = heartbeatPrefix;
        this.delimiter = delimiter;
        this.dataCollectionTopicNamer = dataCollectionTopicNamer;
    }

    public static <I extends DataCollectionId> TopicSelector<I> defaultSelector(String prefix, String heartbeatPrefix, String delimiter, DataCollectionTopicNamer<I> dataCollectionTopicNamer) {
        return new TopicSelector<>(prefix, heartbeatPrefix, delimiter, dataCollectionTopicNamer);
    }
    public static <I extends DataCollectionId> TopicSelector<I> defaultSelector(CommonConnectorConfig connectorConfig, DataCollectionTopicNamer<I> dataCollectionTopicNamer) {
        String prefix = connectorConfig.getLogicalName();
        String heartbeatTopicsPrefix = connectorConfig.getHeartbeatTopicsPrefix();
        String delimiter = ".";

        return new TopicSelector<>(prefix, heartbeatTopicsPrefix, delimiter, dataCollectionTopicNamer);
    }

    /**
     * Returns the name of the Kafka topic for a given data collection identifier
     *
     * @param id the data collection identifier, never {@code null}
     * @return the name of the Kafka topic, never {@code null}
     */
    public String topicNameFor(I id) {
        return dataCollectionTopicNamer.topicNameFor(id, prefix, delimiter);
    }

    /**
     * Get the name of the primary topic.
     *
     * @return the topic name; never null
     */
    public String getPrimaryTopic() {
        return prefix;
    }

    /**
     * Get the name of the heartbeat topic.
     *
     * @return the topic name; never null
     */
    /**
     * Get the name of the heartbeat topic for the given server. This method returns
     * "{@code <prefix>-heartbeat}".
     *
     * @return the topic name; never null
     */
    public String getHeartbeatTopic() {
        return String.join(delimiter, heartbeatPrefix, prefix);
    }

    /**
     * Implementations determine the topic name corresponding to a given data collection.
     */
    @FunctionalInterface
    public interface DataCollectionTopicNamer<I extends DataCollectionId> {
        String topicNameFor(I id, String prefix, String delimiter);
    }
}
