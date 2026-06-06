/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.schema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.ThreadSafe;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.util.BoundedConcurrentHashMap;
import io.debezium.util.BoundedConcurrentHashMap.Eviction;

/**
 * Implementations return names for Kafka topics (data and meta-data).
 *
 * @author Randal Hauch
 * @author Gunnar Morling
 *
 * @param <I>
 *            The type of {@link DataCollectionId} used by a given implementation
 * @deprecated Use {@link io.debezium.spi.topic.TopicNamingStrategy} instead.
 */
@Deprecated
public class TopicSelector<I extends DataCollectionId> {

    private final String prefix;
    private final String heartbeatPrefix;
    private final String delimiter;
    private final DataCollectionTopicNamer<I> dataCollectionTopicNamer;

    private TopicSelector(String prefix, String heartbeatPrefix, String delimiter, DataCollectionTopicNamer<I> dataCollectionTopicNamer) {
        this.prefix = prefix;
        this.heartbeatPrefix = heartbeatPrefix;
        this.delimiter = delimiter;
        this.dataCollectionTopicNamer = new TopicNameCache<>(new TopicNameSanitizer<>(dataCollectionTopicNamer));
    }

    public static <I extends DataCollectionId> TopicSelector<I> defaultSelector(String prefix, String heartbeatPrefix, String delimiter,
                                                                                DataCollectionTopicNamer<I> dataCollectionTopicNamer) {
        return new TopicSelector<>(prefix, heartbeatPrefix, delimiter, dataCollectionTopicNamer);
    }

    public static <I extends DataCollectionId> TopicSelector<I> defaultSelector(CommonConnectorConfig connectorConfig,
                                                                                DataCollectionTopicNamer<I> dataCollectionTopicNamer) {
        String prefix = connectorConfig.getLogicalName();
        String heartbeatTopicsPrefix = connectorConfig.getHeartbeatTopicsPrefix();
        String delimiter = ".";

        return defaultSelector(prefix, heartbeatTopicsPrefix, delimiter, dataCollectionTopicNamer);
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

    /**
     * A topic namer that replaces any characters invalid in a topic name with {@code _}.
     */
    private static class TopicNameSanitizer<I extends DataCollectionId> implements DataCollectionTopicNamer<I> {

        private static final Logger LOGGER = LoggerFactory.getLogger(TopicNameSanitizer.class);

        private static final String REPLACEMENT_CHAR = "_";

        private final DataCollectionTopicNamer<I> delegate;

        TopicNameSanitizer(DataCollectionTopicNamer<I> delegate) {
            this.delegate = delegate;
        }

        @Override
        public String topicNameFor(I id, String prefix, String delimiter) {
            String topicName = delegate.topicNameFor(id, prefix, delimiter);

            StringBuilder sanitizedNameBuilder = new StringBuilder(topicName.length());
            boolean changed = false;

            for (int i = 0; i < topicName.length(); i++) {
                char c = topicName.charAt(i);
                if (isValidTopicNameCharacter(c)) {
                    sanitizedNameBuilder.append(c);
                }
                else {
                    sanitizedNameBuilder.append(REPLACEMENT_CHAR);
                    changed = true;
                }
            }

            if (changed) {
                String sanitizedName = sanitizedNameBuilder.toString();
                LOGGER.warn("Topic '{}' name isn't a valid topic name, replacing it with '{}'.", topicName, sanitizedName);

                return sanitizedName;
            }
            else {
                return topicName;
            }
        }

        /**
         * Whether the given character is a legal character of a Kafka topic name. Legal characters are
         * {@code [a-zA-Z0-9._-]}.
         *
         * @link https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/common/internals/Topic.java
         */
        private boolean isValidTopicNameCharacter(char c) {
            return c == '.' || c == '_' || c == '-' || (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9');
        }
    }

    /**
     * A topic namer that caches names it has obtained from a delegate.
     */
    @ThreadSafe
    private static class TopicNameCache<I extends DataCollectionId> implements DataCollectionTopicNamer<I> {

        private final BoundedConcurrentHashMap<I, String> topicNames;
        private final DataCollectionTopicNamer<I> delegate;

        TopicNameCache(DataCollectionTopicNamer<I> delegate) {
            this.topicNames = new BoundedConcurrentHashMap<>(10_000, 10, Eviction.LRU);
            this.delegate = delegate;
        }

        @Override
        public String topicNameFor(I id, String prefix, String delimiter) {
            return topicNames.computeIfAbsent(id, i -> delegate.topicNameFor(i, prefix, delimiter));
        }
    }
}
