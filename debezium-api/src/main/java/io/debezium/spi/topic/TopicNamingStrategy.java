/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.spi.topic;

import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.common.annotation.Incubating;
import io.debezium.spi.schema.DataCollectionId;

/**
 * An interface that defines the topic naming strategy, including DataChange, SchemaChange, Transaction, Heartbeat events etc.
 *
 * @param <I>
 * @author Harvey Yue
 */
@Incubating
public interface TopicNamingStrategy<I extends DataCollectionId> {
    Logger LOGGER = LoggerFactory.getLogger(TopicNamingStrategy.class);

    String REPLACEMENT_CHAR = "_";

    void configure(Properties props);

    String dataChangeTopic(I id);

    String schemaChangeTopic();

    String heartbeatTopic();

    String transactionTopic();

    /**
     * Sanitize the given character whether is a legal character of a Kafka topic name.
     * Legal characters are {@code [a-zA-Z0-9._-]}.
     *
     * @link https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/common/internals/Topic.java
     */
    default String sanitizedTopicName(String topicName) {
        StringBuilder sanitizedNameBuilder = new StringBuilder(topicName.length());
        boolean changed = false;

        for (int i = 0; i < topicName.length(); i++) {
            char c = topicName.charAt(i);
            if (c == '.' || c == '_' || c == '-' || (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9')) {
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
}
