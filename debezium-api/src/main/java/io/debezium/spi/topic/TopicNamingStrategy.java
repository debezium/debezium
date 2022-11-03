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

    int MAX_NAME_LENGTH = 249;

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

        String sanitizedName = sanitizedNameBuilder.toString();
        if (sanitizedName.length() > MAX_NAME_LENGTH) {
            sanitizedName = sanitizedName.substring(0, MAX_NAME_LENGTH);
            changed = true;
        }
        else if (sanitizedName.equals(".")) {
            sanitizedName = "_";
            changed = true;
        }
        else if (sanitizedName.equals("..")) {
            sanitizedName = "__";
            changed = true;
        }

        if (changed) {
            LOGGER.warn("Topic '{}' name isn't a valid topic name, replacing it with '{}'.", topicName, sanitizedName);

            return sanitizedName;
        }
        else {
            return topicName;
        }
    }

    default TopicSchemaAugment keySchemaAugment() {
        return NO_SCHEMA_OP;
    }

    default TopicValueAugment keyValueAugment() {
        return NO_VALUE_OP;
    }

    /**
     * An interface that augments the field to topic key/value schema.
     * @param <S> The schema builder
     */
    interface TopicSchemaAugment<S> {
        /**
         * Augment field to schema.
         * @param schemaBuilder the schema builder
         * @return {@code true} if augment the field to schema, or {@code false} otherwise
         */
        boolean augment(S schemaBuilder);
    }

    /**
     * An interface that augments the field's value to topic key/value.
     * @param <S> The schema of topic key/value
     * @param <R> The struct represents the value of topic key/value
     */
    interface TopicValueAugment<I extends DataCollectionId, S, R> {
        /**
         * Augment value to the struct.
         * @param id The data collection id, for example: TableId, CollectionId
         * @param schema The schema which may contain the augmented field
         * @param struct The struct represents the value
         * @return {@code true} if augment the field's value to struct, or {@code false} otherwise
         */
        boolean augment(I id, S schema, R struct);
    }

    TopicSchemaAugment NO_SCHEMA_OP = schemaBuilder -> false;

    TopicValueAugment NO_VALUE_OP = (id, schema, struct) -> false;
}
