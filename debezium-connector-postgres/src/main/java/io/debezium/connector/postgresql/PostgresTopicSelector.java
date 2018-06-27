/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import io.debezium.relational.TableId;
import io.debezium.schema.TopicSelector;

/**
 * Generator of topic names for {@link io.debezium.relational.Table table ids} used by the Postgres connector to determine
 * which Kafka topics contain which messages
 *
 * @author Horia Chiorean (hchiorea@redhat.com)
 */
public interface PostgresTopicSelector extends TopicSelector<TableId> {

    public static PostgresTopicSelector create(PostgresConnectorConfig config) {
        PostgresConnectorConfig.TopicSelectionStrategy topicSelectionStrategy = config.topicSelectionStrategy();

        switch (topicSelectionStrategy) {
            case TOPIC_PER_SCHEMA:
                return topicPerSchema(config.serverName());
            case TOPIC_PER_TABLE:
                return topicPerTable(config.serverName());
            default:
                throw new IllegalArgumentException("Unknown topic selection strategy: " + topicSelectionStrategy);
        }
    }

    /**
     * Generates a topic name for each table, based on the table schema, table name and a prefix
     *
     * @param prefix a prefix which will be prepended to the topic name
     * @return a {@link TopicSelector} instance, never {@code null}
     */
    static PostgresTopicSelector topicPerTable(String prefix) {
        return tableId -> String.join(".", prefix, tableId.schema(), tableId.table());
    }

    /**
     * Generates a topic name for each table, based on the table schema and a prefix
     *
     * @param prefix a prefix which will be prepended to the topic name
     * @return a {@link TopicSelector} instance, never {@code null}
     */
    static PostgresTopicSelector topicPerSchema(String prefix) {
        return tableId -> String.join(".", prefix, tableId.schema());
    }
}
