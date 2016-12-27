/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import io.debezium.relational.TableId;

/**
 * Generator of topic names for {@link io.debezium.relational.Table table ids} used by the Postgres connector to determine
 * which Kafka topics contain which messages
 * 
 * @author Horia Chiorean (hchiorea@redhat.com)
 */
public interface TopicSelector {
            
    /**
     * Generates a topic name for each table, based on the table schema, table name and a prefix
     *
     * @param prefix a prefix which will be prepended to the topic name
     * @return a {@link TopicSelector} instance, never {@code null}
     */
    static TopicSelector topicPerTable(String prefix) {
        return tableId -> String.join(".", prefix, tableId.schema(), tableId.table());     
    }
     
    /**
     * Generates a topic name for each table, based on the table schema and a prefix
     *
     * @param prefix a prefix which will be prepended to the topic name
     * @return a {@link TopicSelector} instance, never {@code null}
     */
    static TopicSelector topicPerSchema(String prefix) {
        return tableId -> String.join(".", prefix, tableId.schema());
    }
    
    /**
     * Returns the name of the Kafka topic for a given table identifier
     * 
     * @param tableId the table identifier, never {@code null}
     * @return the name of the Kafka topic, never {@code null}
     */
    String topicNameFor(TableId tableId);
}
