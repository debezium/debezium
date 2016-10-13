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
     * Selector which generates a separate topic name for each table, based on the FQN name of the table
     */
    TopicSelector TOPIC_PER_TABLE = tableId -> String.join(".", tableId.schema(), tableId.table());
    /**
     * Selector which generates a separate topic for an entire DB schema
     */
    TopicSelector TOPIC_PER_SCHEMA = TableId::schema;
    
    /**
     * Returns the name of the Kafka topic for a given table identifier
     * 
     * @param tableId the table identifier, never {@code null}
     * @return the name of the Kafka topic, never {@code null}
     */
    String topicNameFor(TableId tableId);
}
