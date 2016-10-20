/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.topic;

import io.debezium.annotation.ThreadSafe;
import io.debezium.relational.TableId;

/**
 * A function that determines the name of topics for data and metadata.
 * 
 * @author Randall Hauch
 */
@ThreadSafe
public interface TopicSelector {

    /**
     * Get the name of the topic for the given server name.
     * 
     * @param tableId the identifier of the table; may not be null
     * @return the topic name; never null
     */
    default String getTopic(TableId tableId) {
        return getTopic(tableId.catalog(),tableId.table());
    }

    /**
     * Get the name of the topic for the given server name.
     * 
     * @param databaseName the name of the database; may not be null
     * @param tableName the name of the table; may not be null
     * @return the topic name; never null
     */
    String getTopic(String databaseName, String tableName);

    /**
     * Get the name of the primary topic.
     * 
     * @return the topic name; never null
     */
    String getPrimaryTopic();
}
