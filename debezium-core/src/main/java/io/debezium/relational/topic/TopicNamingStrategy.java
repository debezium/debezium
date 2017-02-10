/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.topic;

import io.debezium.annotation.ThreadSafe;
import io.debezium.relational.TableId;

/**
 * A function that determines the name of topics for databases and tables.
 * <p>
 * This is designed so that TopicNamingStrategies can be chained together. For example, the
 * {@link #getTopic(String, String, String)} and {@link #getTopic(String, TableId)} methods return null when the strategy
 * does not apply, and in this case a subsequent strategy can be used.
 * 
 * @author Randall Hauch
 */
@ThreadSafe
@FunctionalInterface
public interface TopicNamingStrategy {

    /**
     * The delimiter between the various segments in a topic name.
     */
    public static final String DELIMITER = ".";

    /**
     * Get the name of the topic given the specified topic name prefix, database name, and table name.
     * 
     * @param prefix the topic name prefix; may not be null
     * @param databaseName the name of the database; may be null or empty
     * @param tableName the name of the table; may be null or empty
     * @return the topic name; may be null if this strategy could not be applied
     */
    String getTopic(String prefix, String databaseName, String tableName);

    /**
     * Get the name of the topic given the specified topic name prefix and table identifier.
     * 
     * @param prefix the topic name prefix; may not be null or empty
     * @param tableId the identifier of the table; may be null
     * @return the topic name; may be null if this strategy could not be applied
     */
    default String getTopic(String prefix, TableId tableId) {
        return getTopic(prefix, tableId.catalog(), tableId.table());
    }

    /**
     * Get the name of the primary topic given the specified topic name prefix.
     * 
     * @param prefix the topic name prefix; may not be null
     * @return the topic name; never null
     */
    default String getPrimaryTopic(String prefix) {
        return prefix;
    }
}
