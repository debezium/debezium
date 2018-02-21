/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

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
     * Get the default topic selector logic, which uses a '.' delimiter character when needed.
     *
     * @param prefix the name of the prefix to be used for all topics; may not be null and must not terminate in the
     *            {@code delimiter}
     * @param heartbeatPrefix the name of the prefix to be used for all heartbeat topics; may not be null and must not terminate in the
     *            {@code delimiter}
     * @return the topic selector; never null
     */
    static TopicSelector defaultSelector(String prefix, String heartbeatPrefix) {
        return defaultSelector(prefix, heartbeatPrefix, ".");
    }

    /**
     * Get the default topic selector logic, which uses the supplied delimiter character when needed.
     *
     * @param prefix the name of the prefix to be used for all topics; may not be null and must not terminate in the
     *            {@code delimiter}
     * @param heartbeatPrefix - a prefix that will be used for heartbeat topics. All heartbeat topics will start with this prefix and will use
     *            {@code delimiter} to separate the heartbeat prefix and the rest of the name
     * @param delimiter the string delineating the server, database, and table names; may not be null
     * @return the topic selector; never null
     */
    static TopicSelector defaultSelector(String prefix, String heartbeatPrefix, String delimiter) {
        return new TopicSelector() {
            /**
             * Get the name of the topic for the given server, database, and table names. This method returns
             * "{@code <serverName>}".
             *
             * @return the topic name; never null
             */
            @Override
            public String getPrimaryTopic() {
                return prefix;
            }

            /**
             * Get the name of the topic for the given server name. This method returns
             * "{@code <prefix>.<databaseName>.<tableName>}".
             *
             * @param databaseName the name of the database; may not be null
             * @param tableName the name of the table; may not be null
             * @return the topic name; never null
             */
            @Override
            public String getTopic(String databaseName, String tableName) {
                return String.join(delimiter, prefix, databaseName, tableName);
            }

            /**
             * Get the name of the heartbeat topic for the given server. This method returns
             * "{@code <prefix>-heartbeat}".
             *
             * @return the topic name; never null
             */
            @Override
            public String getHeartbeatTopic() {
                return String.join(delimiter, heartbeatPrefix, prefix);
            }

        };
    }

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

    /**
     * Get the name of the heartbeat topic.
     *
     * @return the topic name; never null
     */
    String getHeartbeatTopic();
}
