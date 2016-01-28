/*
 * Copyright Debezium Authors.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.mysql.source;

import io.debezium.annotation.ThreadSafe;

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
     * @return the topic selector; never null
     */
    static TopicSelector defaultSelector() {
        return defaultSelector(".");
    }

    /**
     * Get the default topic selector logic, which uses the supplied delimiter character when needed.
     * 
     * @param delimiter the string delineating the server, database, and table names; may not be null
     * @return the topic selector; never null
     */
    static TopicSelector defaultSelector(String delimiter) {
        return new TopicSelector() {
            /**
             * Get the name of the topic for the given server, database, and table names. This method returns
             * "{@code <serverName>}".
             * 
             * @param serverName the name of the database server; may not be null
             * @return the topic name; never null
             */
            @Override
            public String getTopic(String serverName) {
                return serverName;
            }
            /**
             * Get the name of the topic for the given server name. This method returns
             * "{@code <serverName>.<databaseName>.<tableName>}".
             * 
             * @param serverName the name of the database server; may not be null
             * @param databaseName the name of the database; may not be null
             * @param tableName the name of the table; may not be null
             * @return the topic name; never null
             */
            @Override
            public String getTopic(String serverName, String databaseName, String tableName) {
                return String.join(delimiter, serverName, databaseName, tableName);
            }
        };
    }

    /**
     * Get the name of the topic for the given server name.
     * 
     * @param serverName the name of the database server; may not be null
     * @param databaseName the name of the database; may not be null
     * @param tableName the name of the table; may not be null
     * @return the topic name; never null
     */
    String getTopic(String serverName, String databaseName, String tableName);

    /**
     * Get the name of the topic for the given server, database, and table names.
     * 
     * @param serverName the name of the database server; may not be null
     * @return the topic name; never null
     */
    String getTopic(String serverName);
}
