/*
 * Copyright 2015 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.mysql.ingest;

/**
 * A function that determines the name of a topic given the table name and database name.
 * 
 * @author Randall Hauch
 */
@FunctionalInterface
public interface TopicSelector {
    /**
     * Get the default topic selector logic, which simply concatenates the database name and topic name using a '.' delimiter
     * character.
     * 
     * @return the topic selector; never null
     */
    static TopicSelector defaultSelector() {
        return defaultSelector(".");
    }

    /**
     * Get the default topic selector logic, which simply concatenates the database name and topic name using the supplied
     * delimiter.
     * 
     * @param delimiter the string delineating the database name and table name; may not be null
     * @return the topic selector; never null
     */
    static TopicSelector defaultSelector(String delimiter) {
        return (databaseName, tableName) -> databaseName + delimiter + tableName;
    }

    /**
     * Get the name of the topic given the database and table names.
     * @param databaseName the name of the database; may not be null
     * @param tableName the name of the table; may not be null
     * @return the topic name; never null
     */
    String getTopic(String databaseName, String tableName);
}
