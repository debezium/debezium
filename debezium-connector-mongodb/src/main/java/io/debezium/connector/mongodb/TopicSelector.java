/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.util.StringJoiner;

import io.debezium.annotation.ThreadSafe;

/**
 * A function that determines the name of topics for data and metadata.
 * 
 * @author Randall Hauch
 */
@ThreadSafe
@FunctionalInterface
public interface TopicSelector {
    /**
     * Get the name of the topic for a given database name and collection name. This method returns
     * "{@code <dbName>.<collectionName>}".
     * 
     * @return the topic selector; never null
     */
    static TopicSelector defaultSelector() {
        return defaultSelector(null,".");
    }

    /**
     * Get the name of the topic for a given prefix, database name, and collection name. This method returns
     * "{@code <prefix>.<dbName>.<collectionName>}", and does not use the replica set name.
     * 
     * @param prefix the prefix; be null or empty if no prefix is needed
     * @return the topic selector; never null
     */
    static TopicSelector defaultSelector(String prefix) {
        return defaultSelector(prefix,".");
    }

    /**
     * Get the name of the topic for a given prefix, database name, and collection name. This method returns
     * "{@code <prefix><delimiter><dbName><delimiter><collectionName>}", and does not use the replica set name.
     * 
     * @param delimiter the string delineating the prefix, database, and collection names; may not be null
     * @param prefix the prefix; be null or empty if no prefix is needed
     * @return the topic selector; never null
     */
    static TopicSelector defaultSelector(String prefix, String delimiter) {
        if (prefix != null && prefix.trim().length() > 0) {
            String trimmedPrefix = prefix.trim();
            return (collectionId) -> {
                StringJoiner sb = new StringJoiner(delimiter);
                sb.add(trimmedPrefix);
                sb.add(collectionId.dbName());
                sb.add(collectionId.name());
                return sb.toString();
            };
        }
        return (collectionId) -> {
            StringJoiner sb = new StringJoiner(delimiter);
            sb.add(collectionId.dbName());
            sb.add(collectionId.name());
            return sb.toString();
        };
    }

    /**
     * Get the name of the topic for the given server name and database name.
     * 
     * @param collectionId the identifier of the collection for which records are to be produced; may not be null
     * @return the topic name; never null
     */
    String getTopic(CollectionId collectionId);
}
