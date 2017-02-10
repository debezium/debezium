/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.topic;

import io.debezium.annotation.Immutable;

/**
 * A {@link TopicNamingStrategy} that maps each table to a different topic composed of a fixed prefix, the database name, and
 * table name. This topic never returns a null topic name.
 * 
 * @author Randall Hauch
 */
@Immutable
public class ByTableTopicNamingStrategy implements TopicNamingStrategy {
    
    public static final String ALIAS = "table";

    @Override
    public String getTopic(String prefix, String databaseName, String tableName) {
        if (databaseName == null || databaseName.length() == 0) return prefix;
        if (tableName == null || tableName.length() == 0) {
            return String.join(DELIMITER, prefix, databaseName);
        }
        return String.join(DELIMITER, prefix, databaseName, tableName);
    }

    @Override
    public String toString() {
        return getTopic("<prefix>", "<database>", "<tableName>");
    }
}
