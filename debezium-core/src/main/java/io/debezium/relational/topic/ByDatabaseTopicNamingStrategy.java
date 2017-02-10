/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.topic;

import io.debezium.annotation.Immutable;

/**
 * A {@link TopicNamingStrategy} that maps all tables to topics named only for the database.
 * This topic never returns a null topic name.
 * 
 * @author Randall Hauch
 */
@Immutable
public class ByDatabaseTopicNamingStrategy implements TopicNamingStrategy {

    public static final String ALIAS = "database";

    @Override
    public String getTopic(String prefix, String databaseName, String tableName) {
        if (databaseName == null || databaseName.length() == 0) return prefix;
        return String.join(DELIMITER, prefix, databaseName);
    }

    @Override
    public String toString() {
        return getTopic("<prefix>", "<database>", "");
    }
}
