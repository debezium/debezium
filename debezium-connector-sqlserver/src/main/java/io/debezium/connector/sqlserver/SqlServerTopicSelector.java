/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import io.debezium.relational.TableId;
import io.debezium.schema.TopicSelector;

/**
 * The topic naming strategy based on connector configuration and table name
 *
 * @author Jiri Pechanec
 *
 */
public class SqlServerTopicSelector implements TopicSelector<TableId> {

    private final String prefix;

    public SqlServerTopicSelector(String prefix) {
        this.prefix = prefix;
    }

    public static SqlServerTopicSelector defaultSelector(String prefix) {
        return new SqlServerTopicSelector(prefix);
    }

    @Override
    public String topicNameFor(TableId tableId) {
        return String.join(".", prefix, tableId.schema(), tableId.table());
    }
}
