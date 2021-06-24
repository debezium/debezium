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
public class SqlServerTopicSelector {

    public static TopicSelector<TableId> defaultSelector(SqlServerConnectorConfig connectorConfig) {
        return TopicSelector.defaultSelector(connectorConfig,
                (tableId, prefix, delimiter) -> String.join(delimiter, prefix, tableId.schema(), tableId.table()));
    }
}
