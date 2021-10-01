/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import io.debezium.relational.TableId;
import io.debezium.schema.TopicSelector;
import io.debezium.schema.TopicSelector.DataCollectionTopicNamer;

/**
 * The topic naming strategy based on connector configuration and table name
 *
 * @author Jiri Pechanec
 *
 */
public class SqlServerTopicSelector {

    public static TopicSelector<TableId> defaultSelector(SqlServerConnectorConfig connectorConfig) {
        DataCollectionTopicNamer<TableId> topicNamer;
        if (connectorConfig.isMultiPartitionModeEnabled()) {
            topicNamer = (tableId, prefix, delimiter) -> String.join(delimiter, prefix, tableId.catalog(), tableId.schema(), tableId.table());
        }
        else {
            topicNamer = (tableId, prefix, delimiter) -> String.join(delimiter, prefix, tableId.schema(), tableId.table());
        }

        return TopicSelector.defaultSelector(connectorConfig, topicNamer);
    }
}
