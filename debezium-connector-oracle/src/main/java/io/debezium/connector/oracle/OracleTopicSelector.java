/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import io.debezium.relational.TableId;
import io.debezium.schema.TopicSelector;

/**
 * @deprecated Use {@link io.debezium.schema.SchemaTopicNamingStrategy} instead.
 */
@Deprecated
public class OracleTopicSelector {

    public static TopicSelector<TableId> defaultSelector(OracleConnectorConfig connectorConfig) {
        return TopicSelector.defaultSelector(connectorConfig,
                (tableId, prefix, delimiter) -> String.join(delimiter, prefix, tableId.schema(), tableId.table()));
    }
}
