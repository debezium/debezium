/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import io.debezium.relational.TableId;
import io.debezium.schema.TopicSelector;

/**
 * Factory for this connector's {@link TopicSelector}.
 *
 * @author Horia Chiorean (hchiorea@redhat.com)
 */
public class PostgresTopicSelector {

    public static TopicSelector<TableId> create(PostgresConnectorConfig connectorConfig) {
        return TopicSelector.defaultSelector(connectorConfig,
                (id, prefix, delimiter) -> String.join(delimiter, prefix, id.schema(), id.table()));
    }
}
