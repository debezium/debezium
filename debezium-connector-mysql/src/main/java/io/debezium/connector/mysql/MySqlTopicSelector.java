/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import io.debezium.annotation.ThreadSafe;
import io.debezium.relational.TableId;
import io.debezium.schema.TopicSelector;

/**
 * Factory for this connector's {@link TopicSelector}.
 *
 * @author Randall Hauch
 * @deprecated Use {@link io.debezium.schema.DefaultTopicNamingStrategy} instead.
 */
@Deprecated
@ThreadSafe
public class MySqlTopicSelector {

    /**
     * Get the default topic selector logic, which uses a '.' delimiter character when needed.
     *
     * @param prefix the name of the prefix to be used for all topics; may not be null and must not terminate in the
     *            {@code delimiter}
     * @param heartbeatPrefix the name of the prefix to be used for all heartbeat topics; may not be null and must not terminate in the
     *            {@code delimiter}
     * @return the topic selector; never null
     */
    public static TopicSelector<TableId> defaultSelector(String prefix, String heartbeatPrefix) {
        return TopicSelector.defaultSelector(prefix, heartbeatPrefix, ".",
                (t, pref, delimiter) -> String.join(delimiter, pref, t.catalog(), t.table()));
    }

    public static TopicSelector<TableId> defaultSelector(MySqlConnectorConfig connectorConfig) {
        return TopicSelector.defaultSelector(connectorConfig,
                (tableId, prefix, delimiter) -> String.join(delimiter, prefix, tableId.catalog(), tableId.table()));
    }
}
