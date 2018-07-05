/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import io.debezium.relational.TableId;
import io.debezium.schema.TopicSelector;

public class OracleTopicSelector implements TopicSelector<TableId> {

    private final String prefix;

    private OracleTopicSelector(String prefix) {
        this.prefix = prefix;
    }

    public static OracleTopicSelector defaultSelector(String prefix) {
        return new OracleTopicSelector(prefix);
    }

    @Override
    public String topicNameFor(TableId tableId) {
        return String.join(".", prefix, tableId.schema(), tableId.table());
    }
}
