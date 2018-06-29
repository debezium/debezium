/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import java.util.function.Predicate;

import io.debezium.config.Configuration;
import io.debezium.schema.HistorizedDatabaseSchema;
import io.debezium.schema.TopicSelector;

public abstract class HistorizedRelationalDatabaseSchema extends RelationalDatabaseSchema implements HistorizedDatabaseSchema<TableId> {

    protected HistorizedRelationalDatabaseSchema(Configuration config, String serverName,
            TopicSelector<TableId> topicSelector, Predicate<TableId> tableFilter, Predicate<ColumnId> columnFilter,
            TableSchemaBuilder schemaBuilder, boolean tableIdCaseInsensitive) {
        super(config, serverName, topicSelector, tableFilter, columnFilter, schemaBuilder, tableIdCaseInsensitive);
    }
}
