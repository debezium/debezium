/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import java.util.function.Predicate;

import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.mapping.ColumnMappers;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.schema.TopicSelector;

public abstract class HistorizedRelationalDatabaseSchema extends RelationalDatabaseSchema {

    protected HistorizedRelationalDatabaseSchema(String serverName, TopicSelector<TableId> topicSelector,
            Predicate<TableId> tableFilter, Predicate<ColumnId> columnFilter, ColumnMappers columnMappers,
            TableSchemaBuilder schemaBuilder, boolean tableIdCaseInsensitive) {
        super(serverName, topicSelector, tableFilter, columnFilter, columnMappers, schemaBuilder, tableIdCaseInsensitive);
    }

    public abstract void applySchemaChange(SchemaChangeEvent schemaChange);

    public abstract void recover(OffsetContext offset);
}
