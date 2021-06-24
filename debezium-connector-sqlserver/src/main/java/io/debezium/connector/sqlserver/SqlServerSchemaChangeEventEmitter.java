/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import io.debezium.pipeline.spi.SchemaChangeEventEmitter;
import io.debezium.relational.Table;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.schema.SchemaChangeEvent.SchemaChangeEventType;

/**
 * {@link SchemaChangeEventEmitter} implementation based on SQL Server.
 *
 * @author Jiri Pechanec
 */
public class SqlServerSchemaChangeEventEmitter implements SchemaChangeEventEmitter {

    private final SqlServerOffsetContext offsetContext;
    private final SqlServerChangeTable changeTable;
    private final Table tableSchema;
    private final SchemaChangeEventType eventType;

    public SqlServerSchemaChangeEventEmitter(SqlServerOffsetContext offsetContext, SqlServerChangeTable changeTable, Table tableSchema, SchemaChangeEventType eventType) {
        this.offsetContext = offsetContext;
        this.changeTable = changeTable;
        this.tableSchema = tableSchema;
        this.eventType = eventType;
    }

    @Override
    public void emitSchemaChangeEvent(Receiver receiver) throws InterruptedException {
        final SchemaChangeEvent event = new SchemaChangeEvent(
                offsetContext.getPartition(),
                offsetContext.getOffset(),
                offsetContext.getSourceInfo(),
                changeTable.getSourceTableId().catalog(),
                changeTable.getSourceTableId().schema(),
                "N/A",
                tableSchema,
                eventType,
                false);

        receiver.schemaChangeEvent(event);
    }
}
