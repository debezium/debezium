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

    private final SqlServerPartition partition;
    private final SqlServerOffsetContext offsetContext;
    private final SqlServerChangeTable changeTable;
    private final Table tableSchema;
    private final SqlServerDatabaseSchema schema;
    private final SchemaChangeEventType eventType;

    public SqlServerSchemaChangeEventEmitter(SqlServerPartition partition,
                                             SqlServerOffsetContext offsetContext,
                                             SqlServerChangeTable changeTable,
                                             Table tableSchema,
                                             SqlServerDatabaseSchema schema,
                                             SchemaChangeEventType eventType) {
        this.partition = partition;
        this.offsetContext = offsetContext;
        this.changeTable = changeTable;
        this.tableSchema = tableSchema;
        this.schema = schema;
        this.eventType = eventType;
    }

    @Override
    public void emitSchemaChangeEvent(Receiver receiver) throws InterruptedException {
        final SchemaChangeEvent event = SchemaChangeEvent.of(
                eventType,
                partition,
                offsetContext,
                changeTable.getSourceTableId().catalog(),
                changeTable.getSourceTableId().schema(),
                "N/A",
                tableSchema,
                false);

        if (!schema.skipSchemaChangeEvent(event)) {
            receiver.schemaChangeEvent(event);
        }
    }
}
