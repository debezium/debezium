/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2;

import io.debezium.pipeline.spi.SchemaChangeEventEmitter;
import io.debezium.relational.Table;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.schema.SchemaChangeEvent.SchemaChangeEventType;

/**
 * {@link SchemaChangeEventEmitter} implementation based on DB2.
 *
 * @author Jiri Pechanec
 */
public class Db2SchemaChangeEventEmitter implements SchemaChangeEventEmitter {

    private final Db2OffsetContext offsetContext;
    private final Db2ChangeTable changeTable;
    private final Table tableSchema;
    private final SchemaChangeEventType eventType;

    public Db2SchemaChangeEventEmitter(Db2OffsetContext offsetContext, Db2ChangeTable changeTable, Table tableSchema, SchemaChangeEventType eventType) {
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
