/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private static final Logger LOGGER = LoggerFactory.getLogger(SqlServerSchemaChangeEventEmitter.class);

    private final SqlServerOffsetContext offsetContext;
    private final ChangeTable changeTable;
    private final Table tableSchema;

    public SqlServerSchemaChangeEventEmitter(SqlServerOffsetContext offsetContext, ChangeTable changeTable, Table tableSchema) {
        this.offsetContext = offsetContext;
        this.changeTable = changeTable;
        this.tableSchema = tableSchema;
    }

    @Override
    public void emitSchemaChangeEvent(Receiver receiver) throws InterruptedException {
        final SchemaChangeEvent event = new SchemaChangeEvent(offsetContext.getPartition(), offsetContext.getOffset(), changeTable.getSourceTableId().catalog(), changeTable.getSourceTableId().schema(), "N/A", tableSchema, SchemaChangeEventType.CREATE, false);
        receiver.schemaChangeEvent(event);
    }
}
