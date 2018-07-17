/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.pipeline.spi.SchemaChangeEventEmitter;
import io.debezium.relational.TableId;

/**
 * {@link SchemaChangeEventEmitter} implementation based on SqlServer.
 *
 * @author Jiri Pechanec
 */
public class SqlServerSchemaChangeEventEmitter implements SchemaChangeEventEmitter {

    private static final Logger LOGGER = LoggerFactory.getLogger(SqlServerSchemaChangeEventEmitter.class);

    private final SqlServerOffsetContext offsetContext;
    private final TableId tableId;

    public SqlServerSchemaChangeEventEmitter(SqlServerOffsetContext offsetContext, TableId tableId) {
        this.offsetContext = offsetContext;
        this.tableId = tableId;
    }

    @Override
    public void emitSchemaChangeEvent(Receiver receiver) throws InterruptedException {
        throw new UnsupportedOperationException("Schema evolution is not supported by the connector");
    }
}
