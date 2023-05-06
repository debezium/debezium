/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import io.debezium.data.Envelope.Operation;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;
import io.debezium.relational.RelationalChangeRecordEmitter;
import io.debezium.util.Clock;

/**
 * Emits change data based on a single (or two in case of updates) CDC data row(s).
 *
 * @author Jiri Pechanec
 */
public class SqlServerChangeRecordEmitter extends RelationalChangeRecordEmitter {

    public static final int OP_DELETE = 1;
    public static final int OP_INSERT = 2;
    public static final int OP_UPDATE_BEFORE = 3;
    public static final int OP_UPDATE_AFTER = 4;

    private final int operation;
    private final Object[] data;
    private final Object[] dataNext;

    public SqlServerChangeRecordEmitter(Partition partition, OffsetContext offset, int operation, Object[] data, Object[] dataNext, Clock clock,
                                        SqlServerConnectorConfig connectorConfig) {
        super(partition, offset, clock, connectorConfig);

        this.operation = operation;
        this.data = data;
        this.dataNext = dataNext;
    }

    @Override
    public Operation getOperation() {
        if (operation == OP_DELETE) {
            return Operation.DELETE;
        }
        else if (operation == OP_INSERT) {
            return Operation.CREATE;
        }
        else if (operation == OP_UPDATE_BEFORE) {
            return Operation.UPDATE;
        }
        throw new IllegalArgumentException("Received event of unexpected command type: " + operation);
    }

    @Override
    protected Object[] getOldColumnValues() {
        switch (getOperation()) {
            case CREATE:
            case READ:
                return null;
            default:
                return data;
        }
    }

    @Override
    protected Object[] getNewColumnValues() {
        switch (getOperation()) {
            case CREATE:
            case READ:
                return data;
            case UPDATE:
                return dataNext;
            default:
                return null;
        }
    }
}
