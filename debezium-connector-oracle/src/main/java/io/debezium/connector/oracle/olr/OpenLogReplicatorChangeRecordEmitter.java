/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.olr;

import io.debezium.connector.oracle.BaseChangeRecordEmitter;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.OraclePartition;
import io.debezium.data.Envelope.Operation;
import io.debezium.relational.Table;
import io.debezium.util.Clock;

/**
 * A change record emitter for the OpenLogReplicator streaming adapter.
 *
 * @author Chris Cranford
 */
public class OpenLogReplicatorChangeRecordEmitter extends BaseChangeRecordEmitter<Object> {

    private final Operation operation;

    public OpenLogReplicatorChangeRecordEmitter(OracleConnectorConfig connectorConfig, OraclePartition partition,
                                                OracleOffsetContext offsetContext, Operation operation,
                                                Object[] oldValues, Object[] newValues,
                                                Table table, OracleDatabaseSchema schema, Clock clock) {
        super(connectorConfig, partition, offsetContext, schema, table, clock, oldValues, newValues);
        this.operation = operation;
    }

    @Override
    public Operation getOperation() {
        return operation;
    }

}
