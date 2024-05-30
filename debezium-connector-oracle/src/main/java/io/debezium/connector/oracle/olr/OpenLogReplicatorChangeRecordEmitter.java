/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.olr;

import java.sql.Connection;

import io.debezium.connector.oracle.BaseChangeRecordEmitter;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.OraclePartition;
import io.debezium.data.Envelope.Operation;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.util.Clock;

import oracle.jdbc.internal.OracleTypes;

/**
 * A change record emitter for the OpenLogReplicator streaming adapter.
 *
 * @author Chris Cranford
 */
public class OpenLogReplicatorChangeRecordEmitter extends BaseChangeRecordEmitter<Object> {

    private static final String EPOCH_NANO = "TIMESTAMP'1970-01-01 00:00:00' + NUMTODSINTERVAL(%s/1000000000,'SECOND')";
    private static final String TO_DSINTERVAL = "TO_DSINTERVAL('%s')";
    private static final String TO_YMINTERVAL = "TO_YMINTERVAL('%s')";

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

    @Override
    protected Object convertReselectPrimaryKeyColumn(Connection connection, Column column, Object value) {
        switch (column.jdbcType()) {
            case OracleTypes.TIMESTAMP:
            case OracleTypes.DATE:
                if (value instanceof Number) {
                    // OpenLogReplicator should be configured to provide values in nanoseconds precision.
                    value = convertValueViaQuery(connection, String.format(EPOCH_NANO, value));
                }
                break;
            case OracleTypes.INTERVALDS:
                if (value instanceof String) {
                    // OpenLogReplicator provides this as an TO_DSINTERVAL constructor argument string.
                    value = convertValueViaQuery(connection, String.format(TO_DSINTERVAL, ((String) value).replace(",", " ")));
                }
                break;
            case OracleTypes.INTERVALYM:
                if (value instanceof String) {
                    value = convertValueViaQuery(connection, String.format(TO_YMINTERVAL, value));
                }
                break;
        }
        return value;
    }
}
