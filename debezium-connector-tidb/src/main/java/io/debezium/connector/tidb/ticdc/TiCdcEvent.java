/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.tidb.ticdc;

import java.time.Instant;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import io.debezium.data.Envelope.Operation;
import io.debezium.relational.TableId;

/**
 * A single data change event parsed from the Debezium-format output of a TiCDC changefeed.
 *
 * @author Aviral Srivastava
 */
public class TiCdcEvent {

    private final TableId tableId;
    private final Operation operation;
    private final Struct before;
    private final Struct after;
    private final Schema rowSchema;
    private final Struct key;
    private final Schema keySchema;
    private final Instant sourceTimestamp;
    private final long commitTs;
    private final String clusterId;

    public TiCdcEvent(TableId tableId, Operation operation, Struct before, Struct after, Schema rowSchema,
                      Struct key, Schema keySchema, Instant sourceTimestamp, long commitTs, String clusterId) {
        this.tableId = tableId;
        this.operation = operation;
        this.before = before;
        this.after = after;
        this.rowSchema = rowSchema;
        this.key = key;
        this.keySchema = keySchema;
        this.sourceTimestamp = sourceTimestamp;
        this.commitTs = commitTs;
        this.clusterId = clusterId;
    }

    public TableId tableId() {
        return tableId;
    }

    public Operation operation() {
        return operation;
    }

    public Struct before() {
        return before;
    }

    public Struct after() {
        return after;
    }

    /**
     * @return the schema of a single row of the table, as advertised by the TiCDC message
     */
    public Schema rowSchema() {
        return rowSchema;
    }

    public Struct key() {
        return key;
    }

    public Schema keySchema() {
        return keySchema;
    }

    /**
     * @return the commit time of the source transaction in TiDB
     */
    public Instant sourceTimestamp() {
        return sourceTimestamp;
    }

    /**
     * @return the TSO commit timestamp of the source transaction
     */
    public long commitTs() {
        return commitTs;
    }

    /**
     * @return the id of the TiDB cluster the change originates from, may be {@code null}
     */
    public String clusterId() {
        return clusterId;
    }

    @Override
    public String toString() {
        return "TiCdcEvent [tableId=" + tableId + ", operation=" + operation + ", commitTs=" + commitTs + "]";
    }
}
