/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Schema;

import java.util.List;
import java.util.Objects;

import static io.debezium.connector.cassandra.SchemaHolder.getFieldSchema;

/**
 * An immutable data structure representing a change event, and can be converted
 * to a kafka connect Struct representing key/value of the change event.
 */
public abstract class Record implements Event {
    static final String AFTER = "after";
    static final String OPERATION = "op";
    static final String SOURCE = "source";
    static final String TIMESTAMP = "ts_ms";

    private final SourceInfo source;
    private final RowData rowData;
    private final Operation op;
    private final long ts;
    private final Schema keySchema;
    private final Schema valueSchema;
    private final boolean shouldMarkOffset;

    public enum Operation {
        INSERT("i"),
        UPDATE("u"),
        DELETE("d");

        private String value;

        Operation(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }

        @Override
        public String toString() {
            return value;
        }
    }

    Record(SourceInfo source, RowData rowData, Schema keySchema, Schema valueSchema, Operation op, boolean shouldMarkOffset, long ts) {
        this.source = source;
        this.rowData = rowData;
        this.op = op;
        this.keySchema = keySchema;
        this.valueSchema = valueSchema;
        this.shouldMarkOffset = shouldMarkOffset;
        this.ts = ts;
    }

    public Struct buildKey() {
        if (keySchema == null) {
            return null;
        }

        List<CellData> primary = rowData.getPrimary();
        Struct struct = new Struct(keySchema);
        for (CellData cellData : primary) {
            struct.put(cellData.name, cellData.value);
        }
        return struct;
    }

    public Struct buildValue() {
        if (valueSchema == null) {
            return null;
        }

        return new Struct(valueSchema)
                .put(TIMESTAMP, ts)
                .put(OPERATION, op.getValue())
                .put(SOURCE, source.struct())
                .put(AFTER, rowData.record(getFieldSchema(AFTER, valueSchema)));
    }

    @Override
    public String toString() {
        return "Record{"
                + "source=" + source
                + ", after=" + rowData
                + ", keySchema=" + keySchema
                + ", valueSchema=" + valueSchema
                + ", op=" + op
                + ", ts=" + ts
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Record record = (Record) o;
        return ts == record.ts
                && Objects.equals(source, record.source)
                && Objects.equals(rowData, record.rowData)
                && Objects.equals(keySchema, record.keySchema)
                && Objects.equals(valueSchema, record.valueSchema)
                && op == record.op;
    }

    @Override
    public int hashCode() {
        return Objects.hash(source, rowData, keySchema, valueSchema, op, ts);
    }

    public SourceInfo getSource() {
        return source;
    }

    public RowData getRowData() {
        return rowData;
    }

    public Operation getOp() {
        return op;
    }

    public long getTs() {
        return ts;
    }

    public Schema getKeySchema() {
        return keySchema;
    }

    public Schema getValueSchema() {
        return valueSchema;
    }

    public boolean shouldMarkOffset() {
        return shouldMarkOffset;
    }
}
