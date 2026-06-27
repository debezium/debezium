/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package ${package};

import io.debezium.data.Envelope;
import io.debezium.relational.RelationalChangeRecordEmitter;
import io.debezium.util.Clock;

/**
 * Converts a changed table row into a Debezium change record and emits it to the framework.
 *
 * <p>Extends {@link RelationalChangeRecordEmitter}, which builds the key, value, and envelope
 * {@link org.apache.kafka.connect.data.Struct}s from the table's {@code TableSchema} at emit
 * time. This class only supplies the operation and the old/new column values; it never builds
 * Structs itself.
 *
 * <p>The column arrays are positional: each entry lines up with a column of the table, in the
 * order the schema holds them. For an insert pass the new values and a {@code null} old array;
 * for a delete pass the old values and a {@code null} new array; for an update pass both.
 */
public class ${connectorName}ChangeRecordEmitter extends RelationalChangeRecordEmitter<${connectorName}Partition> {

    private final Envelope.Operation operation;
    private final Object[] oldColumnValues;
    private final Object[] newColumnValues;

    public ${connectorName}ChangeRecordEmitter(${connectorName}Partition partition,
                                               ${connectorName}OffsetContext offsetContext,
                                               Envelope.Operation operation,
                                               Object[] oldColumnValues,
                                               Object[] newColumnValues,
                                               Clock clock,
                                               ${connectorName}ConnectorConfig config) {
        super(partition, offsetContext, clock, config);
        this.operation = operation;
        this.oldColumnValues = oldColumnValues;
        this.newColumnValues = newColumnValues;
    }

    @Override
    public Envelope.Operation getOperation() {
        return operation;
    }

    @Override
    protected Object[] getOldColumnValues() {
        return oldColumnValues;
    }

    @Override
    protected Object[] getNewColumnValues() {
        return newColumnValues;
    }
}
