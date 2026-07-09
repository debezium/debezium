/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.tidb;

import org.apache.kafka.connect.data.Struct;

import io.debezium.connector.tidb.ticdc.TiCdcEvent;
import io.debezium.data.Envelope.Operation;
import io.debezium.pipeline.AbstractChangeRecordEmitter;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.util.Clock;

/**
 * Emits change records for data changes consumed from the TiCDC stream. The typed row images of
 * the TiCDC message are re-wrapped into the connector's own envelope so that the {@code source}
 * block carries the real TiDB position (TSO commit timestamp) instead of the MySQL-flavoured
 * dummies of TiCDC's Debezium output mode.
 *
 * @author Aviral Srivastava
 */
public class TiDbChangeRecordEmitter extends AbstractChangeRecordEmitter<TiDbPartition, TiDbTableSchema> {

    private final TiCdcEvent event;

    public TiDbChangeRecordEmitter(TiDbPartition partition, OffsetContext offsetContext, Clock clock,
                                   TiDbConnectorConfig connectorConfig, TiCdcEvent event) {
        super(partition, offsetContext, clock, connectorConfig);
        this.event = event;
    }

    @Override
    public Operation getOperation() {
        return event.operation();
    }

    @Override
    protected void emitReadRecord(Receiver<TiDbPartition> receiver, TiDbTableSchema schema) throws InterruptedException {
        final Struct envelope = schema.getEnvelopeSchema().read(
                event.after(), getOffset().getSourceInfo(), getClock().currentTimeAsInstant());
        receiver.changeRecord(getPartition(), schema, Operation.READ, event.key(), envelope, getOffset(), null);
    }

    @Override
    protected void emitCreateRecord(Receiver<TiDbPartition> receiver, TiDbTableSchema schema) throws InterruptedException {
        final Struct envelope = schema.getEnvelopeSchema().create(
                event.after(), getOffset().getSourceInfo(), getClock().currentTimeAsInstant());
        receiver.changeRecord(getPartition(), schema, Operation.CREATE, event.key(), envelope, getOffset(), null);
    }

    @Override
    protected void emitUpdateRecord(Receiver<TiDbPartition> receiver, TiDbTableSchema schema) throws InterruptedException {
        final Struct envelope = schema.getEnvelopeSchema().update(
                event.before(), event.after(), getOffset().getSourceInfo(), getClock().currentTimeAsInstant());
        receiver.changeRecord(getPartition(), schema, Operation.UPDATE, event.key(), envelope, getOffset(), null);
    }

    @Override
    protected void emitDeleteRecord(Receiver<TiDbPartition> receiver, TiDbTableSchema schema) throws InterruptedException {
        final Struct envelope = schema.getEnvelopeSchema().delete(
                event.before(), getOffset().getSourceInfo(), getClock().currentTimeAsInstant());
        receiver.changeRecord(getPartition(), schema, Operation.DELETE, event.key(), envelope, getOffset(), null);
    }
}
