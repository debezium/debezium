/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.pipeline.txmetadata;

import java.time.Instant;

import org.apache.kafka.connect.data.Struct;

import io.debezium.connector.postgresql.PostgresOffsetContext;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.txmetadata.BasicTransactionStructMaker;
import io.debezium.spi.schema.DataCollectionId;

public class PostgresTransactionStructMaker extends BasicTransactionStructMaker {

    @Override
    public Struct prepareTxKey(OffsetContext offsetContext) {
        return adjustTxId(new Struct(transactionKeySchema), offsetContext);
    }

    @Override
    public Struct prepareTxBeginValue(OffsetContext offsetContext, Instant timestamp) {
        return adjustTxId(super.prepareTxBeginValue(offsetContext, timestamp), offsetContext);
    }

    @Override
    public Struct prepareTxEndValue(OffsetContext offsetContext, Instant timestamp) {
        return adjustTxId(super.prepareTxEndValue(offsetContext, timestamp), offsetContext);
    }

    @Override
    public Struct prepareTxStruct(OffsetContext offsetContext, DataCollectionId source) {
        return adjustTxId(super.prepareTxStruct(offsetContext, source), offsetContext);
    }

    private Struct adjustTxId(Struct txStruct, OffsetContext offsetContext) {
        final String lsn = Long.toString(((PostgresOffsetContext) offsetContext).asOffsetState().lastSeenLsn().asLong());
        final String txId = offsetContext.getTransactionContext().getTransactionId();
        txStruct.put(DEBEZIUM_TRANSACTION_ID_KEY, String.format("%s:%s", txId, lsn));
        return txStruct;
    }
}
