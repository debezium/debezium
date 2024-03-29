/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.txmetadata;

import java.util.Map;

public class OrderedTransactionContext extends TransactionContext {

    private TransactionOrderMetadata transactionOrderMetadata;

    public OrderedTransactionContext(TransactionOrderMetadata transactionOrderMetadata) {
        super();
        this.transactionOrderMetadata = transactionOrderMetadata;
    }

    public OrderedTransactionContext(TransactionOrderMetadata transactionOrderMetadata, TransactionContext transactionContext) {
        super();
        this.transactionOrderMetadata = transactionOrderMetadata;

        // Copy fields
        this.transactionId = transactionContext.transactionId;
        this.totalEventCount = transactionContext.totalEventCount;
    }

    @Override
    public void beginTransaction(TransactionInfo transactionInfo) {
        super.beginTransaction(transactionInfo);
        transactionOrderMetadata.beginTransaction(transactionInfo);
    }

    @Override
    public Map<String, Object> store(Map<String, Object> offset) {
        offset = super.store(offset);
        return transactionOrderMetadata.store(offset);
    }

    public static OrderedTransactionContext load(Map<String, ?> offsets, TransactionOrderMetadata transactionOrderMetadata) {
        TransactionContext transactionContext = TransactionContext.load(offsets);
        OrderedTransactionContext orderedTransactionContext = new OrderedTransactionContext(transactionOrderMetadata, transactionContext);
        orderedTransactionContext.transactionOrderMetadata.load(offsets);
        return orderedTransactionContext;
    }

    public TransactionOrderMetadata getTransactionOrderMetadata() {
        return transactionOrderMetadata;
    }
}
