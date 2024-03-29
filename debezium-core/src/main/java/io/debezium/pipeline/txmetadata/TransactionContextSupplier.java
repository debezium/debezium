/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.txmetadata;

import java.util.Map;

import io.debezium.config.CommonConnectorConfig;

public class TransactionContextSupplier {

    private final CommonConnectorConfig connectorConfig;

    public TransactionContextSupplier(CommonConnectorConfig connectorConfig) {
        this.connectorConfig = connectorConfig;
    }

    public TransactionContext newTransactionContext() {
        if (!connectorConfig.shouldProvideOrderedTransactionMetadata()) {
            return new TransactionContext();
        }
        TransactionOrderMetadata transactionOrderMetadata = connectorConfig.getTransactionOrderMetadata();
        return new OrderedTransactionContext(transactionOrderMetadata);
    }

    public TransactionContext loadTransactionContext(Map<String, ?> offsets) {
        if (!connectorConfig.shouldProvideOrderedTransactionMetadata()) {
            return TransactionContext.load(offsets);
        }
        TransactionOrderMetadata transactionOrderMetadata = connectorConfig.getTransactionOrderMetadata();
        return OrderedTransactionContext.load(offsets, transactionOrderMetadata);
    }
}
