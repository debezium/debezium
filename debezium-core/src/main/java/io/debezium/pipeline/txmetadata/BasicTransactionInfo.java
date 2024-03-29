/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.txmetadata;

public class BasicTransactionInfo implements TransactionInfo {

    private final String transactionId;

    public BasicTransactionInfo(String transactionId) {
        this.transactionId = transactionId;
    }

    @Override
    public String getTransactionId() {
        return transactionId;
    }
}
