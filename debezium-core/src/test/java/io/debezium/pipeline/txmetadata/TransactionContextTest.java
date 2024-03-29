/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.txmetadata;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class TransactionContextTest {

    @Test
    public void getTransactionId() {
        TransactionContext context = new TransactionContext();
        String txId = "foo";
        TransactionInfo info = new BasicTransactionInfo(txId);
        context.beginTransaction(info);
        assertThat(context.getTransactionId()).isEqualTo(txId);
    }
}
