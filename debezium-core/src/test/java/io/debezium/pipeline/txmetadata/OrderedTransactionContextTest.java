/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.txmetadata;

import java.util.Map;

import org.junit.Test;

public class OrderedTransactionContextTest {

    public class TestTransactionOrderMetadata implements TransactionOrderMetadata {
        @Override
        public Map<String, Object> store(Map<String, Object> offset) {
            return null;
        }

        @Override
        public void load(Map<String, ?> offsets) {

        }

        @Override
        public void beginTransaction(TransactionInfo transactionInfo) {

        }

        @Override
        public void endTransaction() {

        }
    }

    @Test
    public void beginTransaction() {
        TransactionOrderMetadata transactionOrderMetadata = new TestTransactionOrderMetadata();
        TransactionContext context = new OrderedTransactionContext(transactionOrderMetadata);
        TransactionInfo transactionInfo = new BasicTransactionInfo("foo");
        context.beginTransaction(transactionInfo);
    }
}
