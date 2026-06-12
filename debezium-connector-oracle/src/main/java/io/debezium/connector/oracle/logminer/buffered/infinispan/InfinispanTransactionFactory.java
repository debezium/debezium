/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered.infinispan;

import java.time.Instant;

import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.buffered.TransactionFactory;
import io.debezium.connector.oracle.logminer.events.LogMinerEventRow;

/**
 * Transaction factory implementation for {@link InfinispanTransaction}.
 *
 * @author Chris Cranford
 */
public class InfinispanTransactionFactory implements TransactionFactory<InfinispanTransaction> {
    @Override
    public InfinispanTransaction createTransaction(LogMinerEventRow event) {
        return new InfinispanTransaction(event.getTransactionId(), event.getScn(), event.getChangeTime(),
                event.getUserName(), event.getThread(), event.getClientId());
    }

    @Override
    public InfinispanTransaction createTransaction(String transactionId, Scn startScn, Instant changeTime, String userName, Integer redoThreadId, String clientId) {
        return new InfinispanTransaction(transactionId, startScn, changeTime, userName, redoThreadId, clientId);
    }
}
