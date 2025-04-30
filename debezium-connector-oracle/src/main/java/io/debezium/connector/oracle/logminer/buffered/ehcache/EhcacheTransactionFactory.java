/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered.ehcache;

import io.debezium.connector.oracle.logminer.buffered.TransactionFactory;
import io.debezium.connector.oracle.logminer.events.LogMinerEventRow;

/**
 * Transaction factory implementation for {@link EhcacheTransaction}.
 *
 * @author Chris Cranford
 */
public class EhcacheTransactionFactory implements TransactionFactory<EhcacheTransaction> {
    @Override
    public EhcacheTransaction createTransaction(LogMinerEventRow event) {
        return new EhcacheTransaction(event.getTransactionId(), event.getScn(), event.getChangeTime(),
                event.getUserName(), event.getThread(), event.getClientId());
    }
}
