/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered.chronicle;

import io.debezium.connector.oracle.logminer.buffered.TransactionFactory;
import io.debezium.connector.oracle.logminer.events.LogMinerEventRow;

/**
 * Transaction factory implementation for {@link ChronicleTransaction}.
 *
 * @author Debezium Authors
 */
public class ChronicleTransactionFactory implements TransactionFactory<ChronicleTransaction> {
    @Override
    public ChronicleTransaction createTransaction(LogMinerEventRow event) {
        return new ChronicleTransaction(event.getTransactionId(), event.getScn(), event.getChangeTime(),
                event.getUserName(), event.getThread(), event.getClientId());
    }
}
