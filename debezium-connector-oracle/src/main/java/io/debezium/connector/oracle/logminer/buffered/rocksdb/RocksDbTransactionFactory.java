/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered.rocksdb;

import io.debezium.connector.oracle.logminer.buffered.TransactionFactory;
import io.debezium.connector.oracle.logminer.events.LogMinerEventRow;

/**
 * A factory for creating {@link RocksDbTransaction} instances.
 *
 * @author Debezium Authors
 */
public class RocksDbTransactionFactory implements TransactionFactory<RocksDbTransaction> {

    @Override
    public RocksDbTransaction createTransaction(LogMinerEventRow event) {
        return new RocksDbTransaction(event.getTransactionId(), event.getScn(), event.getChangeTime(), event.getUserName(), event.getThread(), event.getClientId());
    }
}