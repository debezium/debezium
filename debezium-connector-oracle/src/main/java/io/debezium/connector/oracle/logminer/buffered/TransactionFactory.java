/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered;

import java.time.Instant;

import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.events.LogMinerEventRow;

/**
 * A factory for creating specific types of {@link Transaction} instances.
 *
 * @author Chris Cranford
 */
public interface TransactionFactory<T extends Transaction> {
    /**
     * Create a transaction from the mined event.
     *
     * @param event the mined event, should not be {@code null}
     * @return the constructed transaction instance, never {@code null}
     */
    T createTransaction(LogMinerEventRow event);

    /**
     * Create a transaction from explicit metadata.
     *
     * @param transactionId the transaction identifier, should not be {@code null}
     * @param startScn the transaction start scn, should not be {@code null}
     * @param changeTime the transaction change time, should not be {@code null}
     * @param userName the transaction user name, may be {@code null}
     * @param redoThreadId the redo thread id, may be {@code null}
     * @param clientId the transaction client id, may be {@code null}
     * @return the constructed transaction instance, never {@code null}
     */
    T createTransaction(String transactionId, Scn startScn, Instant changeTime, String userName, Integer redoThreadId, String clientId);
}
