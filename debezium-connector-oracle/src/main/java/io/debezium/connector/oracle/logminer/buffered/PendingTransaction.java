/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered;

import java.time.Instant;
import java.util.Comparator;

import io.debezium.connector.oracle.Scn;

/**
 * A snapshot of a transaction that is currently pending in the buffered LogMiner streaming source,
 * either as an active transaction in the transaction cache or as a deferred transaction that has
 * not yet emitted any DML events.
 *
 * @param transactionId the transaction identifier
 * @param startScn the system change number at which the transaction started
 * @param changeTime the time the transaction started
 * @param userName the database user associated with the transaction
 * @param clientId the client identifier associated with the transaction
 * @param redoThreadId the redo thread the transaction was mined from
 * @param eventCount the number of buffered events, always {@code 0} for deferred transactions
 * @param deferred whether the transaction is deferred rather than active
 *
 * @author Chris Cranford
 */
public record PendingTransaction(String transactionId, Scn startScn, Instant changeTime, String userName,
        String clientId, int redoThreadId, int eventCount, boolean deferred) {

    /**
     * Orders pending transactions from oldest to newest by start SCN, using the transaction
     * identifier as a tie-breaker for a stable ordering.
     */
    public static final Comparator<PendingTransaction> OLDEST_FIRST = Comparator
            .comparing(PendingTransaction::startScn)
            .thenComparing(PendingTransaction::transactionId);
}
