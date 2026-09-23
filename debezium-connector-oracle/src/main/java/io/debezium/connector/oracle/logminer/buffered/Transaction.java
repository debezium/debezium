/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered;

import java.time.Instant;

import io.debezium.connector.oracle.Scn;

/**
 * Contract for an Oracle transaction.
 *
 * @author Chris Cranford
 */
public interface Transaction {
    String NO_SEQUENCE_TRX_ID_SUFFIX = "ffffffff";
    int NO_TRANSACTION_ID_USN_SLT = -1; // ffffffff

    static int getUsnSlt(String transactionId) {
        if (transactionId == null) {
            return NO_TRANSACTION_ID_USN_SLT;
        }
        return Character.digit(transactionId.charAt(5), 16)
                | (Character.digit(transactionId.charAt(4), 16) << 4)
                | (Character.digit(transactionId.charAt(7), 16) << 8)
                | (Character.digit(transactionId.charAt(6), 16) << 12)
                | (Character.digit(transactionId.charAt(1), 16) << 16)
                | (Character.digit(transactionId.charAt(0), 16) << 20)
                | (Character.digit(transactionId.charAt(3), 16) << 24)
                | (Character.digit(transactionId.charAt(2), 16) << 28);
    }

    static void checkSqn(String transactionId, String currentTransactionId, Scn currentStartScn) {
        if (!transactionId.endsWith(NO_SEQUENCE_TRX_ID_SUFFIX) && !transactionId.regionMatches(8, currentTransactionId, 8, 8)) {
            throw new IllegalStateException("Invalid XID %s: The slot is occupied by the transaction %s started at SCN %s".formatted(transactionId,
                    currentTransactionId, currentStartScn));
        }
    }

    /**
     * Get the transaction identifier
     *
     * @return the transaction unique identifier, never {@code null}
     */
    String getTransactionId();

    default int getUsnSlt() {
        return getUsnSlt(getTransactionId());
    }

    /**
     * Get the system change number of when the transaction started
     *
     * @return the system change number, never {@code null}
     */
    Scn getStartScn();

    /**
     * Get the time when the transaction started
     *
     * @return the timestamp of the transaction, never {@code null}
     */
    Instant getChangeTime();

    /**
     * Get the username associated with the transaction
     *
     * @return the username, may be {@code null}
     */
    String getUserName();

    /**
     * Get the client id associated with the transaction
     *
     * @return the client id, may be {@code null}
     */
    String getClientId();

    /**
     * Get the number of events participating in the transaction.
     *
     * @return the number of transaction events
     */
    int getNumberOfEvents();

    /**
     * Return the id of an event based on its index.
     * @param index the index of the event
     * @return the event id
     */
    default long getEventId(int index) {
        if (index < 0 || index >= getNumberOfEvents()) {
            throw new IndexOutOfBoundsException("Index " + index + "outside the transaction " + getTransactionId() + " event list bounds");
        }
        return (long) getUsnSlt() << 32 | index;
    }

    /**
     * Helper method to get the next event identifier for the transaction.
     *
     * @return the next event identifier
     */
    int getNextEventId();

    /**
     * Get the redo thread that the transaction participated on.
     *
     * @return the redo thread number
     */
    int getRedoThreadId();

    /**
     * Helper method that resets the event identifier back to {@code 0}.
     * <p>
     * This should be called when a transaction {@code START} event is detected in the event stream.
     * This is required when LOB support is enabled to facilitate the re-mining of existing events.
     */
    void start();

}
