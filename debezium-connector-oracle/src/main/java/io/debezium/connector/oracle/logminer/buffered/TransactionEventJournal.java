/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered;

import java.util.ArrayDeque;
import java.util.Deque;

import io.debezium.connector.oracle.Scn;

/**
 * Tracks the system change numbers of a buffered transaction's events in heap memory, so that a mining step
 * can resume reading after the last event of each transaction rather than re-reading every transaction from
 * its start, and rewind each transaction to the first event that the next mining step re-reads.
 * <p>
 * Only events with a system change number after the read position are retained. The journal is not persisted,
 * so after a restart the transactions are re-read from their start.
 *
 * @author Chris Cranford
 */
public class TransactionEventJournal {

    private final Deque<Entry> entries = new ArrayDeque<>();
    private Scn lastEventScn = Scn.NULL;
    private Scn lastRollbackToSavepointScn = Scn.NULL;
    private Scn discardedUpToScn = Scn.NULL;

    /**
     * Records an event that was added to the transaction buffer.
     *
     * @param eventId the event's identifier
     * @param scn the event's system change number, should not be {@code null}
     * @param rollbackToSavepoint true if the event is a rollback to savepoint, false otherwise
     */
    public void eventAdded(int eventId, Scn scn, boolean rollbackToSavepoint) {
        entries.addLast(new Entry(eventId, scn));
        if (lastEventScn.isNull() || scn.compareTo(lastEventScn) > 0) {
            lastEventScn = scn;
        }
        if (rollbackToSavepoint && (lastRollbackToSavepointScn.isNull() || scn.compareTo(lastRollbackToSavepointScn) > 0)) {
            lastRollbackToSavepointScn = scn;
        }
    }

    /**
     * @return the highest system change number of the transaction's events, or {@link Scn#NULL} if none
     */
    public Scn getLastEventScn() {
        return lastEventScn;
    }

    /**
     * Discards the events at or before the read position, which are not re-read.
     *
     * @param readScn the read position, should not be {@code null}
     */
    public void discardEventsUpTo(Scn readScn) {
        while (!entries.isEmpty() && entries.peekFirst().scn().compareTo(readScn) <= 0) {
            entries.removeFirst();
        }
        if (discardedUpToScn.isNull() || readScn.compareTo(discardedUpToScn) > 0) {
            discardedUpToScn = readScn;
        }
    }

    /**
     * Checks whether the events after the read position can be rewound and re-read by position. This is not
     * possible when the read position is before events that were already discarded, when a rollback to
     * savepoint after the read position may have removed events that are not re-read, or when an event at or
     * before the read position follows an event after it.
     *
     * @param readScn the read position, should not be {@code null}
     * @return true if the transaction can be rewound to the read position, false otherwise
     */
    public boolean isRewindable(Scn readScn) {
        if (!discardedUpToScn.isNull() && readScn.compareTo(discardedUpToScn) < 0) {
            return false;
        }
        if (!lastRollbackToSavepointScn.isNull() && lastRollbackToSavepointScn.compareTo(readScn) > 0) {
            return false;
        }
        for (Entry entry : entries) {
            if (entry.scn().compareTo(readScn) <= 0) {
                return false;
            }
        }
        return true;
    }

    /**
     * @return the identifier of the first event after the read position, or {@code -1} if there are none
     */
    public int getFirstEventId() {
        return entries.isEmpty() ? -1 : entries.peekFirst().eventId();
    }

    /**
     * Discards all events, as they are recorded again when re-read.
     */
    public void clearEvents() {
        entries.clear();
    }

    private record Entry(int eventId, Scn scn) {
    }
}
