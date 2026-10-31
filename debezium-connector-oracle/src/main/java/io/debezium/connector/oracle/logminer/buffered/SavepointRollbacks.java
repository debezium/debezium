/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered;

import java.util.BitSet;
import java.util.function.IntFunction;

import io.debezium.connector.oracle.logminer.events.CachedEvent;
import io.debezium.connector.oracle.logminer.events.EventType;

/**
 * Resolves the partial rollbacks of a transaction when it is read, from its {@link EventMetadataColumns}
 * alone, into a bit set of the events to leave out.
 * <p>
 * The undo markers are visited in order, and each walks backward over the events that the markers before it
 * have not already undone, applying the rules for scalar, inline and out-of-line LOB and XML statements; the
 * events between the matched change and the run of markers that ends at the undo are marked. Resolving at
 * read time rather than as each marker arrives means nothing is ever removed from an open transaction, so a
 * discarded mining pass has only its appends to drop. No stored event is read; a marker is only loaded when
 * a warning needs its details.
 *
 * @author Chris Cranford
 */
public final class SavepointRollbacks {

    private static final int NONE = -1;

    private final String transactionId;
    private final EventMetadataColumns tx;
    private final IntFunction<? extends CachedEvent> markerLoader;
    private final BitSet skipped;
    private final int markerId;
    private CachedEvent marker;

    private SavepointRollbacks(String transactionId, EventMetadataColumns metadata, IntFunction<? extends CachedEvent> markerLoader, BitSet skipped,
                               int markerId) {
        this.transactionId = transactionId;
        this.tx = metadata;
        this.markerLoader = markerLoader;
        this.skipped = skipped;
        this.markerId = markerId;
    }

    /**
     * Computes the events of a transaction that its undo markers revert.
     *
     * @param transactionId the transaction, for the warnings
     * @param metadata the transaction's event metadata
     * @param markerLoader loads the stored event at a position, used only when a warning needs the marker's details
     * @return a bit per event position, set for the events a read leaves out
     */
    public static BitSet resolve(String transactionId, EventMetadataColumns metadata, IntFunction<? extends CachedEvent> markerLoader) {
        final BitSet skipped = new BitSet(metadata.count());
        for (int id = 0; id < metadata.count(); id++) {
            if (metadata.isRollback(id)) {
                final int[] range = new SavepointRollbacks(transactionId, metadata, markerLoader, skipped, id).findRolledBackRange();
                if (range != null) {
                    skipped.set(range[0], range[1]);
                }
            }
        }
        return skipped;
    }

    /**
     * @return the position of the nearest event before {@code id} that an earlier undo has not reverted, or {@code -1}
     */
    private int previousLive(int id) {
        return skipped.previousClearBit(id - 1);
    }

    private int[] findRolledBackRange() {
        // INSERT/UPDATE statements containing LOB/XML columns are stored in the cache as a sequence:
        // 1. (optional) INSERT or UPDATE with an empty ROW_ID, containing regular column values and initial LOB/XML column values
        // 2. (optional) LOB operation groups with an empty ROW_ID, one group per out-of-line LOB value:
        // 2.1. SELECT_LOB_LOCATOR + one or more LOB_WRITE (optional LOB_TRIM is not stored) or
        // 2.2. EXTENDED_STRING_BEGIN + one or more EXTENDED_STRING_WRITE (EXTENDED_STRING_END is not stored)
        // 3. (optional) XML operation groups with an empty ROW_ID, one group per out-of-line XML value:
        // 3.1. XML_BEGIN + one or more XML_WRITE + XML_END
        // 4. Final operation with a real ROW_ID: UPDATE containing inline LOB/XML values or INTERNAL with SEQUENCE# > 1
        //
        // INSERT, UPDATE, SELECT_LOB_LOCATOR and EXTENDED_STRING_BEGIN of the same statement share the same RS_ID.
        //
        // When out-of-line LOB and XML values are inserted/updated in one statement, the final INTERNAL event has SEQUENCE# = 1.
        // When XML values are updated without others in a statement, the first XML_BEGIN event has SEQUENCE# > 1.
        //
        // DBMS_LOB procedures are stored in the cache as:
        // 1. SELECT_LOB_LOCATOR with a real ROW_ID
        // 2. one or more LOB_WRITE/LOB_ERASE (LOB_TRIM is not stored) with a real or empty ROW_ID
        int cursor = markerId;
        if (cursor < 0) {
            return null;
        }

        int end = cursor;
        cursor = previousLive(cursor);
        if (!tx.isRollback(end)) {
            return null;
        }

        final int rollbackEvent = end;
        final EventType rollbackType = tx.eventType(rollbackEvent);
        int rolledBackEntry = NONE;
        int lobStartEntry = NONE;
        boolean lobStmt = false;

        while (cursor >= 0) {
            final int entry = cursor;
            cursor = previousLive(cursor);

            final EventType eventType = tx.eventType(entry);
            if (tx.isRollback(entry)) {
                end = entry;
                continue;
            }
            else if (tx.table(entry) != tx.table(rollbackEvent)) {
                RollbackWarnings.cannotApplyRollbackToSavepoint(transactionId, marker(), "TABLE_NAME", tx.tableId(entry));
                return null;
            }
            else if (tx.sameRowId(entry, rollbackEvent)) {
                if (eventType == EventType.INSERT && rollbackType == EventType.DELETE) {
                    return range(entry, end);
                }
                else if (eventType == EventType.DELETE && rollbackType == EventType.INSERT) {
                    return range(entry, end);
                }
                else if (eventType == EventType.SELECT_LOB_LOCATOR && rollbackType == EventType.UPDATE) {
                    RollbackWarnings.unexpectedEventBeforeRollback(transactionId, marker(), "OPERATION", eventType);
                    return range(entry, end);
                }
                else if ((eventType == EventType.LOB_WRITE || eventType == EventType.LOB_TRIM || eventType == EventType.LOB_ERASE)
                        && rollbackType == EventType.UPDATE) {
                    RollbackWarnings.unexpectedEventBeforeRollback(transactionId, marker(), "OPERATION", eventType);
                    lobStmt = true;
                    break;
                }
                else if ((eventType == EventType.UPDATE || eventType == EventType.INTERNAL)
                        && (rollbackType == EventType.DELETE || rollbackType == EventType.UPDATE)) {
                    rolledBackEntry = entry;
                    lobStmt = eventType == EventType.INTERNAL && rollbackType == EventType.UPDATE;
                    break;
                }
                RollbackWarnings.cannotApplyRollbackToSavepoint(transactionId, marker(), eventType);
                return null;
            }
            else if (tx.hasEmptyRowId(entry)) {
                RollbackWarnings.unexpectedEventBeforeRollback(transactionId, marker(), "ROW_ID", tx.rowIdAsString(entry));
                if (eventType == EventType.INSERT) {
                    if (rollbackType == EventType.DELETE) {
                        return range(entry, end);
                    }
                    RollbackWarnings.cannotApplyRollbackToSavepoint(transactionId, marker(), eventType);
                    return null;
                }
                else if (eventType == EventType.UPDATE) {
                    if (rollbackType == EventType.UPDATE) {
                        return range(entry, end);
                    }
                    RollbackWarnings.cannotApplyRollbackToSavepoint(transactionId, marker(), eventType);
                    return null;
                }
                else if (eventType == EventType.LOB_WRITE || eventType == EventType.LOB_TRIM || eventType == EventType.LOB_ERASE) {
                    lobStmt = true;
                }
                break;
            }
            RollbackWarnings.cannotApplyRollbackToSavepoint(transactionId, marker(), "ROW_ID", tx.rowIdAsString(entry));
            return null;
        }

        while (cursor >= 0) {
            final int entry = cursor;
            cursor = previousLive(cursor);

            final EventType eventType = tx.eventType(entry);
            if (tx.table(entry) != tx.table(rollbackEvent)) {
                RollbackWarnings.unexpectedEventWithEmptyRowId(transactionId, marker(), "TABLE_NAME", tx.tableId(entry));
                break;
            }

            if (lobStmt) {
                if (eventType == EventType.SELECT_LOB_LOCATOR) {
                    if (tx.sameRowId(entry, rollbackEvent)) {
                        return range(entry, end);
                    }
                }
                else if (eventType == EventType.LOB_WRITE || eventType == EventType.LOB_TRIM || eventType == EventType.LOB_ERASE) {
                    if (tx.sameRowId(entry, rollbackEvent) || tx.hasEmptyRowId(entry)) {
                        continue;
                    }
                }
                lobStmt = false;
            }

            if (!tx.hasEmptyRowId(entry) || eventType == EventType.INTERNAL) {
                break;
            }

            if (eventType == EventType.INSERT || eventType == EventType.UPDATE) {
                if (lobStartEntry != NONE && !tx.sameRsId(lobStartEntry, entry)) {
                    RollbackWarnings.unexpectedEventWithEmptyRowId(transactionId, marker(), "RS_ID", tx.rsId(entry));
                    break;
                }
                else if (eventType == EventType.INSERT && rollbackType == EventType.DELETE) {
                    return range(entry, end);
                }
                else if (eventType == EventType.UPDATE && rollbackType == EventType.UPDATE) {
                    return range(entry, end);
                }
                lobStartEntry = NONE;
                RollbackWarnings.unexpectedEventWithEmptyRowId(transactionId, marker(), "OPERATION", eventType);
                break;
            }
            else if (eventType == EventType.SELECT_LOB_LOCATOR || eventType == EventType.EXTENDED_STRING_BEGIN) {
                if (lobStartEntry != NONE && !tx.sameRsId(lobStartEntry, entry)) {
                    RollbackWarnings.unexpectedEventWithEmptyRowId(transactionId, marker(), "RS_ID", tx.rsId(entry));
                    break;
                }
                lobStartEntry = entry;
            }
            else if (eventType == EventType.XML_BEGIN) {
                if (lobStartEntry != NONE) {
                    RollbackWarnings.unexpectedEventWithEmptyRowId(transactionId, marker(), "OPERATION", eventType);
                    break;
                }
                rolledBackEntry = entry;
            }
            else if (eventType == EventType.XML_END
                    && tx.sequence(entry) == 1
                    && rolledBackEntry != NONE
                    && tx.eventType(rolledBackEntry) == EventType.XML_BEGIN
                    && tx.sequence(rolledBackEntry) > 1) {
                break;
            }
        }

        if (lobStartEntry != NONE) {
            rolledBackEntry = lobStartEntry;
        }
        else if (rolledBackEntry == NONE) {
            return null;
        }

        final EventType rolledBackType = tx.eventType(rolledBackEntry);
        if ((rolledBackType == EventType.UPDATE
                || rolledBackType == EventType.SELECT_LOB_LOCATOR
                || rolledBackType == EventType.EXTENDED_STRING_BEGIN
                || rolledBackType == EventType.XML_BEGIN) && rollbackType != EventType.UPDATE) {
            RollbackWarnings.cannotApplyRollbackToSavepoint(transactionId, marker(), rolledBackType);
            return null;
        }

        return range(rolledBackEntry, end);
    }

    /**
     * The events from the matched change up to, and excluding, the first marker of the run that ends at the undo.
     */
    private static int[] range(int start, int endExclusive) {
        return new int[]{ start, endExclusive };
    }

    private CachedEvent marker() {
        if (marker == null) {
            marker = markerLoader.apply(markerId);
        }
        return marker;
    }
}
