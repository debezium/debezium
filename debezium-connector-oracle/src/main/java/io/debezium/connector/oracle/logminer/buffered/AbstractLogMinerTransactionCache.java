/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;
import java.util.function.IntFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.oracle.logminer.events.EventType;
import io.debezium.connector.oracle.logminer.events.LogMinerEvent;
import io.debezium.connector.oracle.logminer.events.RollbackToSavepointEvent;
import io.debezium.connector.oracle.logminer.events.RowIdCodec;
import io.debezium.util.Loggings;

/**
 * An abstract implementation of {@link LogMinerTransactionCache}.
 *
 * @param <T> the transaction type
 *
 * @author Chris Cranford
 */
public abstract class AbstractLogMinerTransactionCache<T extends Transaction> implements LogMinerTransactionCache<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractLogMinerTransactionCache.class);
    private final Set<String> abandonedTransactions = new HashSet<>();

    @Override
    public void abandon(T transaction) {
        abandonedTransactions.add(transaction.getTransactionId());
    }

    @Override
    public void removeAbandonedTransaction(String transactionId) {
        abandonedTransactions.remove(transactionId);
    }

    @Override
    public boolean isAbandoned(String transactionId) {
        return abandonedTransactions.contains(transactionId);
    }

    @Override
    public void resetTransactionToStart(T transaction) {
        transaction.start();
    }

    @Override
    public Optional<ScnDetails> getEldestTransactionScnDetailsInCache() {
        // Returning the eldest transaction would be misleading here because each cache implementation may not
        // be able to guarantee that the transactions are returned in chronological order. So instead, the
        // cache can only provide SCN details, which for multiple eldest transactions may be the same.
        return streamTransactionsAndReturn(stream -> stream.min(this::compareTransactionScnDetails)
                .map(transaction -> new ScnDetails(transaction.getStartScn(), transaction.getChangeTime())));
    }

    protected LogMinerEventEntryRange findRolledBackRange(String transactionId, Iterator<LogMinerEventEntry> iterator) {
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
        if (!iterator.hasNext()) {
            return null;
        }
        LogMinerEventEntry end = iterator.next();
        if (!(end.event() instanceof RollbackToSavepointEvent)) {
            return null;
        }
        LogMinerEvent rollbackEvent = end.event();
        EventType rollbackType = rollbackEvent.getEventType();
        LogMinerEventEntry rolledBackEntry = null;
        LogMinerEventEntry lobStartEntry = null;
        boolean lobStmt = false;
        while (iterator.hasNext()) {
            final LogMinerEventEntry entry = iterator.next();
            final LogMinerEvent event = entry.event();
            if (entry.event() instanceof RollbackToSavepointEvent) {
                end = entry;
                continue;
            }
            else if (!event.getTableId().equals(rollbackEvent.getTableId())) {
                logCannotApplyRollbackToSavepointWarning(transactionId, rollbackEvent, "TABLE_NAME", event.getTableId());
                return null;
            }
            else if (event.getRowId().equals(rollbackEvent.getRowId())) {
                if (event.getEventType() == EventType.INSERT && rollbackType == EventType.DELETE) {
                    return new LogMinerEventEntryRange(entry, end);
                }
                else if (event.getEventType() == EventType.DELETE && rollbackType == EventType.INSERT) {
                    return new LogMinerEventEntryRange(entry, end);
                }
                else if (event.getEventType() == EventType.SELECT_LOB_LOCATOR && rollbackType == EventType.UPDATE) {
                    logUnexpectedEventBeforeRollbackWarning(transactionId, rollbackEvent, "OPERATION", event.getEventType());
                    return new LogMinerEventEntryRange(entry, end);
                }
                else if ((event.getEventType() == EventType.LOB_WRITE || event.getEventType() == EventType.LOB_TRIM || event.getEventType() == EventType.LOB_ERASE)
                        && rollbackType == EventType.UPDATE) {
                    logUnexpectedEventBeforeRollbackWarning(transactionId, rollbackEvent, "OPERATION", event.getEventType());
                    lobStmt = true;
                    break;
                }
                else if ((event.getEventType() == EventType.UPDATE || event.getEventType() == EventType.INTERNAL)
                        && (rollbackType == EventType.DELETE || rollbackType == EventType.UPDATE)) {
                    rolledBackEntry = entry;
                    lobStmt = event.getEventType() == EventType.INTERNAL && rollbackType == EventType.UPDATE;
                    break;
                }
                logCannotApplyRollbackToSavepointWarning(transactionId, rollbackEvent, event.getEventType());
                return null;
            }
            else if (RowIdCodec.EMPTY_ROW_ID.equals(event.getRowId())) {
                logUnexpectedEventBeforeRollbackWarning(transactionId, rollbackEvent, "ROW_ID", event.getRowIdAsString());
                if (event.getEventType() == EventType.INSERT) {
                    if (rollbackType == EventType.DELETE) {
                        return new LogMinerEventEntryRange(entry, end);
                    }
                    logCannotApplyRollbackToSavepointWarning(transactionId, rollbackEvent, event.getEventType());
                    return null;
                }
                else if (event.getEventType() == EventType.UPDATE) {
                    if (rollbackType == EventType.UPDATE) {
                        return new LogMinerEventEntryRange(entry, end);
                    }
                    logCannotApplyRollbackToSavepointWarning(transactionId, rollbackEvent, event.getEventType());
                    return null;
                }
                else if (event.getEventType() == EventType.LOB_WRITE || event.getEventType() == EventType.LOB_TRIM || event.getEventType() == EventType.LOB_ERASE) {
                    lobStmt = true;
                }
                break;
            }
            logCannotApplyRollbackToSavepointWarning(transactionId, rollbackEvent, "ROW_ID", event.getRowIdAsString());
            return null;
        }
        while (iterator.hasNext()) {
            final LogMinerEventEntry entry = iterator.next();
            final LogMinerEvent event = entry.event();
            if (!event.getTableId().equals(rollbackEvent.getTableId())) {
                logUnexpectedEventWithEmptyRowIdWarning(transactionId, rollbackEvent, "TABLE_NAME", event.getTableId());
                break;
            }
            if (lobStmt) {
                if (event.getEventType() == EventType.SELECT_LOB_LOCATOR) {
                    if (event.getRowId().equals(rollbackEvent.getRowId())) {
                        return new LogMinerEventEntryRange(entry, end);
                    }
                }
                else if (event.getEventType() == EventType.LOB_WRITE || event.getEventType() == EventType.LOB_TRIM || event.getEventType() == EventType.LOB_ERASE) {
                    if (event.getRowId().equals(rollbackEvent.getRowId()) || RowIdCodec.EMPTY_ROW_ID.equals(event.getRowId())) {
                        continue;
                    }
                }
                lobStmt = false;
            }
            if (!RowIdCodec.EMPTY_ROW_ID.equals(event.getRowId()) || event.getEventType() == EventType.INTERNAL) {
                break;
            }
            if (event.getEventType() == EventType.INSERT || event.getEventType() == EventType.UPDATE) {
                if (lobStartEntry != null && !lobStartEntry.event().getRsId().equals(event.getRsId())) {
                    logUnexpectedEventWithEmptyRowIdWarning(transactionId, rollbackEvent, "RS_ID", event.getRsId());
                    break;
                }
                else if (event.getEventType() == EventType.INSERT && rollbackType == EventType.DELETE) {
                    return new LogMinerEventEntryRange(entry, end);
                }
                else if (event.getEventType() == EventType.UPDATE && rollbackType == EventType.UPDATE) {
                    return new LogMinerEventEntryRange(entry, end);
                }
                lobStartEntry = null;
                logUnexpectedEventWithEmptyRowIdWarning(transactionId, rollbackEvent, "OPERATION", event.getEventType());
                break;
            }
            else if (event.getEventType() == EventType.SELECT_LOB_LOCATOR || event.getEventType() == EventType.EXTENDED_STRING_BEGIN) {
                if (lobStartEntry != null && !lobStartEntry.event().getRsId().equals(event.getRsId())) {
                    logUnexpectedEventWithEmptyRowIdWarning(transactionId, rollbackEvent, "RS_ID", event.getRsId());
                    break;
                }
                lobStartEntry = entry;
            }
            else if (event.getEventType() == EventType.XML_BEGIN) {
                if (lobStartEntry != null) {
                    logUnexpectedEventWithEmptyRowIdWarning(transactionId, rollbackEvent, "OPERATION", event.getEventType());
                    break;
                }
                rolledBackEntry = entry;
            }
        }

        if (lobStartEntry != null) {
            rolledBackEntry = lobStartEntry;
        }
        else if (rolledBackEntry == null) {
            return null;
        }

        EventType rolledBackType = rolledBackEntry.event().getEventType();
        if ((rolledBackType == EventType.UPDATE
                || rolledBackType == EventType.SELECT_LOB_LOCATOR
                || rolledBackType == EventType.EXTENDED_STRING_BEGIN
                || rolledBackType == EventType.XML_BEGIN) && rollbackType != EventType.UPDATE) {
            logCannotApplyRollbackToSavepointWarning(transactionId, rollbackEvent, rolledBackType);
            return null;
        }
        return new LogMinerEventEntryRange(rolledBackEntry, end);
    }

    private void logCannotApplyRollbackToSavepointWarning(String transactionId, LogMinerEvent rollbackEvent, String fieldName, Object fieldValue) {
        Loggings.logWarningAndTraceRecord(LOGGER, rollbackEvent,
                "Cannot apply the undo change in transaction '{}' with SCN '{}' on table '{}' by row-id '{}' since the preceding event in the transaction cache has a different {} '{}'. Manual investigation is required.",
                transactionId, rollbackEvent.getScn(), rollbackEvent.getTableId(), rollbackEvent.getRowIdAsString(), fieldName, fieldValue);
    }

    private void logCannotApplyRollbackToSavepointWarning(String transactionId, LogMinerEvent rollbackEvent, EventType rolledBackType) {
        Loggings.logWarningAndTraceRecord(LOGGER, rollbackEvent,
                "Cannot apply the undo change in transaction '{}' with SCN '{}' on table '{}' by row-id '{}' since '{}' was not expected before '{}'. Manual investigation is required.",
                transactionId, rollbackEvent.getScn(), rollbackEvent.getTableId(), rollbackEvent.getRowIdAsString(), rolledBackType, rollbackEvent.getEventType());
    }

    private void logUnexpectedEventBeforeRollbackWarning(String transactionId, LogMinerEvent rollbackEvent, String fieldName, Object fieldValue) {
        Loggings.logWarningAndTraceRecord(LOGGER, rollbackEvent,
                "An event with an unexpected {} '{}' is followed by the rollback event in transaction '{}' with SCN '{}' on table '{}' by row-id '{}'. Please enable 'log.mining.include.internal.events'.",
                fieldName, fieldValue, transactionId, rollbackEvent.getScn(), rollbackEvent.getTableId(), rollbackEvent.getRowIdAsString());
    }

    private void logUnexpectedEventWithEmptyRowIdWarning(String transactionId, LogMinerEvent rollbackEvent, String fieldName, Object fieldValue) {
        Loggings.logWarningAndTraceRecord(LOGGER, rollbackEvent,
                "An event with an empty ROW_ID and an unexpected {} '{}' was detected while applying the undo change in transaction '{}' with SCN '{}' on table '{}' by row-id '{}'. Please enable 'log.mining.include.internal.events'.",
                fieldName, fieldValue, transactionId, rollbackEvent.getScn(), rollbackEvent.getTableId(), rollbackEvent.getRowIdAsString());
    }

    private int compareTransactionScnDetails(T first, T second) {
        int scnComparison = first.getStartScn().compareTo(second.getStartScn());
        if (scnComparison != 0) {
            return scnComparison;
        }
        return first.getChangeTime().compareTo(second.getChangeTime());
    }

    /**
     * An event record used to map event-id and event.
     *
     * @param eventId the event's unique identifier
     * @param event the event object
     */
    public record LogMinerEventEntry(int eventId, LogMinerEvent event) {
    }

    public record LogMinerEventEntryRange(LogMinerEventEntry start, LogMinerEventEntry end) {
    }

    protected static class LogMinerEventEntryIterator implements Iterator<LogMinerEventEntry> {
        private final Iterator<Integer> eventIdIterator;
        private final IntFunction<LogMinerEvent> getTransactionEvent;

        public LogMinerEventEntryIterator(Iterator<Integer> eventIdIterator, IntFunction<LogMinerEvent> getTransactionEvent) {
            this.eventIdIterator = eventIdIterator;
            this.getTransactionEvent = getTransactionEvent;
        }

        @Override
        public boolean hasNext() {
            return eventIdIterator.hasNext();
        }

        @Override
        public LogMinerEventEntry next() {
            final int eventId = eventIdIterator.next();
            return new LogMinerEventEntry(eventId, getTransactionEvent.apply(eventId));
        }
    }
}
