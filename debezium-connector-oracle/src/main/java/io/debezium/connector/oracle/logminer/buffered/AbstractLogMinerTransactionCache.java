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

    protected LogMinerEventEntry findFirstRolledBackEventEntry(T transaction, Iterator<LogMinerEventEntry> iterator, RollbackToSavepointEvent rollbackEvent) {
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
        String transactionId = transaction.getTransactionId();
        EventType rollbackType = rollbackEvent.getEventType();
        LogMinerEventEntry rolledBackEntry = null;
        LogMinerEventEntry lobStartEntry = null;
        boolean lobStmt = false;
        while (iterator.hasNext()) {
            final LogMinerEventEntry entry = iterator.next();
            final LogMinerEvent event = entry.event();
            if (entry.event() instanceof RollbackToSavepointEvent) {
                continue;
            }
            else if (!event.getTableId().equals(rollbackEvent.getTableId())) {
                Loggings.logWarningAndTraceRecord(LOGGER, rollbackEvent,
                        "Cannot apply the undo change in transaction '{}' with SCN '{}' on table '{}' by row-id '{}' since the preceding event in the transaction cache is from table '{}'. Manual investigation is required.",
                        transactionId, rollbackEvent.getScn(), rollbackEvent.getTableId(), rollbackEvent.getRowId(), event.getTableId());
                return null;
            }
            else if (event.getRowId().equals(rollbackEvent.getRowId())) {
                if (event.getEventType() == EventType.INSERT && rollbackType == EventType.DELETE) {
                    return entry;
                }
                else if (event.getEventType() == EventType.DELETE && rollbackType == EventType.INSERT) {
                    return entry;
                }
                else if (event.getEventType() == EventType.SELECT_LOB_LOCATOR && rollbackType == EventType.UPDATE) {
                    LOGGER.warn(
                            "An event with an unexpected OPERATION '{}' is followed by the rollback event in transaction '{}' with SCN '{}' on table '{}' by row-id '{}'. Please enable 'log.mining.include.internal.events'.",
                            event.getEventType(), transactionId, rollbackEvent.getScn(), rollbackEvent.getTableId(), rollbackEvent.getRowId());
                    return entry;
                }
                else if ((event.getEventType() == EventType.LOB_WRITE || event.getEventType() == EventType.LOB_TRIM || event.getEventType() == EventType.LOB_ERASE)
                        && rollbackType == EventType.UPDATE) {
                    LOGGER.warn(
                            "An event with an unexpected OPERATION '{}' is followed by the rollback event in transaction '{}' with SCN '{}' on table '{}' by row-id '{}'. Please enable 'log.mining.include.internal.events'.",
                            event.getEventType(), transactionId, rollbackEvent.getScn(), rollbackEvent.getTableId(), rollbackEvent.getRowId());
                    lobStmt = true;
                    break;
                }
                else if ((event.getEventType() == EventType.UPDATE || event.getEventType() == EventType.INTERNAL)
                        && (rollbackType == EventType.DELETE || rollbackType == EventType.UPDATE)) {
                    rolledBackEntry = entry;
                    lobStmt = event.getEventType() == EventType.INTERNAL && rollbackType == EventType.UPDATE;
                    break;
                }
                Loggings.logWarningAndTraceRecord(LOGGER, rollbackEvent,
                        "Cannot apply the undo change in transaction '{}' with SCN '{}' on table '{}' by row-id '{}' since '{}' was not expected before '{}'. Manual investigation is required.",
                        transactionId, rollbackEvent.getScn(), rollbackEvent.getTableId(), rollbackEvent.getRowId(), event.getEventType(), rollbackType);
                return null;
            }
            else if (RowIdCodec.EMPTY_ROW_ID.equals(event.getRowId())) {
                LOGGER.warn(
                        "An event with an empty row-id is followed by the rollback event in transaction '{}' with SCN '{}' on table '{}' by row-id '{}'. Please enable 'log.mining.include.internal.events'.",
                        transactionId, rollbackEvent.getScn(), rollbackEvent.getTableId(), rollbackEvent.getRowId());
                if (event.getEventType() == EventType.INSERT) {
                    if (rollbackType == EventType.DELETE) {
                        return entry;
                    }
                    Loggings.logWarningAndTraceRecord(LOGGER, rollbackEvent,
                            "Cannot apply the undo change in transaction '{}' with SCN '{}' on table '{}' by row-id '{}' since '{}' was not expected before '{}'. Manual investigation is required.",
                            transactionId, rollbackEvent.getScn(), rollbackEvent.getTableId(), rollbackEvent.getRowId(), event.getEventType(), rollbackType);
                    return null;
                }
                else if (event.getEventType() == EventType.UPDATE) {
                    if (rollbackType == EventType.UPDATE) {
                        return entry;
                    }
                    Loggings.logWarningAndTraceRecord(LOGGER, rollbackEvent,
                            "Cannot apply the undo change in transaction '{}' with SCN '{}' on table '{}' by row-id '{}' since '{}' was not expected before '{}'. Manual investigation is required.",
                            transactionId, rollbackEvent.getScn(), rollbackEvent.getTableId(), rollbackEvent.getRowId(), event.getEventType(), rollbackType);
                    return null;
                }
                else if (event.getEventType() == EventType.LOB_WRITE || event.getEventType() == EventType.LOB_TRIM || event.getEventType() == EventType.LOB_ERASE) {
                    lobStmt = true;
                }
                break;
            }
            Loggings.logWarningAndTraceRecord(LOGGER, rollbackEvent,
                    "Cannot apply the undo change in transaction '{}' with SCN '{}' on table '{}' by row-id '{}' since the preceding event in the transaction cache has a different row-id '{}'. Manual investigation is required.",
                    transactionId, rollbackEvent.getScn(), rollbackEvent.getTableId(), rollbackEvent.getRowId(), event.getRowId());
            return null;
        }
        while (iterator.hasNext()) {
            final LogMinerEventEntry entry = iterator.next();
            final LogMinerEvent event = entry.event();
            if (!event.getTableId().equals(rollbackEvent.getTableId())) {
                LOGGER.warn(
                        "An event with an empty ROW_ID and an unexpected TABLE_NAME '{}' was detected while applying the undo change in transaction '{}' with SCN '{}' on table '{}' by row-id '{}'. Please enable 'log.mining.include.internal.events'.",
                        event.getTableId(), transactionId, rollbackEvent.getScn(), rollbackEvent.getTableId(), rollbackEvent.getRowId());
                break;
            }
            if (lobStmt) {
                if (event.getEventType() == EventType.SELECT_LOB_LOCATOR) {
                    if (event.getRowId().equals(rollbackEvent.getRowId())) {
                        return entry;
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
                    LOGGER.warn(
                            "An event with an empty ROW_ID and an unexpected RS_ID '{}' was detected while applying the undo change in transaction '{}' with SCN '{}' on table '{}' by row-id '{}'. Please enable 'log.mining.include.internal.events'.",
                            event.getRsId(), transactionId, rollbackEvent.getScn(), rollbackEvent.getTableId(), rollbackEvent.getRowId());
                    break;
                }
                else if (event.getEventType() == EventType.INSERT && rollbackType == EventType.DELETE) {
                    return entry;
                }
                else if (event.getEventType() == EventType.UPDATE && rollbackType == EventType.UPDATE) {
                    return entry;
                }
                lobStartEntry = null;
                LOGGER.warn(
                        "An event with an empty ROW_ID and an unexpected OPERATION '{}' was detected while applying the undo change in transaction '{}' with SCN '{}' on table '{}' by row-id '{}'. Please enable 'log.mining.include.internal.events'.",
                        event.getEventType(), transactionId, rollbackEvent.getScn(), rollbackEvent.getTableId(), rollbackEvent.getRowId());
                break;
            }
            else if (event.getEventType() == EventType.SELECT_LOB_LOCATOR || event.getEventType() == EventType.EXTENDED_STRING_BEGIN) {
                if (lobStartEntry != null && !lobStartEntry.event().getRsId().equals(event.getRsId())) {
                    LOGGER.warn(
                            "An event with an empty ROW_ID and an unexpected RS_ID '{}' was detected while applying the undo change in transaction '{}' with SCN '{}' on table '{}' by row-id '{}'. Please enable 'log.mining.include.internal.events'.",
                            event.getRsId(), transactionId, rollbackEvent.getScn(), rollbackEvent.getTableId(), rollbackEvent.getRowId());
                    break;
                }
                lobStartEntry = entry;
            }
            else if (event.getEventType() == EventType.XML_BEGIN) {
                if (lobStartEntry != null) {
                    LOGGER.warn(
                            "An unexpected XML_BEGIN event was detected while applying the undo change in transaction '{}' with SCN '{}' on table '{}' by row-id '{}'. Please enable 'log.mining.include.internal.events'.",
                            transactionId, rollbackEvent.getScn(), rollbackEvent.getTableId(), rollbackEvent.getRowId());
                    break;
                }
                rolledBackEntry = entry;
            }
        }

        if (lobStartEntry != null) {
            rolledBackEntry = lobStartEntry;
        }

        EventType rolledBackType = rolledBackEntry.event().getEventType();
        if ((rolledBackType == EventType.UPDATE
                || rolledBackType == EventType.SELECT_LOB_LOCATOR
                || rolledBackType == EventType.EXTENDED_STRING_BEGIN
                || rolledBackType == EventType.XML_BEGIN) && rollbackType != EventType.UPDATE) {
            Loggings.logWarningAndTraceRecord(LOGGER, rollbackEvent,
                    "Cannot apply the undo change in transaction '{}' with SCN '{}' on table '{}' by row-id '{}' since '{}' was not expected before '{}'. Manual investigation is required.",
                    transactionId, rollbackEvent.getScn(), rollbackEvent.getTableId(), rollbackEvent.getRowId(), rolledBackType, rollbackType);
            return null;
        }
        return rolledBackEntry;
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
