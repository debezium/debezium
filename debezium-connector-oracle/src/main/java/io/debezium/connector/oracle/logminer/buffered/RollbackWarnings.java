/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.oracle.logminer.events.CachedEvent;
import io.debezium.connector.oracle.logminer.events.EventType;
import io.debezium.util.Loggings;

/**
 * The warnings the transaction caches log when an undo change cannot be matched to the change it reverts,
 * shared by the per-event caches and the packed heap buffer so that they read the same whichever buffer is
 * in use.
 *
 * @author Chris Cranford
 */
public final class RollbackWarnings {

    private static final Logger LOGGER = LoggerFactory.getLogger(RollbackWarnings.class);

    private RollbackWarnings() {
    }

    public static void cannotApplyRollbackToSavepoint(String transactionId, CachedEvent rollbackEvent, String fieldName, Object fieldValue) {
        Loggings.logWarningAndTraceRecord(LOGGER, rollbackEvent,
                "Cannot apply the undo change in transaction '{}' with SCN '{}' on table '{}' by row-id '{}' since the preceding event in the transaction cache has a different {} '{}'. Manual investigation is required.",
                transactionId, rollbackEvent.getScn(), rollbackEvent.getTableId(), rollbackEvent.getRowIdAsString(), fieldName, fieldValue);
    }

    public static void cannotApplyRollbackToSavepoint(String transactionId, CachedEvent rollbackEvent, EventType rolledBackType) {
        Loggings.logWarningAndTraceRecord(LOGGER, rollbackEvent,
                "Cannot apply the undo change in transaction '{}' with SCN '{}' on table '{}' by row-id '{}' since '{}' was not expected before '{}'. Manual investigation is required.",
                transactionId, rollbackEvent.getScn(), rollbackEvent.getTableId(), rollbackEvent.getRowIdAsString(), rolledBackType, rollbackEvent.getEventType());
    }

    public static void unexpectedEventBeforeRollback(String transactionId, CachedEvent rollbackEvent, String fieldName, Object fieldValue) {
        Loggings.logWarningAndTraceRecord(LOGGER, rollbackEvent,
                "An event with an unexpected {} '{}' is followed by the rollback event in transaction '{}' with SCN '{}' on table '{}' by row-id '{}'. Please enable 'log.mining.include.internal.events'.",
                fieldName, fieldValue, transactionId, rollbackEvent.getScn(), rollbackEvent.getTableId(), rollbackEvent.getRowIdAsString());
    }

    public static void unexpectedEventWithEmptyRowId(String transactionId, CachedEvent rollbackEvent, String fieldName, Object fieldValue) {
        Loggings.logWarningAndTraceRecord(LOGGER, rollbackEvent,
                "An event with an empty ROW_ID and an unexpected {} '{}' was detected while applying the undo change in transaction '{}' with SCN '{}' on table '{}' by row-id '{}'. Please enable 'log.mining.include.internal.events'.",
                fieldName, fieldValue, transactionId, rollbackEvent.getScn(), rollbackEvent.getTableId(), rollbackEvent.getRowIdAsString());
    }
}
