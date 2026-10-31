/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.events;

import io.debezium.connector.oracle.Scn;
import io.debezium.relational.TableId;

/**
 * The fields of a cached event that the transaction caches read themselves, to attribute a partial rollback to
 * the events it undoes. Everything else an event carries is opaque to the cache.
 *
 * @author Chris Cranford
 */
public interface CachedEvent {

    EventType getEventType();

    /**
     * Whether the event is an undo marker, a row LogMiner flagged with {@code ROLLBACK = 1} that reverts an
     * earlier change of the same transaction. The event type for an undo marker is the ordinary type of the
     * reverting operation, so only this flag tells it apart.
     */
    boolean isRollback();

    Scn getScn();

    TableId getTableId();

    RowIdCodec.Packed getRowId();

    default String getRowIdAsString() {
        return RowIdCodec.decode(getRowId());
    }

    String getRsId();

    /**
     * The row's {@code SEQUENCE#}, which distinguishes the XML operation groups of one statement. Events that do
     * not record it return {@code 0}.
     */
    long getTransactionSequence();
}
