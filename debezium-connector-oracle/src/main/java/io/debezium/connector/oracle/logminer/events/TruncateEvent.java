/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.oracle.logminer.events;

import java.time.Instant;

import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.parser.LogMinerDmlEntry;
import io.debezium.relational.TableId;

public class TruncateEvent extends DmlEvent {
    public TruncateEvent(LogMinerEventRow row, LogMinerDmlEntry dmlEntry) {
        super(row, dmlEntry);
    }

    public TruncateEvent(EventType eventType, Scn scn, TableId tableId, String rowId, String rsId,
                         Instant changeTime, LogMinerDmlEntry dmlEntry) {
        super(eventType, scn, tableId, rowId, rsId, changeTime, dmlEntry);
    }
}
