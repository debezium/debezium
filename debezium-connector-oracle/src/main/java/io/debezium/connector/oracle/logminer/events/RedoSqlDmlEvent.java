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

/**
 * A specialization of {@link DmlEvent} that also stores the LogMiner REDO SQL statements.
 *
 * @author Chris Cranford
 */
public class RedoSqlDmlEvent extends DmlEvent {

    private final String redoSql;

    public RedoSqlDmlEvent(LogMinerEventRow row, LogMinerDmlEntry dmlEntry, String redoSql) {
        super(row, dmlEntry);
        this.redoSql = redoSql;
    }

    public RedoSqlDmlEvent(EventType eventType, Scn scn, TableId tableId, String rowId, String rsId, Instant changeTime,
                           Object[] oldValues, Object[] newValues, String redoSql) {
        super(eventType, scn, tableId, rowId, rsId, changeTime, oldValues, newValues);
        this.redoSql = redoSql;
    }

    public String getRedoSql() {
        return redoSql;
    }

}
