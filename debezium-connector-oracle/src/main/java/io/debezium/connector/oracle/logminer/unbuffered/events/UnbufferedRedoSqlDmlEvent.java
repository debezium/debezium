/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.unbuffered.events;

import io.debezium.connector.oracle.logminer.events.LogMinerEventRow;
import io.debezium.connector.oracle.logminer.parser.LogMinerDmlEntry;

/**
 * An unbuffered specialization of {@link UnbufferedDmlEvent} that stores the LogMiner REDO SQL statements.
 *
 * @author Chris Cranford
 */
public class UnbufferedRedoSqlDmlEvent extends UnbufferedDmlEvent {

    private final String redoSql;

    public UnbufferedRedoSqlDmlEvent(LogMinerEventRow row, LogMinerDmlEntry dmlEntry, String redoSql) {
        super(row, dmlEntry);
        this.redoSql = redoSql;
    }

    public String getRedoSql() {
        return redoSql;
    }

    @Override
    public String toString() {
        return "UnbufferedRedoSqlDmlEvent{" +
                "redoSql='" + redoSql + '\'' +
                "} " + super.toString();
    }
}
