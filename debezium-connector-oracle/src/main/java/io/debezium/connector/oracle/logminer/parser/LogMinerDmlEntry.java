/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.parser;

import io.debezium.connector.oracle.logminer.events.EventType;

public interface LogMinerDmlEntry {
    /**
     * @return object array that contains the before state, values from WHERE clause.
     */
    Object[] getOldValues();

    /**
     * @return object array that contains the after state, values from an insert's
     * values list or the values in the SET clause of an update statement.
     */
    Object[] getNewValues();

    /**
     * @return LogMiner event type
     */
    EventType getEventType();

    /**
     * @return schema name
     */
    String getObjectOwner();

    /**
     * @return table name
     */
    String getObjectName();

    /**
     * Sets table name
     * @param name table name
     */
    void setObjectName(String name);

    /**
     * Sets schema owner
     * @param name schema owner
     */
    void setObjectOwner(String name);
}
