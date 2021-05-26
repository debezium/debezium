/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.valueholder;

import java.util.List;

public interface LogMinerDmlEntry {
    /**
     * This getter
     * @return old(current) values of the database record.
     * They represent values in WHERE clauses
     */
    List<LogMinerColumnValue> getOldValues();

    /**
     * this getter
     * @return new values to be applied to the database record
     * Those values are applicable for INSERT and UPDATE statements
     */
    List<LogMinerColumnValue> getNewValues();

    /**
     * @return LogMiner event operation type
     */
    int getOperation();

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
