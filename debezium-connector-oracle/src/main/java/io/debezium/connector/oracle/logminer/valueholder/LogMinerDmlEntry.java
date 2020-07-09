/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.valueholder;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.List;

import io.debezium.data.Envelope;

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
     * this getter
     * @return Envelope.Operation enum
     */
    Envelope.Operation getCommandType();

    /**
     * the scn obtained from a Log Miner entry.
     * This SCN is not a final SCN, just a candidate.
     * The actual SCN will be assigned after commit
     * @return it's value
     */
    BigDecimal getScn();

    /**
     * @return transaction ID
     */
    String getTransactionId();

    /**
     * @return schema name
     */
    String getObjectOwner();

    /**
     * @return table name
     */
    String getObjectName();

    /**
     * @return database change time of this logical record
     */
    Timestamp getSourceTime();

    /**
     * sets scn obtained from a Log Miner entry
     * @param scn it's value
     */
    void setScn(BigDecimal scn);

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

    /**
     * Sets the time of the database change
     * @param changeTime the time of the change
     */
    void setSourceTime(Timestamp changeTime);

    /**
     * @param id unique transaction ID
     */
    void setTransactionId(String id);

}
