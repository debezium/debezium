/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.valueholder;

import java.sql.Timestamp;
import java.util.List;
import java.util.Objects;

import io.debezium.connector.oracle.Scn;

/**
 * This class holds one parsed DML LogMiner record details
 *
 */
public class LogMinerDmlEntryImpl implements LogMinerDmlEntry {

    private final int operation;
    private final List<LogMinerColumnValue> newLmColumnValues;
    private final List<LogMinerColumnValue> oldLmColumnValues;
    private String objectOwner;
    private String objectName;
    private Timestamp sourceTime;
    private String transactionId;
    private Scn scn;
    private String rowId;
    private int sequence;

    public LogMinerDmlEntryImpl(int operation, List<LogMinerColumnValue> newLmColumnValues, List<LogMinerColumnValue> oldLmColumnValues) {
        this.operation = operation;
        this.newLmColumnValues = newLmColumnValues;
        this.oldLmColumnValues = oldLmColumnValues;
    }

    @Override
    public int getOperation() {
        return operation;
    }

    @Override
    public List<LogMinerColumnValue> getOldValues() {
        return oldLmColumnValues;
    }

    @Override
    public List<LogMinerColumnValue> getNewValues() {
        return newLmColumnValues;
    }

    @Override
    public String getTransactionId() {
        return transactionId;
    }

    @Override
    public String getObjectOwner() {
        return objectOwner;
    }

    @Override
    public String getObjectName() {
        return objectName;
    }

    @Override
    public Timestamp getSourceTime() {
        return sourceTime;
    }

    @Override
    public String getRowId() {
        return rowId;
    }

    @Override
    public int getSequence() {
        return sequence;
    }

    @Override
    public void setObjectName(String name) {
        this.objectName = name;
    }

    @Override
    public void setObjectOwner(String name) {
        this.objectOwner = name;
    }

    @Override
    public void setSourceTime(Timestamp changeTime) {
        this.sourceTime = changeTime;
    }

    @Override
    public void setTransactionId(String id) {
        this.transactionId = id;
    }

    @Override
    public Scn getScn() {
        return scn;
    }

    @Override
    public void setScn(Scn scn) {
        this.scn = scn;
    }

    @Override
    public void setRowId(String rowId) {
        this.rowId = rowId;
    }

    @Override
    public void setSequence(int sequence) {
        this.sequence = sequence;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LogMinerDmlEntryImpl that = (LogMinerDmlEntryImpl) o;
        return operation == that.operation &&
                Objects.equals(newLmColumnValues, that.newLmColumnValues) &&
                Objects.equals(oldLmColumnValues, that.oldLmColumnValues);
    }

    @Override
    public int hashCode() {
        return Objects.hash(operation, newLmColumnValues, oldLmColumnValues);
    }

    @Override
    public String toString() {
        return "{LogMinerDmlEntryImpl={operation=" + operation + ",newColumns=" + newLmColumnValues + ",oldColumns=" + oldLmColumnValues + "}";
    }
}
