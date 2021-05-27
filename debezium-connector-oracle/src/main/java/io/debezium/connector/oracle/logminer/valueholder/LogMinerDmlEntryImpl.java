/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.valueholder;

import java.util.Arrays;
import java.util.Objects;

import io.debezium.connector.oracle.logminer.RowMapper;

/**
 * This class holds one parsed DML LogMiner record details
 *
 */
public class LogMinerDmlEntryImpl implements LogMinerDmlEntry {

    private final int operation;
    private final Object[] newValues;
    private final Object[] oldValues;
    private String objectOwner;
    private String objectName;

    private LogMinerDmlEntryImpl(int operation, Object[] newValues, Object[] oldValues) {
        this.operation = operation;
        this.newValues = newValues;
        this.oldValues = oldValues;
    }

    public static LogMinerDmlEntry forInsert(Object[] newColumnValues) {
        return new LogMinerDmlEntryImpl(RowMapper.INSERT, newColumnValues, new Object[0]);
    }

    public static LogMinerDmlEntry forUpdate(Object[] newColumnValues, Object[] oldColumnValues) {
        return new LogMinerDmlEntryImpl(RowMapper.UPDATE, newColumnValues, oldColumnValues);
    }

    public static LogMinerDmlEntry forDelete(Object[] oldColumnValues) {
        return new LogMinerDmlEntryImpl(RowMapper.DELETE, new Object[0], oldColumnValues);
    }

    public static LogMinerDmlEntry forLobLocator(Object[] newColumnValues) {
        // TODO: can that copy be avoided?
        final Object[] oldColumnValues = Arrays.copyOf(newColumnValues, newColumnValues.length);
        return new LogMinerDmlEntryImpl(RowMapper.SELECT_LOB_LOCATOR, newColumnValues, oldColumnValues);
    }

    @Override
    public int getOperation() {
        return operation;
    }

    @Override
    public Object[] getOldValues() {
        return oldValues;
    }

    @Override
    public Object[] getNewValues() {
        return newValues;
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
    public void setObjectName(String name) {
        this.objectName = name;
    }

    @Override
    public void setObjectOwner(String name) {
        this.objectOwner = name;
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
                Arrays.equals(newValues, that.newValues) &&
                Arrays.equals(oldValues, that.oldValues);
    }

    @Override
    public int hashCode() {
        return Objects.hash(operation, newValues, oldValues);
    }

    @Override
    public String toString() {
        return "{LogMinerDmlEntryImpl={operation=" + operation + ",newColumns=" + newValues + ",oldColumns=" + oldValues + "}";
    }
}
