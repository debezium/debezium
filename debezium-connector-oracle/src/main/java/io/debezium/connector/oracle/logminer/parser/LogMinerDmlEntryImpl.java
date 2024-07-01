/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.parser;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

import io.debezium.connector.oracle.logminer.events.EventType;
import io.debezium.connector.oracle.logminer.processor.infinispan.marshalling.VisibleForMarshalling;

/**
 * This class holds one parsed DML LogMiner record details
 *
 */
public class LogMinerDmlEntryImpl implements LogMinerDmlEntry, Serializable {

    private static final long serialVersionUID = 1L;

    private EventType eventType;
    private Object[] newValues;
    private Object[] oldValues;
    private String objectOwner;
    private String objectName;

    @VisibleForMarshalling
    public LogMinerDmlEntryImpl(int eventType, Object[] newValues, Object[] oldValues, String owner, String name) {
        this.eventType = EventType.from(eventType);
        this.newValues = newValues;
        this.oldValues = oldValues;
        this.objectOwner = owner;
        this.objectName = name;
    }

    private LogMinerDmlEntryImpl(EventType eventType, Object[] newValues, Object[] oldValues) {
        this.eventType = eventType;
        this.newValues = newValues;
        this.oldValues = oldValues;
    }

    public static LogMinerDmlEntry forInsert(Object[] newColumnValues) {
        return new LogMinerDmlEntryImpl(EventType.INSERT, newColumnValues, new Object[0]);
    }

    public static LogMinerDmlEntry forUpdate(Object[] newColumnValues, Object[] oldColumnValues) {
        return new LogMinerDmlEntryImpl(EventType.UPDATE, newColumnValues, oldColumnValues);
    }

    public static LogMinerDmlEntry forDelete(Object[] oldColumnValues) {
        return new LogMinerDmlEntryImpl(EventType.DELETE, new Object[0], oldColumnValues);
    }

    public static LogMinerDmlEntry forValuelessDdl() {
        return new LogMinerDmlEntryImpl(EventType.DDL, new Object[0], new Object[0]);
    }

    public static LogMinerDmlEntry forLobLocator(Object[] newColumnValues) {
        // TODO: can that copy be avoided?
        final Object[] oldColumnValues = Arrays.copyOf(newColumnValues, newColumnValues.length);
        return new LogMinerDmlEntryImpl(EventType.SELECT_LOB_LOCATOR, newColumnValues, oldColumnValues);
    }

    public static LogMinerDmlEntry forXml(Object[] oldColumnValues) {
        // TODO: can that copy be avoided?
        final Object[] newColumnValues = Arrays.copyOf(oldColumnValues, oldColumnValues.length);
        return new LogMinerDmlEntryImpl(EventType.XML_BEGIN, newColumnValues, oldColumnValues);
    }

    @Override
    public EventType getEventType() {
        return eventType;
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
        return eventType == that.eventType &&
                Arrays.equals(newValues, that.newValues) &&
                Arrays.equals(oldValues, that.oldValues);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventType, newValues, oldValues);
    }

    @Override
    public String toString() {
        return "{LogMinerDmlEntryImpl={eventType=" + eventType + ",newColumns=" + newValues + ",oldColumns=" + oldValues + "}";
    }
}
