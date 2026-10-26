/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered.infinispan.marshalling;

import java.time.Instant;

import org.infinispan.protostream.annotations.ProtoAdapter;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;

import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.events.EventType;
import io.debezium.connector.oracle.logminer.events.LogMinerRedoEvent;
import io.debezium.relational.TableId;

/**
 * The ProtoStream adapter for {@link LogMinerRedoEvent}, the one type the events cache stores.
 *
 * @author Chris Cranford
 */
@ProtoAdapter(LogMinerRedoEvent.class)
public class LogMinerRedoEventAdapter {

    @ProtoFactory
    public LogMinerRedoEvent factory(int eventType, boolean rollback, String scn, String tableId, String rowId, String rsId, String changeTime,
                                     String redoSql, int status, String info, long transactionSequence, long objectId, long dataObjectId) {
        return new LogMinerRedoEvent(
                EventType.from(eventType),
                rollback,
                Scn.valueOf(scn),
                TableId.parse(tableId),
                rowId,
                rsId,
                Instant.parse(changeTime),
                redoSql,
                status,
                info,
                transactionSequence,
                objectId,
                dataObjectId);
    }

    @ProtoField(number = 1, defaultValue = "0")
    public int getEventType(LogMinerRedoEvent event) {
        return event.getEventType().getValue();
    }

    @ProtoField(number = 2, defaultValue = "false")
    public boolean getRollback(LogMinerRedoEvent event) {
        return event.isRollback();
    }

    @ProtoField(number = 3)
    public String getScn(LogMinerRedoEvent event) {
        return event.getScn().toString();
    }

    @ProtoField(number = 4)
    public String getTableId(LogMinerRedoEvent event) {
        return event.getTableId().toDoubleQuotedString();
    }

    @ProtoField(number = 5)
    public String getRowId(LogMinerRedoEvent event) {
        return event.getRowIdAsString();
    }

    @ProtoField(number = 6)
    public String getRsId(LogMinerRedoEvent event) {
        return event.getRsId();
    }

    @ProtoField(number = 7)
    public String getChangeTime(LogMinerRedoEvent event) {
        return event.getChangeTime().toString();
    }

    @ProtoField(number = 8)
    public String getRedoSql(LogMinerRedoEvent event) {
        return event.getRedoSql();
    }

    @ProtoField(number = 9, defaultValue = "0")
    public int getStatus(LogMinerRedoEvent event) {
        return event.getStatus();
    }

    @ProtoField(number = 10)
    public String getInfo(LogMinerRedoEvent event) {
        return event.getInfo();
    }

    @ProtoField(number = 11, defaultValue = "0")
    public long getTransactionSequence(LogMinerRedoEvent event) {
        return event.getTransactionSequence();
    }

    @ProtoField(number = 12, defaultValue = "0")
    public long getObjectId(LogMinerRedoEvent event) {
        return event.getObjectId();
    }

    @ProtoField(number = 13, defaultValue = "0")
    public long getDataObjectId(LogMinerRedoEvent event) {
        return event.getDataObjectId();
    }
}
