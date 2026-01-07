/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.events;

import java.time.Instant;

import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.parser.LobWriteParser;
import io.debezium.relational.TableId;

/**
 * A LogMiner event that represents a {@code LOB_WRITE} operation.
 *
 * @author Chris Cranford
 */
public class LobWriteEvent extends LogMinerEvent {

    private final String data;
    private final int offset;
    private final int length;

    public LobWriteEvent(LogMinerEventRow row, LobWriteParser.LobWrite parsedEvent) {
        this(row, parsedEvent.data(), parsedEvent.offset(), parsedEvent.length());
    }

    public LobWriteEvent(LogMinerEventRow row, String data, int offset, int length) {
        super(row);
        this.data = data;
        this.offset = offset;
        this.length = length;
    }

    public LobWriteEvent(EventType eventType, Scn scn, TableId tableId, String rowId, String rsId, Instant changeTime, String data, int offset, int length) {
        super(eventType, scn, tableId, rowId, rsId, changeTime);
        this.data = data;
        this.offset = offset;
        this.length = length;
    }

    public String getData() {
        return data;
    }

    public int getOffset() {
        return offset;
    }

    public int getLength() {
        return length;
    }

    @Override
    public String toString() {
        return "LobWriteEvent{" +
                "offset=" + offset + ", " +
                "length=" + length + ", " +
                "data='" + data + '\'' +
                "} " + super.toString();
    }
}
