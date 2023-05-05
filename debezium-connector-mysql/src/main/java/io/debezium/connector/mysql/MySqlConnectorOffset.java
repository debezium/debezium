/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.time.Instant;

import io.debezium.document.Document;

public class MySqlConnectorOffset {

    private final Document doc;

    public MySqlConnectorOffset(Document document) {
        this.doc = document;
    }

    public MySqlConnectorOffset(String gtidSet, String binlogFilename, long binlogPosition, int serverId, long tsSec) {
        this.doc = Document.create();
        doc.setString(MySqlOffsetContext.GTID_SET_KEY, gtidSet);
        doc.setString(SourceInfo.BINLOG_FILENAME_OFFSET_KEY, binlogFilename);
        doc.setNumber(SourceInfo.BINLOG_POSITION_OFFSET_KEY, binlogPosition);
        doc.setNumber(SourceInfo.SERVER_ID_KEY, serverId);
        doc.setNumber(MySqlOffsetContext.TIMESTAMP_KEY, tsSec);
    }

    public Document document() {
        return this.doc;
    }

    protected Instant timestamp() {
        return Instant.ofEpochSecond(doc.getLong(MySqlOffsetContext.TIMESTAMP_KEY, 0));
    }

    protected String file() {
        return doc.getString(SourceInfo.BINLOG_FILENAME_OFFSET_KEY);
    }

    protected Long position() {
        return doc.getLong(SourceInfo.BINLOG_POSITION_OFFSET_KEY, 0);
    }

    protected String gtids() {
        return doc.getString(MySqlOffsetContext.GTID_SET_KEY);
    }

    protected Integer row() {
        return doc.getInteger(SourceInfo.BINLOG_ROW_IN_EVENT_OFFSET_KEY, 0);
    }

    protected Long serverId() {
        return doc.getLong(SourceInfo.SERVER_ID_KEY, 0);
    }

    protected Long event() {
        return doc.getLong(MySqlOffsetContext.EVENTS_TO_SKIP_OFFSET_KEY, 0);
    }

    @Override
    public String toString() {
        return doc.toString();
    }
}
