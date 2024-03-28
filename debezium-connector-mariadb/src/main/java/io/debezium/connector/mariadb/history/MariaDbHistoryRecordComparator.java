/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb.history;

import java.util.function.Predicate;

import io.debezium.connector.binlog.gtid.GtidSetFactory;
import io.debezium.connector.binlog.history.BinlogHistoryRecordComparator;
import io.debezium.connector.mariadb.MariaDbOffsetContext;
import io.debezium.connector.mariadb.SourceInfo;
import io.debezium.document.Document;

/**
 * Schema history record comparator implementation for MariaDB.
 *
 * @author Chris Cranford
 */
public class MariaDbHistoryRecordComparator extends BinlogHistoryRecordComparator {

    public MariaDbHistoryRecordComparator(Predicate<String> gtidSourceFilter, GtidSetFactory gtidSetFactory) {
        super(gtidSourceFilter, gtidSetFactory);
    }

    @Override
    protected String getGtidSet(Document document) {
        return document.getString(MariaDbOffsetContext.GTID_SET_KEY);
    }

    @Override
    protected int getServerId(Document document) {
        return document.getInteger(SourceInfo.SERVER_ID_KEY, 0);
    }

    @Override
    protected boolean isSnapshot(Document document) {
        return document.has(SourceInfo.SNAPSHOT_KEY);
    }

    @Override
    protected long getTimestamp(Document document) {
        return document.getLong(SourceInfo.TIMESTAMP_KEY, 0);
    }

    @Override
    protected BinlogFileName getBinlogFileName(Document document) {
        return BinlogFileName.of(document.getString(SourceInfo.BINLOG_FILENAME_OFFSET_KEY));
    }

    @Override
    protected int getBinlogPosition(Document document) {
        return document.getInteger(SourceInfo.BINLOG_POSITION_OFFSET_KEY, -1);
    }

    @Override
    protected int getEventsToSkip(Document document) {
        return document.getInteger(MariaDbOffsetContext.EVENTS_TO_SKIP_OFFSET_KEY, 0);
    }

    @Override
    protected int getBinlogRowInEvent(Document document) {
        return document.getInteger(SourceInfo.BINLOG_ROW_IN_EVENT_OFFSET_KEY, -1);
    }

}
