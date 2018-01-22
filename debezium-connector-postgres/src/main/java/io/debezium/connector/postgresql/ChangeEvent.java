/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import org.apache.kafka.connect.source.SourceRecord;

/**
 * Container for {@link SourceRecord}s and associated metadata
 *
 * @author Jiri Pechanec
 *
 */
public class ChangeEvent {

    /**
     * source record to be sent
     */
    private final SourceRecord record;
    /**
     * true if the originating decoder event is the last one in a batch of events with the same LSN
     */
    private final boolean isLastOfLsn;

    public ChangeEvent(SourceRecord record, boolean isLastOfLsn) {
        this.record = record;
        this.isLastOfLsn = isLastOfLsn;
    }

    public ChangeEvent(SourceRecord record) {
        this.record = record;
        this.isLastOfLsn = true;
    }

    public SourceRecord getRecord() {
        return record;
    }

    public boolean isLastOfLsn() {
        return isLastOfLsn;
    }

    @Override
    public String toString() {
        return "ChangeEvent [record=" + record + ", isLastOfLsn=" + isLastOfLsn + "]";
    }

}
