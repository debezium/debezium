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

    /*
     * source record to be sent
     */
    private final SourceRecord record;

    /**
     * The last LSN of that was completely processed. Depending on the batching it is either
     * LSN of a current record or LSN of the previous transaction.
     */
    private final Long lastCompletelyProcessedLsn;

    public ChangeEvent(SourceRecord record, Long lastCompletelyProcessedLsn) {
        this.record = record;
        this.lastCompletelyProcessedLsn = lastCompletelyProcessedLsn;
    }

    public SourceRecord getRecord() {
        return record;
    }

    public Long getLastCompletelyProcessedLsn() {
        return lastCompletelyProcessedLsn;
    }

    @Override
    public String toString() {
        return "ChangeEvent [record=" + record + ", lastCompletelyProcessedLsn=" + lastCompletelyProcessedLsn + "]";
    }
}
