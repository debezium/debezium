/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

import io.debezium.document.Document;

/**
 * Compares two schema history positions based on WAL frame indices.
 *
 * @author Zihan Dai
 */
public final class SqliteHistoryRecordComparator {

    /**
     * Key used to store the WAL frame index in the offset / history record.
     */
    public static final String WAL_POSITION_KEY = "wal_position";

    private SqliteHistoryRecordComparator() {
    }

    /**
     * Determines whether the recorded position is at or before the desired position.
     *
     * @param recorded the position stored in the schema history
     * @param desired  the position we want to compare against
     * @return {@code true} if {@code recorded} is at or before {@code desired}
     */
    public static boolean isPositionAtOrBefore(Document recorded, Document desired) {
        // TODO: Implement proper WAL frame index comparison once streaming is in place.
        // For now, fall back to a simple long comparison on the wal_position field.
        long recordedPos = recorded.getLong(WAL_POSITION_KEY, 0L);
        long desiredPos = desired.getLong(WAL_POSITION_KEY, 0L);
        return recordedPos <= desiredPos;
    }
}
