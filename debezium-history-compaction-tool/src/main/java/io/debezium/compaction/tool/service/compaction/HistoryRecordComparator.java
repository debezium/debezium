/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.compaction.tool.service.compaction;

import java.util.function.BiFunction;

import io.debezium.document.Document;

public class HistoryRecordComparator {

    public static final HistoryRecordComparator INSTANCE = new HistoryRecordComparator();

    /**
     * Create a {@link io.debezium.relational.history.HistoryRecordComparator} that requires identical sources but will use the supplied function to compare
     * positions.
     *
     * @param positionComparator the non-null function that returns {@code true} if the first position is at or before
     *            the second position or {@code false} otherwise
     * @return the comparator instance; never null
     */
    public static io.debezium.relational.history.HistoryRecordComparator usingPositions(BiFunction<Document, Document, Boolean> positionComparator) {
        return new io.debezium.relational.history.HistoryRecordComparator() {
            @Override
            protected boolean isPositionAtOrBefore(Document position1, Document position2) {
                return positionComparator.apply(position1, position2);
            }
        };
    }

    /**
     * Determine if the first {@link HistoryRecord} is at the same or earlier point in time than the second {@link HistoryRecord}.
     *
     * @param record1 the first record; never null
     * @param record2 the second record; never null
     * @return {@code true} if the first record is at the same or earlier point in time than the second record, or {@code false}
     *         otherwise
     */
    public boolean isAtOrBefore(HistoryRecord record1, HistoryRecord record2) {
        return isSameSource(record1.source(), record2.source()) && isPositionAtOrBefore(record1.position(), record2.position());
    }

    protected boolean isPositionAtOrBefore(Document position1, Document position2) {
        return position1.compareToUsingSimilarFields(position2) <= 0;
    }

    protected boolean isSameSource(Document source1, Document source2) {
        return source1.equals(source2);
    }
}
