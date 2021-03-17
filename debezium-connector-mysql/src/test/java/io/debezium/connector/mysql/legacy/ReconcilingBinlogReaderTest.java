/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.legacy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author Moira Tagle
 */
public class ReconcilingBinlogReaderTest {

    @Test
    public void haltAfterPredicateTrue() {
        List<Map<String, ?>> offsets = createOrderedOffsets(2);
        ReconcilingBinlogReader.OffsetLimitPredicate offsetLimitPredicate = new ReconcilingBinlogReader.OffsetLimitPredicate(offsets.get(1), (x) -> true);

        SourceRecord testSourceRecord = createSourceRecordWithOffset(offsets.get(0));
        // tested record (0) is before limit (1), so we should return true.
        Assert.assertTrue(offsetLimitPredicate.accepts(testSourceRecord));
    }

    @Test
    public void haltAfterPredicateFalse() {
        List<Map<String, ?>> offsets = createOrderedOffsets(2);
        ReconcilingBinlogReader.OffsetLimitPredicate offsetLimitPredicate = new ReconcilingBinlogReader.OffsetLimitPredicate(offsets.get(0), (x) -> true);

        SourceRecord testSourceRecord = createSourceRecordWithOffset(offsets.get(1));
        // tested record (1) is beyond limit (0), so we should return false.
        Assert.assertFalse(offsetLimitPredicate.accepts(testSourceRecord));
    }

    private final int SERVER_ID = 0;
    private final String BINLOG_FILENAME = "bin.log1";
    private final int STARTING_BINLOG_POSTION = 20;

    /**
     * Create an ordered list of offsets from earliest to latest.
     * @param size the number of offsets to create.
     * @return
     */
    private List<Map<String, ?>> createOrderedOffsets(int size) {
        List<Map<String, ?>> orderedDocuments = new ArrayList<>(size);

        // using non-gtids because SourceInfo.isPositionAtOrBefore
        // doesn't seem to function as expected when comparing gtids
        for (int i = 0; i < size; i++) {
            Map<String, Object> offset = new HashMap<>(3);
            offset.put(SourceInfo.SERVER_ID_KEY, SERVER_ID);
            offset.put(SourceInfo.BINLOG_FILENAME_OFFSET_KEY, BINLOG_FILENAME);
            offset.put(SourceInfo.BINLOG_POSITION_OFFSET_KEY, STARTING_BINLOG_POSTION + i);
            orderedDocuments.add(offset);
        }
        return orderedDocuments;
    }

    private SourceRecord createSourceRecordWithOffset(Map<String, ?> offset) {
        return new SourceRecord(null, offset, null, null, null);
    }

}
