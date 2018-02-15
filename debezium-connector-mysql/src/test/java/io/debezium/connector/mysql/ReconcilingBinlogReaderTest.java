/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Moira Tagle
 */
public class ReconcilingBinlogReaderTest {

    @Test
    public void haltAfterPredicateFalse() {
        List<Map<String, ?>> offsets = createOrderedOffsets(2);
        ReconcilingBinlogReader.HaltAfterPredicate haltAfterPredicate =
            new ReconcilingBinlogReader.HaltAfterPredicate(offsets.get(1), (x) -> true);

        Assert.assertFalse(haltAfterPredicate.test(offsets.get(0)));
    }

    @Test
    public void haltAfterPredicateTrue() {
        List<Map<String, ?>> offsets = createOrderedOffsets(2);
        ReconcilingBinlogReader.HaltAfterPredicate haltAfterPredicate =
            new ReconcilingBinlogReader.HaltAfterPredicate(offsets.get(0), (x) -> true);

        Assert.assertTrue(haltAfterPredicate.test(offsets.get(1)));
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
        for (int i = 0 ; i < size; i++) {
            Map<String, Object> offset = new HashMap<>(3);
            offset.put(SourceInfo.SERVER_ID_KEY, SERVER_ID);
            offset.put(SourceInfo.BINLOG_FILENAME_OFFSET_KEY, BINLOG_FILENAME);
            offset.put(SourceInfo.BINLOG_POSITION_OFFSET_KEY, STARTING_BINLOG_POSTION + i);
            orderedDocuments.add(offset);
        }
        return orderedDocuments;
    }

}
