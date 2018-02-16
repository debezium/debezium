/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.debezium.connector.mysql.ParallelSnapshotReader.ParallelHaltingPredicate;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Moira Tagle
 */
public class ParallelSnapshotReaderTest {

    @Test
    public void startStartsBothReaders() {
        BinlogReader mockOldBinlogReader = mock(BinlogReader.class);
        SnapshotReader mockNewSnapshotReader = mock(SnapshotReader.class);
        BinlogReader mockNewBinlogReader = mock(BinlogReader.class);

        ParallelSnapshotReader parallelSnapshotReader = new ParallelSnapshotReader(mockOldBinlogReader, mockNewSnapshotReader, mockNewBinlogReader);

        parallelSnapshotReader.start();

        Assert.assertTrue(parallelSnapshotReader.state() == Reader.State.RUNNING);

        verify(mockOldBinlogReader).start();
        verify(mockNewSnapshotReader).start();
        // chained reader will only start the snapshot reader
    }

    @Test
    public void pollCombinesBothReadersPolls() throws InterruptedException {
        BinlogReader mockOldBinlogReader = mock(BinlogReader.class);
        SnapshotReader mockNewSnapshotReader = mock(SnapshotReader.class);
        BinlogReader mockNewBinlogReader = mock(BinlogReader.class);

        ParallelSnapshotReader parallelSnapshotReader = new ParallelSnapshotReader(mockOldBinlogReader, mockNewSnapshotReader, mockNewBinlogReader);

        SourceRecord oldBinlogSourceRecord = mock(SourceRecord.class);
        List<SourceRecord> oldBinlogRecords = new ArrayList<>();
        oldBinlogRecords.add(oldBinlogSourceRecord);

        SourceRecord newSnapshotSourceRecord = mock(SourceRecord.class);
        List<SourceRecord> newSnapshotRecords = new ArrayList<>();
        newSnapshotRecords.add(newSnapshotSourceRecord);

        when(mockOldBinlogReader.poll()).thenReturn(oldBinlogRecords);
        when(mockNewSnapshotReader.poll()).thenReturn(newSnapshotRecords);

        // this needs to happen so that the chained reader can be polled.
        parallelSnapshotReader.start();

        List<SourceRecord> parallelRecords = parallelSnapshotReader.poll();

        Assert.assertEquals(2, parallelRecords.size());
        Assert.assertTrue(parallelRecords.contains(oldBinlogSourceRecord));
        Assert.assertTrue(parallelRecords.contains(newSnapshotSourceRecord));
    }

    @Test
    public void pollReturnsNewIfOldReaderIsStopped() throws InterruptedException {
        BinlogReader mockOldBinlogReader = mock(BinlogReader.class);
        SnapshotReader mockNewSnapshotReader = mock(SnapshotReader.class);
        BinlogReader mockNewBinlogReader = mock(BinlogReader.class);

        ParallelSnapshotReader parallelSnapshotReader = new ParallelSnapshotReader(mockOldBinlogReader, mockNewSnapshotReader, mockNewBinlogReader);

        SourceRecord newSnapshotSourceRecord = mock(SourceRecord.class);
        List<SourceRecord> newSnapshotRecords = new ArrayList<>();
        newSnapshotRecords.add(newSnapshotSourceRecord);

        when(mockOldBinlogReader.poll()).thenReturn(null);
        when(mockNewSnapshotReader.poll()).thenReturn(newSnapshotRecords);

        // this needs to happen so that the chained reader runs correctly.
        parallelSnapshotReader.start();

        List<SourceRecord> parallelRecords = parallelSnapshotReader.poll();

        Assert.assertEquals(1, parallelRecords.size());
        Assert.assertTrue(parallelRecords.contains(newSnapshotSourceRecord));
    }

    // this test and the next don't appear to be halting. Something with the chained reader maybe.
    @Test
    public void pollReturnsOldIfNewReaderIsStopped() throws InterruptedException {
        BinlogReader mockOldBinlogReader = mock(BinlogReader.class);
        SnapshotReader mockNewSnapshotReader = mock(SnapshotReader.class);
        BinlogReader mockNewBinlogReader = mock(BinlogReader.class);

        ParallelSnapshotReader parallelSnapshotReader = new ParallelSnapshotReader(mockOldBinlogReader, mockNewSnapshotReader, mockNewBinlogReader);

        SourceRecord oldBinlogSourceRecord = mock(SourceRecord.class);
        List<SourceRecord> oldBinlogRecords = new ArrayList<>();
        oldBinlogRecords.add(oldBinlogSourceRecord);

        when(mockOldBinlogReader.poll()).thenReturn(oldBinlogRecords);

        // cheap way to have the new reader be stopped is to just not start it; so don't start the parallel reader

        List<SourceRecord> parallelRecords = parallelSnapshotReader.poll();

        Assert.assertEquals(1, parallelRecords.size());
        Assert.assertTrue(parallelRecords.contains(oldBinlogSourceRecord));
    }

    @Test
    public void pollReturnsNullIfBothReadersAreStopped() throws InterruptedException {
        BinlogReader mockOldBinlogReader = mock(BinlogReader.class);
        SnapshotReader mockNewSnapshotReader = mock(SnapshotReader.class);
        BinlogReader mockNewBinlogReader = mock(BinlogReader.class);

        ParallelSnapshotReader parallelSnapshotReader = new ParallelSnapshotReader(mockOldBinlogReader, mockNewSnapshotReader, mockNewBinlogReader);

        when(mockOldBinlogReader.poll()).thenReturn(null);

        // cheap way to have the new reader be stopped is to just not start it; so don't start the parallel reader

        List<SourceRecord> parallelRecords = parallelSnapshotReader.poll();

        Assert.assertEquals(null, parallelRecords);
    }

    @Test
    public void testStopStopsBothReaders() {
        BinlogReader mockOldBinlogReader = mock(BinlogReader.class);
        SnapshotReader mockNewSnapshotReader = mock(SnapshotReader.class);
        BinlogReader mockNewBinlogReader = mock(BinlogReader.class);

        ParallelSnapshotReader parallelSnapshotReader = new ParallelSnapshotReader(mockOldBinlogReader, mockNewSnapshotReader, mockNewBinlogReader);

        parallelSnapshotReader.start();
        parallelSnapshotReader.stop();

        Assert.assertTrue(parallelSnapshotReader.state() == Reader.State.STOPPED);

        verify(mockOldBinlogReader).stop();
        verify(mockNewSnapshotReader).stop();
    }


    @Test
    public void testHaltingPredicateHonorsTimeRange() {
        // verify that halting predicate does nothing and changes no state if the
        // document's timestamp is outside of the time range.

        AtomicBoolean thisReaderBoolean = new AtomicBoolean(false);
        AtomicBoolean otherReaderBoolean = new AtomicBoolean(false);

        long timeRange = 5 * 60 * 1000; // five minutes

        ParallelHaltingPredicate parallelHaltingPredicate = new ParallelHaltingPredicate(thisReaderBoolean, otherReaderBoolean, timeRange);

        boolean testResult = parallelHaltingPredicate.test(createSourceRecordWithTimestamp(System.currentTimeMillis() - (timeRange * 2)));

        Assert.assertFalse(testResult);

        Assert.assertFalse(thisReaderBoolean.get());
        Assert.assertFalse(otherReaderBoolean.get());
    }

    @Test
    public void testHaltingPredicateFlipsThisReaderBoolean() {
        // verify that the halting predicate flips the `this reader` boolean if the
        // document's timestamp is within the time range, but still returns false.

        AtomicBoolean thisReaderBoolean = new AtomicBoolean(false);
        AtomicBoolean otherReaderBoolean = new AtomicBoolean(false);

        long timeRange = 5 * 60 * 1000; // five minutes

        ParallelHaltingPredicate parallelHaltingPredicate = new ParallelHaltingPredicate(thisReaderBoolean, otherReaderBoolean, timeRange);

        boolean testResult = parallelHaltingPredicate.test(createSourceRecordWithTimestamp(System.currentTimeMillis()));

        Assert.assertFalse(testResult);

        Assert.assertTrue(thisReaderBoolean.get());
        Assert.assertFalse(otherReaderBoolean.get());
    }

    @Test
    public void testHaltingPredicateHalts() {
        // verify that the halting predicate returns true if both the `this` and
        // `other` reader booleans are true.

        AtomicBoolean thisReaderBoolean = new AtomicBoolean(false);
        AtomicBoolean otherReaderBoolean = new AtomicBoolean(true);

        long timeRange = 5 * 60 * 1000; // five minutes

        ParallelHaltingPredicate parallelHaltingPredicate = new ParallelHaltingPredicate(thisReaderBoolean, otherReaderBoolean, timeRange);

        boolean testResult = parallelHaltingPredicate.test(createSourceRecordWithTimestamp(System.currentTimeMillis()));

        Assert.assertTrue(testResult);

        Assert.assertTrue(thisReaderBoolean.get());
        Assert.assertTrue(otherReaderBoolean.get());
    }

    /**
     * Create an "offset" containing a single timestamp element with the given value.
     * @param tsMs the timestamp in the resulting offset.
     * @return an "offset" containing the given timestamp.
     */
    private SourceRecord createSourceRecordWithTimestamp(long tsMs) {
        Map<String, ?> offset = Collections.singletonMap(SourceInfo.TIMESTAMP_KEY, tsMs);
        return new SourceRecord(null, offset, null, null, null);
    }
}
