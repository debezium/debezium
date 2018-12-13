/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Assert;
import org.junit.Test;

import java.time.Duration;
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

        Assert.assertSame(parallelSnapshotReader.state(), Reader.State.RUNNING);

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

        when(mockOldBinlogReader.isRunning()).thenReturn(true);
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

        // if the old reader is polled when it's stopped it will throw an exception.
        when(mockOldBinlogReader.isRunning()).thenReturn(false);
        when(mockOldBinlogReader.poll()).thenThrow(new InterruptedException());

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

        when(mockOldBinlogReader.isRunning()).thenReturn(true);
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

        when(mockOldBinlogReader.isRunning()).thenReturn(false);
        when(mockOldBinlogReader.poll()).thenThrow(new InterruptedException());

        when(mockNewBinlogReader.poll()).thenReturn(null);

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

        AtomicBoolean thisReaderNearEnd = new AtomicBoolean(false);
        AtomicBoolean otherReaderNearEnd = new AtomicBoolean(false);

        long durationSec = 5 * 60; // five minutes
        Duration duration = Duration.ofSeconds(durationSec);

        ParallelHaltingPredicate parallelHaltingPredicate = new ParallelHaltingPredicate(thisReaderNearEnd, otherReaderNearEnd, duration);

        boolean testResult = parallelHaltingPredicate.test(createSourceRecordWithTimestamp(System.currentTimeMillis()/1000 - (durationSec * 2)));

        Assert.assertTrue(testResult);

        Assert.assertFalse(thisReaderNearEnd.get());
        Assert.assertFalse(otherReaderNearEnd.get());
    }

    @Test
    public void testHaltingPredicateFlipsthisReaderNearEnd() {
        // verify that the halting predicate flips the `this reader` boolean if the
        // document's timestamp is within the time range, but still returns false.


        AtomicBoolean thisReaderNearEnd = new AtomicBoolean(false);
        AtomicBoolean otherReaderNearEnd = new AtomicBoolean(false);

        Duration duration = Duration.ofSeconds(5 * 60); // five minutes

        ParallelHaltingPredicate parallelHaltingPredicate = new ParallelHaltingPredicate(thisReaderNearEnd, otherReaderNearEnd, duration);

        boolean testResult = parallelHaltingPredicate.test(createSourceRecordWithTimestamp(System.currentTimeMillis()/1000));

        Assert.assertTrue(testResult);

        Assert.assertTrue(thisReaderNearEnd.get());
        Assert.assertFalse(otherReaderNearEnd.get());
    }

    @Test
    public void testHaltingPredicateHalts() {
        // verify that the halting predicate returns false if both the 'this' and
        // 'other' reader are near the end of the binlog.

        AtomicBoolean thisReaderNearEnd = new AtomicBoolean(false);
        AtomicBoolean otherReaderNearEnd = new AtomicBoolean(true);

        Duration duration = Duration.ofSeconds(5 * 60); // five minutes

        ParallelHaltingPredicate parallelHaltingPredicate =
            new ParallelHaltingPredicate(thisReaderNearEnd, otherReaderNearEnd, duration);

        boolean testResult =
            parallelHaltingPredicate.test(createSourceRecordWithTimestamp(System.currentTimeMillis()/1000));

        Assert.assertFalse(testResult);
        
        Assert.assertTrue(thisReaderNearEnd.get());
        Assert.assertTrue(otherReaderNearEnd.get());
    }

    /**
     * Create an "offset" containing a single timestamp element with the given value.
     * @param tsSec the timestamp (in seconds) in the resulting offset.
     * @return an "offset" containing the given timestamp.
     */
    private SourceRecord createSourceRecordWithTimestamp(long tsSec) {
        Map<String, ?> offset = Collections.singletonMap(SourceInfo.TIMESTAMP_KEY, tsSec);
        return new SourceRecord(null, offset, null, null, null);
    }
}
