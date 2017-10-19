/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Before;
import org.junit.Test;

import static org.fest.assertions.Assertions.assertThat;

import io.debezium.connector.mysql.Reader.State;
import io.debezium.util.Collect;

/**
 * @author Randall Hauch
 *
 */
public class ChainedReaderTest {

    private static final List<SourceRecord> RL1 = Collect.arrayListOf(record());
    private static final List<SourceRecord> RL2 = Collect.arrayListOf(record());
    private static final List<SourceRecord> RL3 = Collect.arrayListOf(record());
    private static final List<SourceRecord> RL4 = Collect.arrayListOf(record());
    private static final List<SourceRecord> RL5 = Collect.arrayListOf(record());
    @SuppressWarnings("unchecked")
    private static final List<List<SourceRecord>> SOURCE_RECORDS = Collect.arrayListOf(RL1, RL2, RL3, RL4, RL5);

    protected static Supplier<List<SourceRecord>> records() {
        Iterator<List<SourceRecord>> iter = SOURCE_RECORDS.iterator();
        return () -> {
            return iter.hasNext() ? iter.next() : null;
        };
    }

    private static SourceRecord record() {
        return new SourceRecord(null, null, null, null, null, null);
    }

    private ChainedReader reader;

    @Before
    public void beforeEach() {
        reader = new ChainedReader();
    }

    @Test
    public void shouldNotStartWithoutReaders() throws InterruptedException {
        assertThat(reader.state()).isEqualTo(State.STOPPED);
        reader.start();
        assertThat(reader.state()).isEqualTo(State.STOPPED);
        assertPollReturnsNoMoreRecords();
    }

    @Test
    public void shouldStartAndStopSingleReaderBeforeReaderStopsItself() throws InterruptedException {
        reader.add(new MockReader("r1", records()));
        reader.uponCompletion("Stopped the r1 reader");
        reader.start();
        assertThat(reader.state()).isEqualTo(State.RUNNING);
        assertThat(reader.poll()).isSameAs(RL1);
        assertThat(reader.poll()).isSameAs(RL2);
        assertThat(reader.poll()).isSameAs(RL3);
        assertThat(reader.poll()).isSameAs(RL4);
        reader.stop();
        assertThat(reader.state()).isEqualTo(State.STOPPING);
        assertThat(reader.poll()).isNull();
        assertThat(reader.state()).isEqualTo(State.STOPPED);
        assertPollReturnsNoMoreRecords();
    }

    @Test
    public void shouldStartSingleReaderThatStopsAutomatically() throws InterruptedException {
        reader.add(new MockReader("r2", records()));
        reader.uponCompletion("Stopped the r2 reader");
        reader.start();
        assertThat(reader.state()).isEqualTo(State.RUNNING);
        assertThat(reader.poll()).isSameAs(RL1);
        assertThat(reader.poll()).isSameAs(RL2);
        assertThat(reader.poll()).isSameAs(RL3);
        assertThat(reader.poll()).isSameAs(RL4);
        assertThat(reader.poll()).isSameAs(RL5);
        assertThat(reader.poll()).isNull(); // cause the mock reader to stop itself
        assertThat(reader.state()).isEqualTo(State.STOPPED);
        assertPollReturnsNoMoreRecords();
    }

    @Test
    public void shouldStartAndStopMultipleReaders() throws InterruptedException {
        reader.add(new MockReader("r3", records()));
        reader.add(new MockReader("r4", records()));
        reader.uponCompletion("Stopped the r3+r4 reader");
        reader.start();
        assertThat(reader.state()).isEqualTo(State.RUNNING);
        assertThat(reader.poll()).isSameAs(RL1);
        assertThat(reader.poll()).isSameAs(RL2);
        assertThat(reader.poll()).isSameAs(RL3);
        assertThat(reader.poll()).isSameAs(RL4);
        assertThat(reader.poll()).isSameAs(RL5);
        assertThat(reader.poll()).isSameAs(RL1);
        assertThat(reader.poll()).isSameAs(RL2);
        assertThat(reader.poll()).isSameAs(RL3);
        assertThat(reader.poll()).isSameAs(RL4);
        assertThat(reader.poll()).isSameAs(RL5);
        assertThat(reader.poll()).isNull(); // cause the 2nd mock reader to stop itself
        assertThat(reader.state()).isEqualTo(State.STOPPED);
        assertPollReturnsNoMoreRecords();
    }

    @Test
    public void shouldStartAndStopReaderThatContinuesProducingItsRecordsAfterBeingStopped() throws InterruptedException {
        reader.add(new CompletingMockReader("r5", records()));
        reader.uponCompletion("Stopped the r5 reader");
        reader.start();
        assertThat(reader.state()).isEqualTo(State.RUNNING);
        assertThat(reader.poll()).isSameAs(RL1);
        assertThat(reader.poll()).isSameAs(RL2);
        // Manually stop this reader, and it will continue returning all of its 5 record lists ...
        reader.stop();
        assertThat(reader.state()).isEqualTo(State.STOPPING);
        // Read the remaining records ...
        assertThat(reader.poll()).isSameAs(RL3);
        assertThat(reader.poll()).isSameAs(RL4);
        assertThat(reader.poll()).isSameAs(RL5);
        assertThat(reader.poll()).isNull();
        // The reader has no more records, so it should now be stopped ...
        assertThat(reader.state()).isEqualTo(State.STOPPED);
        assertPollReturnsNoMoreRecords();
    }
    
    protected void assertPollReturnsNoMoreRecords() throws InterruptedException {
        for (int i=0;i!=10; ++i) {
            assertThat(reader.poll()).isNull();
        }
    }

    /**
     * A {@link Reader} that returns records until manually stopped.
     */
    public static class MockReader implements Reader {
        private final String name;
        private final Supplier<List<SourceRecord>> pollResultsSupplier;
        private final AtomicReference<Runnable> completionHandler = new AtomicReference<>();
        private final AtomicBoolean running = new AtomicBoolean();
        private final AtomicBoolean completed = new AtomicBoolean();

        public MockReader(String name, Supplier<List<SourceRecord>> pollResultsSupplier) {
            this.name = name;
            this.pollResultsSupplier = pollResultsSupplier;
        }

        @Override
        public State state() {
            if (running.get()) return State.RUNNING;
            if (completed.get()) return State.STOPPED;
            return State.STOPPING;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public List<SourceRecord> poll() throws InterruptedException {
            List<SourceRecord> record = null;
            if (continueReturningRecordsFromPolling()) {
                record = pollResultsSupplier.get();
            }
            if (record == null) {
                // We're done ...
                Runnable handler = this.completionHandler.get();
                if (handler != null) {
                    handler.run();
                }
                completed.set(true);
                running.set(false);
            }
            return record;
        }

        protected boolean continueReturningRecordsFromPolling() {
            return running.get();
        }

        @Override
        public void start() {
            assertThat(running.get()).isFalse();
            running.set(true);
        }

        @Override
        public void stop() {
            running.set(false);
        }

        @Override
        public void uponCompletion(Runnable handler) {
            completionHandler.set(handler);
        }
    }

    /**
     * A {@link MockReader} that always returns all records even after this reader is manually stopped.
     */
    public static class CompletingMockReader extends MockReader {
        public CompletingMockReader(String name, Supplier<List<SourceRecord>> pollResultsSupplier) {
            super(name,pollResultsSupplier);
        }
        @Override
        protected boolean continueReturningRecordsFromPolling() {
            return true;
        }
    }
}
