/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.legacy;

import static org.fest.assertions.Assertions.assertThat;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Assert;
import org.junit.Test;

import io.debezium.config.ConfigurationDefaults;
import io.debezium.connector.mysql.legacy.Reader.State;
import io.debezium.util.Clock;
import io.debezium.util.Collect;
import io.debezium.util.Threads;
import io.debezium.util.Threads.Timer;

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

    @Test
    public void shouldNotStartWithoutReaders() throws InterruptedException {
        reader = new ChainedReader.Builder().build();
        assertThat(reader.state()).isEqualTo(State.STOPPED);
        reader.start();
        assertThat(reader.state()).isEqualTo(State.STOPPED);
        assertPollReturnsNoMoreRecords();
    }

    @Test
    public void shouldStartAndStopSingleReaderBeforeReaderStopsItself() throws InterruptedException {
        reader = new ChainedReader.Builder()
                .addReader(new MockReader("r1", records()))
                .completionMessage("Stopped the r1 reader")
                .build();
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
        reader = new ChainedReader.Builder()
                .addReader(new MockReader("r2", records()))
                .completionMessage("Stopped the r2 reader")
                .build();
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
        reader = new ChainedReader.Builder()
                .addReader(new MockReader("r3", records()))
                .addReader(new MockReader("r4", records()))
                .completionMessage("Stopped the r3+r4 reader")
                .build();
        reader.start();
        assertThat(reader.state()).isEqualTo(State.RUNNING);
        assertThat(reader.poll()).isSameAs(RL1);
        assertThat(reader.poll()).isSameAs(RL2);
        assertThat(reader.poll()).isSameAs(RL3);
        assertThat(reader.poll()).isSameAs(RL4);
        assertThat(reader.poll()).isSameAs(RL5);
        // Wait for 2nd reader to start
        List<SourceRecord> records = reader.poll();
        final Timer timeout = Threads.timer(Clock.SYSTEM, ConfigurationDefaults.RETURN_CONTROL_INTERVAL);
        while (records == null) {
            if (timeout.expired()) {
                Assert.fail("Subsequent reader has not started");
            }
            Thread.sleep(100);
            records = reader.poll();
        }
        assertThat(records).isSameAs(RL1);
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
        reader = new ChainedReader.Builder()
                .addReader(new CompletingMockReader("r5", records()))
                .completionMessage("Stopped the r5 reader")
                .build();
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
        for (int i = 0; i != 10; ++i) {
            assertThat(reader.poll()).isNull();
        }
    }

    @Test
    public void shouldInitAndDestroyResources() {
        MockReader r1 = new MockReader("r1", records());
        MockReader r2 = new MockReader("r2", records());

        reader = new ChainedReader.Builder().addReader(r1).addReader(r2).build();
        reader.initialize();
        assertThat(r1.mockResource).isNotNull();
        assertThat(r2.mockResource).isNotNull();
        reader.destroy();
        assertThat(r1.mockResource).isNull();
        assertThat(r2.mockResource).isNull();
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
        private Object mockResource;

        public MockReader(String name, Supplier<List<SourceRecord>> pollResultsSupplier) {
            this.name = name;
            this.pollResultsSupplier = pollResultsSupplier;
        }

        @Override
        public State state() {
            if (running.get()) {
                return State.RUNNING;
            }
            if (completed.get()) {
                return State.STOPPED;
            }
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

        @Override
        public void initialize() {
            mockResource = new String();
        }

        @Override
        public void destroy() {
            mockResource = null;
        }
    }

    /**
     * A {@link MockReader} that always returns all records even after this reader is manually stopped.
     */
    public static class CompletingMockReader extends MockReader {
        public CompletingMockReader(String name, Supplier<List<SourceRecord>> pollResultsSupplier) {
            super(name, pollResultsSupplier);
        }

        @Override
        protected boolean continueReturningRecordsFromPolling() {
            return true;
        }
    }
}
