/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import io.debezium.config.Configuration;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

/**
 * A reader that runs a {@link ChainedReader} consisting of a {@link SnapshotReader} and a {@link BinlogReader}
 * for all tables newly added to the config in parallel with a {@link BinlogReader} for all the tables previously
 * in the config.
 */
public class ParallelSnapshotReader implements Reader {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final BinlogReader oldTablesReader;
    private final BinlogReader newTablesBinlogReader;
    private final ChainedReader newTablesReader;

    private final AtomicBoolean running = new AtomicBoolean();
    private final AtomicReference<Runnable> uponCompletion = new AtomicReference<>();

    /**
     * Create a ParallelSnapshotReader.
     *
     * @param config the current connector configuration.
     * @param noSnapshotContext The context for those tables not undergoing a snapshot.
     * @param snapshotFilters {@link Filters} matching the tables that should be snapshotted.
     */
    public ParallelSnapshotReader(Configuration config,
                                  MySqlTaskContext noSnapshotContext,
                                  Filters snapshotFilters) {
        AtomicBoolean oldTablesReaderNearEnd = new AtomicBoolean(false);
        AtomicBoolean newTablesReaderNearEnd = new AtomicBoolean(false);
        ParallelHaltingPredicate oldTablesReaderHaltingPredicate =
            new ParallelHaltingPredicate(oldTablesReaderNearEnd, newTablesReaderNearEnd);
        ParallelHaltingPredicate newTablesReaderHaltingPredicate =
            new ParallelHaltingPredicate(newTablesReaderNearEnd, oldTablesReaderNearEnd);

        this.oldTablesReader = new BinlogReader("oldBinlog", noSnapshotContext, oldTablesReaderHaltingPredicate);

        MySqlTaskContext newTablesContext = new MySqlTaskContext(config, snapshotFilters);
        SnapshotReader newTablesSnapshotReader = new SnapshotReader("newSnapshot", newTablesContext);

        this.newTablesBinlogReader = new BinlogReader("newBinlog", newTablesContext, newTablesReaderHaltingPredicate);
        this.newTablesReader = new ChainedReader().add(newTablesSnapshotReader).add(newTablesBinlogReader);

    }

    // for testing purposes
    /*package private*/ ParallelSnapshotReader(BinlogReader oldTablesBinlogReader,
                                               SnapshotReader newTablesSnapshotReader,
                                               BinlogReader newTablesBinlogReader) {
        this.oldTablesReader = oldTablesBinlogReader;
        this.newTablesBinlogReader = newTablesBinlogReader;
        this.newTablesReader = new ChainedReader().add(newTablesSnapshotReader).add(newTablesBinlogReader);
    }

    /**
     * Create and return a {@link ReconcilingBinlogReader} for the two binlog readers contained in this
     * ParallelSnapshotReader.
     * @return a {@link ReconcilingBinlogReader}
     */
    public ReconcilingBinlogReader createReconcillingBinlogReader() {
        return new ReconcilingBinlogReader(oldTablesReader, newTablesBinlogReader);
    }

    @Override
    public void uponCompletion(Runnable handler) {
        uponCompletion.set(handler);
    }

    @Override
    public void initialize() {
        oldTablesReader.initialize();
        newTablesReader.initialize();
    }

    @Override
    public void start() {
        if (running.compareAndSet(false, true)) {
            oldTablesReader.start();
            newTablesReader.start();
        }
    }

    @Override
    public void stop() {
        try {
            logger.info("Stopping the {} reader", oldTablesReader.name());
            oldTablesReader.stop();
        } catch (Throwable t) {
            logger.error("Unexpected error stopping the {} reader", oldTablesReader.name());
        }

        try {
            logger.info("Stopping the {} reader", newTablesReader.name());
            newTablesReader.stop();
        } catch (Throwable t) {
            logger.error("Unexpected error stopping the {} reader", newTablesReader.name());
        }
        running.set(false);
    }

    @Override
    public State state() {
        if (running.get()) {
            return State.RUNNING;
        }
        return State.STOPPED;
    }


    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        List<SourceRecord> allRecords = oldTablesReader.poll();
        List<SourceRecord> newTablesRecords = newTablesReader.poll();
        if (newTablesRecords != null) {
            if (allRecords == null) {
                allRecords = newTablesRecords;
            } else {
                allRecords.addAll(newTablesRecords);
            }
        }
        return allRecords;
    }

    @Override
    public String name() {
        return "parallelSnapshotReader";
    }

    /**
     * A Halting Predicate for the parallel snapshot reader
     *
     * This halting predicate assumes that there is another reader also running with a complementary halting predicate.
     * The lagging reader's halting predicate is the only predicate that will return true, but it will signal for the
     * other reader to halt at the same time.
     */

    /*package local*/ static class ParallelHaltingPredicate implements Predicate<Map<String, ?>> {

        // todo maybe this should eventually be configured, but for now the time diff were are interested in
        // is hard coded in as 5 minutes.
        private static final long DEFAULT_TIME_RANGE_MS = 5 * 60 * 1000;

        private volatile AtomicBoolean thisReaderNearEnd;
        private volatile AtomicBoolean otherReaderNearEnd;

        private final long timeRangeMs;

        /*package local*/ ParallelHaltingPredicate(AtomicBoolean thisReaderNearEndRef,
                                                   AtomicBoolean otherReaderNearEndRef) {
            this(thisReaderNearEndRef, otherReaderNearEndRef, DEFAULT_TIME_RANGE_MS);
        }

        /*package local*/ ParallelHaltingPredicate(AtomicBoolean thisReaderNearEndRef,
                                                   AtomicBoolean otherReaderNearEndRef,
                                                   long timeRangeMs) {
            this.otherReaderNearEnd = otherReaderNearEndRef;
            this.thisReaderNearEnd = thisReaderNearEndRef;
            this.timeRangeMs = timeRangeMs;
        }

        @Override
        public boolean test(Map<String, ?> ourNewOffset) {
            // we assume if we ever end up near the end of the binlog, then we will remain there.
            if (!thisReaderNearEnd.get()) {
                Long currentTsMs = System.currentTimeMillis();
                Long offsetTsMs = (Long) ourNewOffset.get(SourceInfo.TIMESTAMP_KEY);
                if (offsetTsMs + timeRangeMs > currentTsMs) {
                    // we are within timeRangeMs of the end
                    thisReaderNearEnd.set(true);
                }
            }
            return thisReaderNearEnd.get() && otherReaderNearEnd.get();
        }
    }
}
