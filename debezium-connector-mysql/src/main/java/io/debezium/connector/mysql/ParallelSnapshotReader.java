/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;

/**
 * A reader that runs a {@link ChainedReader} consisting of a {@link SnapshotReader} and a {@link BinlogReader}
 * for all tables newly added to the config in parallel with a {@link BinlogReader} for all the tables previously
 * in the config.
 */
public class ParallelSnapshotReader implements Reader {

    private static final Logger LOGGER = LoggerFactory.getLogger(ParallelSnapshotReader.class);

    private final BinlogReader oldTablesReader;
    private final BinlogReader newTablesBinlogReader;
    private final ChainedReader newTablesReader;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean completed = new AtomicBoolean(false);
    private final AtomicReference<Runnable> uponCompletion = new AtomicReference<>();

    private final MySqlConnectorTask.ServerIdGenerator serverIdGenerator;

    /**
     * Create a ParallelSnapshotReader.
     *
     * @param config the current connector configuration.
     * @param noSnapshotContext The context for those tables not undergoing a snapshot.
     * @param snapshotFilters {@link Filters} matching the tables that should be snapshotted.
     * @param serverIdGenerator a generator for creating unconflicting serverIds.
     */
    public ParallelSnapshotReader(Configuration config,
                                  MySqlTaskContext noSnapshotContext,
                                  Filters snapshotFilters,
                                  MySqlConnectorTask.ServerIdGenerator serverIdGenerator) {
        this.serverIdGenerator = serverIdGenerator;
        AtomicBoolean oldTablesReaderNearEnd = new AtomicBoolean(false);
        AtomicBoolean newTablesReaderNearEnd = new AtomicBoolean(false);
        ParallelHaltingPredicate oldTablesReaderHaltingPredicate = new ParallelHaltingPredicate(oldTablesReaderNearEnd, newTablesReaderNearEnd);
        ParallelHaltingPredicate newTablesReaderHaltingPredicate = new ParallelHaltingPredicate(newTablesReaderNearEnd, oldTablesReaderNearEnd);

        this.oldTablesReader = new BinlogReader("oldBinlog",
                noSnapshotContext,
                oldTablesReaderHaltingPredicate,
                serverIdGenerator.getNextServerId());

        MySqlTaskContext newTablesContext = new MySqlTaskContext(config,
                snapshotFilters,
                noSnapshotContext.source().offset());
        newTablesContext.start();
        SnapshotReader newTablesSnapshotReader = new SnapshotReader("newSnapshot", newTablesContext);

        this.newTablesBinlogReader = new BinlogReader("newBinlog",
                newTablesContext,
                newTablesReaderHaltingPredicate,
                serverIdGenerator.getNextServerId());
        this.newTablesReader = new ChainedReader.Builder().addReader(newTablesSnapshotReader).addReader(newTablesBinlogReader).build();
    }

    // for testing purposes
    /* package private */ ParallelSnapshotReader(BinlogReader oldTablesBinlogReader,
                                                 SnapshotReader newTablesSnapshotReader,
                                                 BinlogReader newTablesBinlogReader) {
        this.oldTablesReader = oldTablesBinlogReader;
        this.newTablesBinlogReader = newTablesBinlogReader;
        this.newTablesReader = new ChainedReader.Builder().addReader(newTablesSnapshotReader).addReader(newTablesBinlogReader).build();
        this.serverIdGenerator = null;
    }

    /**
     * Create and return a {@link ReconcilingBinlogReader} for the two binlog readers contained in this
     * ParallelSnapshotReader.
     * @return a {@link ReconcilingBinlogReader}
     */
    public ReconcilingBinlogReader createReconcilingBinlogReader(BinlogReader unifiedReader) {
        return new ReconcilingBinlogReader(oldTablesReader,
                newTablesBinlogReader,
                unifiedReader,
                serverIdGenerator.getNextServerId());
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
        if (running.compareAndSet(true, false)) {
            try {
                LOGGER.info("Stopping the {} reader", oldTablesReader.name());
                oldTablesReader.stop();
                oldTablesReader.context.shutdown();
            }
            catch (Throwable t) {
                LOGGER.error("Unexpected error stopping the {} reader", oldTablesReader.name());
            }

            try {
                LOGGER.info("Stopping the {} reader", newTablesReader.name());
                newTablesReader.stop();
                oldTablesReader.context.shutdown();
            }
            catch (Throwable t) {
                LOGGER.error("Unexpected error stopping the {} reader", newTablesReader.name());
            }
        }
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
        // the old tables reader is a raw BinlogReader and will throw an exception of poll is called when it is not running.
        List<SourceRecord> allRecords = oldTablesReader.isRunning() ? oldTablesReader.poll() : null;
        List<SourceRecord> newTablesRecords = newTablesReader.poll();
        if (newTablesRecords != null) {
            if (allRecords == null) {
                allRecords = newTablesRecords;
            }
            else {
                allRecords.addAll(newTablesRecords);
            }
        }
        else {
            // else newTableRecords == null
            if (allRecords == null) {
                // if both readers have stopped, we need to stop.
                completeSuccessfully();
            }
        }
        return allRecords;
    }

    private void completeSuccessfully() {
        if (completed.compareAndSet(false, true)) {
            stop();
            Runnable completionHandler = uponCompletion.getAndSet(null); // set to null so that we call it only once
            if (completionHandler != null) {
                completionHandler.run();
            }
        }
    }

    @Override
    public String name() {
        return "parallelSnapshotReader";
    }

    /**
     * A Halting Predicate for the parallel snapshot reader.
     * Usage for this predicate assumes two readers using two ParallelHalting Predicates.
     * The booleans are owned by the two predicates, and keep track of whether they or the
     * other reader has reached the end of the binlog.
     *
     * Test returns false if both both readers have been determined to be near the end of the
     * binlog.
     * Being near the end of the binlog is determined to be this predicate having seen a record
     * with a timestamp within {@link ParallelHaltingPredicate#DEFAULT_MIN_HALTING_DURATION} of
     * the current time. Once a single record near the end of the binlog has been seen, we
     * we assume the reader will stay near the end of the binlog.
     */
    /* package local */ static class ParallelHaltingPredicate implements HaltingPredicate {

        private static final Logger LOGGER = LoggerFactory.getLogger(ParallelHaltingPredicate.class);

        private static final Duration DEFAULT_MIN_HALTING_DURATION = Duration.ofMinutes(5);

        private volatile AtomicBoolean thisReaderNearEnd;
        private volatile AtomicBoolean otherReaderNearEnd;

        // The minimum duration we must be within before we attempt to halt.
        private final Duration minHaltingDuration;

        /* package local */ ParallelHaltingPredicate(AtomicBoolean thisReaderNearEndRef,
                                                     AtomicBoolean otherReaderNearEndRef) {
            this(thisReaderNearEndRef, otherReaderNearEndRef, DEFAULT_MIN_HALTING_DURATION);
        }

        /* package local */ ParallelHaltingPredicate(AtomicBoolean thisReaderNearEndRef,
                                                     AtomicBoolean otherReaderNearEndRef,
                                                     Duration minHaltingDuration) {
            this.otherReaderNearEnd = otherReaderNearEndRef;
            this.thisReaderNearEnd = thisReaderNearEndRef;
            this.minHaltingDuration = minHaltingDuration;
        }

        @Override
        public boolean accepts(SourceRecord ourSourceRecord) {
            // we assume if we ever end up near the end of the binlog, then we will remain there.
            if (!thisReaderNearEnd.get()) {
                Long sourceRecordTimestamp = (Long) ourSourceRecord.sourceOffset().get(SourceInfo.TIMESTAMP_KEY);
                // the timestamp will be null if we have not read any binlog events yet.
                if (sourceRecordTimestamp != null) {
                    Instant recordTimestamp = Instant.ofEpochSecond(sourceRecordTimestamp);
                    Instant now = Instant.now();
                    Duration durationToEnd = Duration.between(recordTimestamp, now);

                    if (durationToEnd.compareTo(minHaltingDuration) <= 0) {
                        // we are within minHaltingDuration of the end
                        LOGGER.debug("Parallel halting predicate: this reader near end");
                        thisReaderNearEnd.set(true);
                    }
                }
            }
            // return false if both readers are near end, true otherwise.
            return !(thisReaderNearEnd.get() && otherReaderNearEnd.get());
        }
    }
}
