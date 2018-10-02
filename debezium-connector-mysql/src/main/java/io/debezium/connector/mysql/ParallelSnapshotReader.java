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

import java.time.Duration;
import java.time.Instant;
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

    private static final long NEW_TABLES_SERVER_ID_OFFSET = 10000;

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

        this.oldTablesReader = new BinlogReader("oldBinlog",
                                                noSnapshotContext,
                                                oldTablesReaderHaltingPredicate);

        MySqlTaskContext newTablesContext = new MySqlTaskContext(config,
                                                                 snapshotFilters,
                                                                 noSnapshotContext.source().offset());
        newTablesContext.start();
        SnapshotReader newTablesSnapshotReader = new SnapshotReader("newSnapshot", newTablesContext);

        long newTablesBinlogReaderServerId = newTablesContext.serverId() + NEW_TABLES_SERVER_ID_OFFSET;
        this.newTablesBinlogReader = new BinlogReader("newBinlog",
                                                      newTablesContext,
                                                      newTablesReaderHaltingPredicate,
                                                      newTablesBinlogReaderServerId);
        this.newTablesReader = new ChainedReader.Builder().addReader(newTablesSnapshotReader).addReader(newTablesBinlogReader).build();
    }

    // for testing purposes
    /*package private*/ ParallelSnapshotReader(BinlogReader oldTablesBinlogReader,
                                               SnapshotReader newTablesSnapshotReader,
                                               BinlogReader newTablesBinlogReader) {
        this.oldTablesReader = oldTablesBinlogReader;
        this.newTablesBinlogReader = newTablesBinlogReader;
        this.newTablesReader = new ChainedReader.Builder().addReader(newTablesSnapshotReader).addReader(newTablesBinlogReader).build();
    }

    /**
     * Create and return a {@link ReconcilingBinlogReader} for the two binlog readers contained in this
     * ParallelSnapshotReader.
     * @return a {@link ReconcilingBinlogReader}
     */
    public ReconcilingBinlogReader createReconcilingBinlogReader(BinlogReader unifiedReader) {
        return new ReconcilingBinlogReader(oldTablesReader, newTablesBinlogReader, unifiedReader);
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
        logger.info("POLLING PARALLEL SNAPSHOT READER");
        List<SourceRecord> allRecords = oldTablesReader.poll();
        List<SourceRecord> newTablesRecords = newTablesReader.poll();
        logger.info("INNER READERS POLL SIZES: {}, {}",
                    allRecords == null? -1 : allRecords.size(),
                    newTablesRecords == null? -1 : newTablesRecords.size());
        if (newTablesRecords != null) {
            if (allRecords == null) {
                allRecords = newTablesRecords;
            } else {
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
        // trying something: stop the inner readers?
        logger.info("COMPLETING THE PARALLELSNAPSHOTREADER SUCCESSFULLY");
        stop();

        Runnable completionHandler = uponCompletion.getAndSet(null); // set to null so that we call it only once
        if (completionHandler != null) {
            completionHandler.run();
        }
    }

    @Override
    public String name() {
        return "parallelSnapshotReader";
    }

    /**
     * A Halting Predicate for the parallel snapshot reader
     *
     * TODO: more documentation (that is correct)
     */
    /*package local*/ static class ParallelHaltingPredicate implements Predicate<SourceRecord> {

        private final Logger logger = LoggerFactory.getLogger(getClass());

        private volatile AtomicBoolean thisReaderNearEnd;
        private volatile AtomicBoolean otherReaderNearEnd;

        // The minimum duration we must be within before we attempt to halt.
        private final Duration minHaltingDuration;
        // todo maybe this should eventually be configured, but for now the time diff were are interested in
        // is hard coded in as 5 minutes.
        private static final Duration DEFAULT_MIN_HALTING_DURATION = Duration.ofMinutes(5);

        /*package local*/ ParallelHaltingPredicate(AtomicBoolean thisReaderNearEndRef,
                                                   AtomicBoolean otherReaderNearEndRef) {
            this(thisReaderNearEndRef, otherReaderNearEndRef, DEFAULT_MIN_HALTING_DURATION);
        }

        /*package local*/ ParallelHaltingPredicate(AtomicBoolean thisReaderNearEndRef,
                                                   AtomicBoolean otherReaderNearEndRef,
                                                   Duration minHaltingDuration) {
            this.otherReaderNearEnd = otherReaderNearEndRef;
            this.thisReaderNearEnd = thisReaderNearEndRef;
            this.minHaltingDuration = minHaltingDuration;
        }

        @Override
        public boolean test(SourceRecord ourSourceRecord) {
            logger.info("TESTING PARALLEL HALTING PREDICATE: {}", ourSourceRecord.sourceOffset().toString());
            // we assume if we ever end up near the end of the binlog, then we will remain there.
            if (!thisReaderNearEnd.get()) {
                //ourSourceRecord.value()
                Long sourceRecordTimestamp = (Long) ourSourceRecord.sourceOffset().get(SourceInfo.TIMESTAMP_KEY);
                if (sourceRecordTimestamp == null) {
                    // TODO timestamp is null because it's not the -real- offset!!
                    // it's the fake, previous offset from the other reader
                    // in this case, it's from a snapshot. So that's why it has no timestamp.
                    logger.info ("TIMESTAMP IS NULL! (FOR SOME REASON?!)");
                    return true;
                }
                Instant recordTimestamp = Instant.ofEpochSecond(sourceRecordTimestamp);
                Instant now = Instant.now();
                Duration durationToEnd =
                    Duration.between(recordTimestamp,
                        now);
                if (durationToEnd.compareTo(minHaltingDuration) <= 0) {
                    // we are within minHaltingDuration of the end
                    logger.info("HALTING PREDICATE: THIS READER NEAR END");
                    thisReaderNearEnd.set(true);
                }
            }
            // return false if both readers are near end, true otherwise.
            return !(thisReaderNearEnd.get() && otherReaderNearEnd.get());
        }
    }
}
