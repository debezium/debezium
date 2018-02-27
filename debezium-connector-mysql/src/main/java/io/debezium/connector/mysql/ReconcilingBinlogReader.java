/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import io.debezium.document.Document;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

/**
 * A reader that unifies the binlog positions of two binlog readers.
 *
 * To do this, at start time we evaluate the (now completed) states of the two binlog
 * readers we want to unify, and create a new {@link BinlogReader} duplicating the
 * lagging reader, but with a halting predicate that will halt it once it has passed the
 * final position of the leading reader.
 */
public class ReconcilingBinlogReader implements Reader {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final BinlogReader binlogReaderA;
    private final BinlogReader binlogReaderB;

    private BinlogReader reconcilingReader;

    private Boolean aReaderLeading = null;

    private final AtomicBoolean running = new AtomicBoolean();
    private final AtomicBoolean completed = new AtomicBoolean();
    private final AtomicReference<Runnable> uponCompletion = new AtomicReference<>();

    /**
     * Create a catch up reader.
     */
    public ReconcilingBinlogReader(BinlogReader binlogReaderA,
                                   BinlogReader binlogReaderB) {
        this.binlogReaderA = binlogReaderA;
        this.binlogReaderB = binlogReaderB;
        uponCompletion.set(new LogCompletionRunnable(null));
    }

    @Override
    public String name() {
        return "reconcilingBinlogReader";
    }

    @Override
    public State state() {
        if (running.get()) {
            return State.RUNNING;
        }
        return completed.get() ? State.STOPPED : State.STOPPING;
    }

    @Override
    public void uponCompletion(Runnable handler) {
        uponCompletion.set(new LogCompletionRunnable(handler));
    }

    @Override
    public void start() {
        if (running.compareAndSet(false, true)) {
            completed.set(false);
            determineLeadingReader();

            MySqlTaskContext laggingReaderContext = getLaggingReader().context;
            OffsetLimitPredicate offsetLimitPredicate =
                new OffsetLimitPredicate(getLeadingReader().getLastOffset(),
                                         laggingReaderContext.gtidSourceFilter());
            // create our actual reader
            reconcilingReader = new BinlogReader("innerReconcilingReader",
                                                 laggingReaderContext,
                                                 offsetLimitPredicate);
            reconcilingReader.start();
        }
    }

    @Override
    public void stop() {
        try {
            logger.info("Stopping the {} reader", reconcilingReader.name());
            reconcilingReader.stop();
        } catch (Throwable t) {
            logger.error("Unexpected error stopping the {} reader", reconcilingReader.name());
        }
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        return reconcilingReader.poll();
    }

    private void determineLeadingReader() {
        Map<String, ?> aOffset = binlogReaderA.getLastOffset();
        Map<String, ?> bOffset = binlogReaderB.getLastOffset();
        boolean aNotStopped = binlogReaderA.state() != State.STOPPED;
        boolean bNotStopped = binlogReaderB.state() != State.STOPPED;
        if (aOffset == null || bOffset == null || aNotStopped || bNotStopped) {
            throw new IllegalStateException("Cannot determine leading reader until both source readers have completed.");
        }

        Document aDocument = SourceInfo.createDocumentFromOffset(aOffset);
        Document bDocument = SourceInfo.createDocumentFromOffset(bOffset);

        aReaderLeading = SourceInfo.isPositionAtOrBefore(bDocument,
                                                         aDocument,
                                                         binlogReaderA.context.gtidSourceFilter());
    }

    /*package private*/ BinlogReader getLeadingReader() {
        checkLaggingLeadingInfo();
        return aReaderLeading? binlogReaderA : binlogReaderB;
    }

    /*package private*/ BinlogReader getLaggingReader() {
        checkLaggingLeadingInfo();
        return aReaderLeading? binlogReaderB : binlogReaderA;
    }

    private void checkLaggingLeadingInfo() {
        if (aReaderLeading == null) {
            throw new IllegalStateException("Cannot return leading or lagging readers until this reader has started.");
        }
    }

    private class LogCompletionRunnable implements Runnable {

        Runnable otherRunnable;

        LogCompletionRunnable(Runnable otherRunnable) {
            this.otherRunnable = otherRunnable;
        }

        @Override
        public void run() {
            if (otherRunnable != null) {
                otherRunnable.run();
            }
            logger.info("Completed Reconciling Readers.");
        }
    }

    // package private for testing purposes
    /**
     * A Predicate that returns false for any record beyond a given offset.
     */
    /*package private*/ static class OffsetLimitPredicate implements Predicate<SourceRecord> {

        private Document leadingReaderFinalOffsetDocument;
        private Predicate<String> gtidFilter;

        /*package private*/ OffsetLimitPredicate(Map<String, ?> leadingReaderFinalOffset,
                                                 Predicate<String> gtidFilter) {
            this.leadingReaderFinalOffsetDocument = SourceInfo.createDocumentFromOffset(leadingReaderFinalOffset);
            this.gtidFilter = gtidFilter;

        }

        @Override
        public boolean test(SourceRecord sourceRecord) {
            Document offsetDocument = SourceInfo.createDocumentFromOffset(sourceRecord.sourceOffset());
            return SourceInfo.isPositionAtOrBefore(leadingReaderFinalOffsetDocument,
                                                   offsetDocument,
                                                   gtidFilter);
        }
    }
}
