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
        uponCompletion.set(handler);
    }

    @Override
    public void start() {
        if (running.compareAndSet(false, true)) {
            logger.info("RECONCILING-BINLOG-READER START");
            completed.set(false);
            determineLeadingReader();

            MySqlTaskContext laggingReaderContext = getLaggingReader().context;
            OffsetLimitPredicate offsetLimitPredicate =
                new OffsetLimitPredicate(getLeadingReader().getLastOffset(),
                                         laggingReaderContext.gtidSourceFilter());
            // create our actual reader
            logger.info("CREATING INNER RBR with offset {}", laggingReaderContext.source().offset());
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
        List<SourceRecord> innerReaderPoll = reconcilingReader.poll();
        if (innerReaderPoll == null) {
            completeSuccessfully();
        }
        return innerReaderPoll;
    }

    private void completeSuccessfully() {
        // if both readers have stopped, we need to stop.
        logger.info("PARALLEL SNAPSHOT READER IS DONE! NEXT UP IS {}", this.uponCompletion.get().getClass());
        running.compareAndSet(true, false);
        Runnable completionHandler = uponCompletion.getAndSet(null); // set to null so that we call it only once
        if (completionHandler != null) {
            completionHandler.run();
        }
    }

    private void determineLeadingReader() {
        Map<String, ?> aOffset = binlogReaderA.getLastOffset();
        Map<String, ?> bOffset = binlogReaderB.getLastOffset();
        boolean aNotStopped = binlogReaderA.state() != State.STOPPED;
        boolean bNotStopped = binlogReaderB.state() != State.STOPPED;
        boolean noOffsets = aOffset == null && bOffset == null;
        if (noOffsets || aNotStopped || bNotStopped) {
            throw new IllegalStateException("Cannot determine leading reader until both source readers have completed.");
        }

        // if one reader has not processed any events, its 'lastOffset' will be null.
        // in this case, it must the be the lagging reader.
        if (aOffset == null) {
            aReaderLeading = false;
        } else if (bOffset == null) {
            aReaderLeading = true;
        } else {
            Document aDocument = SourceInfo.createDocumentFromOffset(aOffset);
            Document bDocument = SourceInfo.createDocumentFromOffset(bOffset);

            aReaderLeading = SourceInfo.isPositionAtOrBefore(bDocument,
                                                             aDocument,
                                                             binlogReaderA.context.gtidSourceFilter());
        }

        if (aReaderLeading) {
            logger.info("OLD TABLES LEADING; READING ONLY FROM NEW TABLES");
        } else {
            logger.info("NEW TABLES LEADING; READING ONLY FROM OLD TABLES");
        }
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

    // package private for testing purposes
    /**
     * A Predicate that returns false for any record beyond a given offset.
     */
    /*package private*/ static class OffsetLimitPredicate implements Predicate<SourceRecord> {

        private final Logger logger = LoggerFactory.getLogger(getClass());

        private Map<String, ?> leadingReaderFinalOffset;
        private Document leadingReaderFinalOffsetDocument;
        private Predicate<String> gtidFilter;

        /*package private*/ OffsetLimitPredicate(Map<String, ?> leadingReaderFinalOffset,
                                                 Predicate<String> gtidFilter) {
            this.leadingReaderFinalOffset = leadingReaderFinalOffset;
            this.leadingReaderFinalOffsetDocument = SourceInfo.createDocumentFromOffset(leadingReaderFinalOffset);
            this.gtidFilter = gtidFilter;

        }

        @Override
        public boolean test(SourceRecord sourceRecord) {
            Document offsetDocument = SourceInfo.createDocumentFromOffset(sourceRecord.sourceOffset());
            boolean positionAtOrBefore = SourceInfo.isPositionAtOrBefore(leadingReaderFinalOffsetDocument,
                                                                    offsetDocument,
                                                                    gtidFilter);
            logger.info("{} VERSUS {} :OFFSETLIMITPREDICATE RETURNS {}", leadingReaderFinalOffset, sourceRecord.sourceOffset(), positionAtOrBefore);
            return positionAtOrBefore;
            // TODO obviously this isn't actually functional for real life but should hopefully be sufficient for me getting past some stuff for now.
            // another option would be to use binlog position. But at this point I'm getting into fixing the issues with isPositionAtOrBefore and I don't
            // really want to do that.
            //long leadingReaderFinalOffsetTsSec = (Long) leadingReaderFinalOffset.get(SourceInfo.TIMESTAMP_KEY);
            //long sourceRecordTsSec = (Long) sourceRecord.sourceOffset().get(SourceInfo.TIMESTAMP_KEY);
            //boolean result = sourceRecordTsSec < leadingReaderFinalOffsetTsSec;
            //logger.info("OFFSETLIMITPREDICATE RETURNING {}", result);
            //return result;
        }
    }
}
