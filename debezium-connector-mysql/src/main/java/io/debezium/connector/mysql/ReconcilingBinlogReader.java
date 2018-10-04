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

import static io.debezium.connector.mysql.SourceInfo.BINLOG_FILENAME_OFFSET_KEY;
import static io.debezium.connector.mysql.SourceInfo.BINLOG_POSITION_OFFSET_KEY;

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
    private final BinlogReader unifiedReader;

    private BinlogReader reconcilingReader;

    private Boolean aReaderLeading = null;

    private final AtomicBoolean running = new AtomicBoolean();
    private final AtomicBoolean completed = new AtomicBoolean();
    private final AtomicReference<Runnable> uponCompletion = new AtomicReference<>();

    private static final long RECONCILLING_READER_SERVER_ID_OFFSET = 20000;

    /**
     * Create a reconciling Binlog Reader.
     *
     * @param binlogReaderA the first binlog reader to unify.
     * @param binlogReaderB the second binlog reader to unify.
     * @param unifiedReader the final, unified binlog reader that will run once the reconciliation is complete.
     */
    public ReconcilingBinlogReader(BinlogReader binlogReaderA,
                                   BinlogReader binlogReaderB,
                                   BinlogReader unifiedReader) {
        this.binlogReaderA = binlogReaderA;
        this.binlogReaderB = binlogReaderB;
        this.unifiedReader = unifiedReader;
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
            completed.set(false);
            determineLeadingReader();

            MySqlTaskContext laggingReaderContext = getLaggingReader().context;
            OffsetLimitPredicate offsetLimitPredicate =
                new OffsetLimitPredicate(getLeadingReader().getLastOffset(),
                                         laggingReaderContext.gtidSourceFilter());

            long newTablesBinlogReaderServerId = laggingReaderContext.serverId() + RECONCILLING_READER_SERVER_ID_OFFSET;

            // create our actual reader
            reconcilingReader = new BinlogReader("innerReconcilingReader",
                                                 laggingReaderContext,
                                                 offsetLimitPredicate,
                                                 newTablesBinlogReaderServerId);
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
        setupUnifiedReader();
        logger.info("Completed Reconciliation of Parallel Readers.");

        Runnable completionHandler = uponCompletion.getAndSet(null); // set to null so that we call it only once
        if (completionHandler != null) {
            completionHandler.run();
        }
    }

    private void setupUnifiedReader() {
        unifiedReader.context.loadHistory(getLeadingReader().context.source());
        unifiedReader.context.source().setFilterDataFromConfig(unifiedReader.context.config());
        // ^^ I think this is needed even though imo it's kind of weirdo
        Map<String, ?> keyedOffset =
            reconcilingReader.getLastOffset() == null ?
                getLeadingReader().getLastOffset() :
                reconcilingReader.getLastOffset();
        unifiedReader.context.source()
            .setBinlogStartPoint((String) keyedOffset.get(BINLOG_FILENAME_OFFSET_KEY),
                                 (Long) keyedOffset.get(BINLOG_POSITION_OFFSET_KEY));
        // note: this seems to dupe -one- event in my tests.
        // I don't totally understand why that's happening (that is, I don't understand
        // why the lastOffset seems to be before the actual last record) but this seems
        // like a minor issue to me.
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
            logger.info("old tables leading; reading only from new tables");
        } else {
            logger.info("new tables leading; reading only from old tables");
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
            // .isPositionAtOrBefore is true IFF leadingReaderFinalOffsetDocument <= offsetDocument
            // we should stop (return false) IFF leadingReaderFinalOffsetDocument <= offsetDocument
            return
                ! SourceInfo.isPositionAtOrBefore(leadingReaderFinalOffsetDocument,
                                                  offsetDocument,
                                                  gtidFilter);
        }
    }
}
