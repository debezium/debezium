/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.oracle.logminer.events.LogMinerRedoEvent;

/**
 * Abstract base class for {@link CacheProvider} implementations.
 *
 * @author Chris Cranford
 */
public abstract class AbstractCacheProvider<T extends Transaction> implements CacheProvider<T> {

    private final Logger LOGGER = LoggerFactory.getLogger(AbstractCacheProvider.class);

    /**
     * Marks a retained buffer that stores {@link LogMinerRedoEvent}. It lives in the processed transactions
     * cache, keyed so that it never collides with a transaction id, with a value no SCN-based cleanup removes.
     * Buffers written by versions that stored parsed events carry no mark.
     */
    protected static final String REDO_EVENT_FORMAT_MARK = "$dbz-event-format:redo-v1";

    /**
     * Clears every cache when a retained buffer was written by a version that stored parsed events, since its
     * entries cannot be read, and marks the buffer with the stored format. The connector then re-mines from
     * its offsets, which needs the archive logs back to the offset SCN, as any restart does.
     */
    protected void verifyStoredEventFormat() {
        final LogMinerCache<String, String> marks = getProcessedTransactionsCache();
        if (!marks.containsKey(REDO_EVENT_FORMAT_MARK)) {
            if (hasBufferedState()) {
                LOGGER.warn("The retained transaction buffer was written by a version that stored parsed events; clearing it. " +
                        "The connector re-mines from its offsets, which requires the archive logs back to the offset SCN.");
                getTransactionCache().clear();
                marks.clear();
            }
            marks.put(REDO_EVENT_FORMAT_MARK, String.valueOf(Long.MAX_VALUE));
        }
    }

    private boolean hasBufferedState() {
        return !getTransactionCache().isEmpty() || !getProcessedTransactionsCache().isEmpty();
    }

    @Override
    public void displayCacheStatistics() {
        LOGGER.info("Overall Cache Statistics:");
        LOGGER.info("\tTransactions        : {}", getTransactionCache().getTransactionCount());
        LOGGER.info("\tRecent Transactions : {}", getProcessedTransactionsCache().size());
        LOGGER.info("\tEvents              : {}", getTransactionCache().getTransactionEvents());
        if (!getTransactionCache().isEmpty() && LOGGER.isDebugEnabled()) {
            getTransactionCache().eventKeys(stream -> stream.forEach(eventKey -> LOGGER.debug("\t\tFound Key: {}", eventKey)));
        }
    }

}
