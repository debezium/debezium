/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract base class for {@link CacheProvider} implementations.
 *
 * @author Chris Cranford
 */
public abstract class AbstractCacheProvider<T extends Transaction> implements CacheProvider<T> {

    private final Logger LOGGER = LoggerFactory.getLogger(AbstractCacheProvider.class);

    /**
     * Clears whatever a previous run left in a persistent buffer. The buffer is transient: the connector never
     * resumes from it and re-mines the in-flight transactions from its offsets, so the state a crash, or a stop
     * with {@code log.mining.buffer.drop.on.stop} disabled, left behind is dropped before mining starts.
     */
    protected void clearRetainedState() {
        LOGGER.info("Clearing any transaction buffer state left by a previous run; in-flight transactions are re-mined from the offsets");
        getTransactionCache().clear();
        getProcessedTransactionsCache().clear();
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
