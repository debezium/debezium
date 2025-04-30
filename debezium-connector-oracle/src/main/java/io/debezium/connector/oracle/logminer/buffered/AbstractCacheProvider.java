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

    @Override
    public void displayCacheStatistics() {
        LOGGER.info("Overall Cache Statistics:");
        LOGGER.info("\tTransactions        : {}", getTransactionCache().getTransactionCount());
        LOGGER.info("\tRecent Transactions : {}", getProcessedTransactionsCache().size());
        LOGGER.info("\tSchema Changes      : {}", getSchemaChangesCache().size());
        LOGGER.info("\tEvents              : {}", getTransactionCache().getTransactionEvents());
        if (!getTransactionCache().isEmpty() && LOGGER.isDebugEnabled()) {
            getTransactionCache().eventKeys(stream -> stream.forEach(eventKey -> LOGGER.debug("\t\tFound Key: {}", eventKey)));
        }
    }

}
