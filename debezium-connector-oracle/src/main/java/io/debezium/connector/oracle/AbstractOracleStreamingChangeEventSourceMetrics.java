/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import java.util.concurrent.atomic.AtomicLong;

import io.debezium.annotation.ThreadSafe;
import io.debezium.connector.base.ChangeEventQueueMetrics;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.pipeline.metrics.CapturedTablesSupplier;
import io.debezium.pipeline.metrics.DefaultStreamingChangeEventSourceMetrics;
import io.debezium.pipeline.source.spi.EventMetadataProvider;

/**
 * Common Oracle Streaming Metrics for all connector adapters.
 *
 * @author Chris Cranford
 */
@ThreadSafe
public abstract class AbstractOracleStreamingChangeEventSourceMetrics
        extends DefaultStreamingChangeEventSourceMetrics<OraclePartition>
        implements OracleCommonStreamingChangeEventSourceMetricsMXBean {

    private final AtomicLong schemaChangeParseErrorCount = new AtomicLong();
    private final AtomicLong committedTransactionCount = new AtomicLong();
    private final AtomicLong lastCapturedDmlCount = new AtomicLong();
    private final AtomicLong maxCapturedDmlCount = new AtomicLong();
    private final AtomicLong totalCapturedDmlCount = new AtomicLong();
    private final AtomicLong warningCount = new AtomicLong();
    private final AtomicLong errorCount = new AtomicLong();

    public AbstractOracleStreamingChangeEventSourceMetrics(CdcSourceTaskContext taskContext,
                                                           ChangeEventQueueMetrics changeEventQueueMetrics,
                                                           EventMetadataProvider metadataProvider,
                                                           CapturedTablesSupplier capturedTablesSupplier) {
        super(taskContext, changeEventQueueMetrics, metadataProvider, capturedTablesSupplier);
    }

    @Override
    public void reset() {
        super.reset();
        committedTransactionCount.set(0);
        lastCapturedDmlCount.set(0);
        maxCapturedDmlCount.set(0);
        totalCapturedDmlCount.set(0);
        warningCount.set(0);
        errorCount.set(0);
    }

    @Override
    public long getNumberOfCommittedTransactions() {
        return committedTransactionCount.get();
    }

    @Override
    public long getTotalSchemaChangeParseErrorCount() {
        return schemaChangeParseErrorCount.get();
    }

    @Override
    public long getLastCapturedDmlCount() {
        return lastCapturedDmlCount.get();
    }

    @Override
    public long getMaxCapturedDmlCountInBatch() {
        return maxCapturedDmlCount.get();
    }

    @Override
    public long getTotalCapturedDmlCount() {
        return totalCapturedDmlCount.get();
    }

    @Override
    public long getWarningCount() {
        return warningCount.get();
    }

    @Override
    public long getErrorCount() {
        return errorCount.get();
    }

    /**
     * Set the last iteration's number of data manipulation (insert, update, delete) events.
     *
     * @param lastDmlCount the last number of insert, update, and delete events
     */
    public void setLastCapturedDmlCount(int lastDmlCount) {
        lastCapturedDmlCount.set(lastDmlCount);
        if (maxCapturedDmlCount.get() < lastDmlCount) {
            maxCapturedDmlCount.set(lastDmlCount);
        }
        totalCapturedDmlCount.getAndAdd(lastDmlCount);
    }

    /**
     * Increments the total number of schema change parser errors.
     */
    public void incrementSchemaChangeParseErrorCount() {
        schemaChangeParseErrorCount.incrementAndGet();
    }

    /**
     * Increments the number of warning messages written to the connector log.
     */
    public void incrementWarningCount() {
        warningCount.incrementAndGet();
    }

    /**
     * Increments the number of error messages written to the connector log.
     */
    public void incrementErrorCount() {
        errorCount.incrementAndGet();
    }

    /**
     * Increments the committed transaction count.
     */
    public void incrementCommittedTransactionCount() {
        committedTransactionCount.incrementAndGet();
    }

}
