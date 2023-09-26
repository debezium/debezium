/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import io.debezium.pipeline.metrics.StreamingChangeEventSourceMetricsMXBean;

/**
 * Common metrics across all Oracle streaming adapters.
 *
 * @author Chris Cranford
 */
public interface OracleCommonStreamingChangeEventSourceMetricsMXBean extends StreamingChangeEventSourceMetricsMXBean {

    @Deprecated
    default long getMaxCapturedDmlInBatch() {
        return getMaxCapturedDmlCountInBatch();
    }

    @Deprecated
    default long getNetworkConnectionProblemsCounter() {
        // Was used specifically by Oracle tests previously and not in runtime code.
        return 0L;
    }

    /**
     * @return total number of schema change parser errors
     * @deprecated to be removed in Debezium 2.7, replaced by {{@link #getTotalSchemaChangeParseErrorCount()}}
     */
    @Deprecated
    default long getUnparsableDdlCount() {
        return getTotalSchemaChangeParseErrorCount();
    }

    /**
     * @return total number of schema change events that resulted in a parser failure
     */
    long getTotalSchemaChangeParseErrorCount();

    /**
     * @return the number of data manipulation (insert, update, delete) events during the last batch
     */
    long getLastCapturedDmlCount();

    /**
     * @return maximum number of data manipulation (insert, update, delete) events during a single batch
     */
    long getMaxCapturedDmlCountInBatch();

    /**
     * @return the number of captured data manipulation (insert, update, delete) events
     */
    long getTotalCapturedDmlCount();

    /**
     * @return number of warnings detected by the connector
     */
    long getWarningCount();

    /**
     * @return number of errors detected by the connector
     */
    long getErrorCount();

}
