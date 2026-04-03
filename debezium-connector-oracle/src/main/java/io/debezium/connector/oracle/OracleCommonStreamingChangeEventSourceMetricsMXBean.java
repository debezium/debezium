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
