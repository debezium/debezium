/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import io.debezium.pipeline.metrics.StreamingChangeEventSourceMetricsMXBean;

/**
 * @author Randall Hauch
 *
 */
public interface BinlogReaderMetricsMXBean extends StreamingChangeEventSourceMetricsMXBean {

    String getBinlogFilename();
    long getBinlogPosition();
    String getGtidSet();
    
    long getSecondsBehindMaster();
    long getNumberOfSkippedEvents();
    long getNumberOfDisconnects();

    long getNumberOfCommittedTransactions();
    long getNumberOfRolledBackTransactions();
    long getNumberOfNotWellFormedTransactions();
    long getNumberOfLargeTransactions();
}
