/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

/**
 * @author Randall Hauch
 *
 */
public interface BinlogReaderMetricsMXBean {

    boolean isConnected();
    String getBinlogFilename();
    long getBinlogPosition();
    String getGtidSet();
    
    String getLastEvent();
    long getSecondsSinceLastEvent();
    long getSecondsBehindMaster();
    long getTotalNumberOfEventsSeen();
    long getNumberOfSkippedEvents();
    long getNumberOfDisconnects();
    void reset();

    long getNumberOfCommittedTransactions();
    long getNumberOfRolledBackTransactions();
    long getNumberOfNotWellFormedTransactions();
    long getNumberOfLargeTransactions();
}
