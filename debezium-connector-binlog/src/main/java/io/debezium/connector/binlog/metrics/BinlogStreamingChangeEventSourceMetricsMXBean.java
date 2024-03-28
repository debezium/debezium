/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog.metrics;

import io.debezium.pipeline.metrics.StreamingChangeEventSourceMetricsMXBean;

/**
 * @author Chris Cranford
 */
public interface BinlogStreamingChangeEventSourceMetricsMXBean extends StreamingChangeEventSourceMetricsMXBean {
    /**
     * Name of the current binlog file being read by underlying binlog client.
     */
    String getBinlogFilename();

    /**
     * Current binlog offset position being read by underlying binlog client.
     */
    long getBinlogPosition();

    /**
     * Current global transaction identifier (GTID) being read by underlying binlog client.
     */
    String getGtidSet();

    /**
     * Tracks the number of events skipped by underlying binlog client, generally due to the client
     * being unable to properly deserialize the event.
     */
    long getNumberOfSkippedEvents();

    /**
     * Tracks the number of times the underlying binlog client has been disconnected from the database.
     */
    long getNumberOfDisconnects();

    /**
     * Tracks the number of committed transactions.
     */
    long getNumberOfCommittedTransactions();

    /**
     * Tracks the number of rolled back transactions.
     */
    long getNumberOfRolledBackTransactions();

    /**
     * Tracks the number of transactions which are not well-formed.
     * Example - The connector sees a commit-transaction event without a matching begin-transaction event.
     */
    long getNumberOfNotWellFormedTransactions();

    /**
     * Tracks the number of transactions that contains events that had more entries than could be contained
     * within the connectors binlog connector's {@see io.debezium.connector.binlog.EventBuffer} instance.
     */
    long getNumberOfLargeTransactions();

    /**
     * Tracks if the connector is running using global transaction identifiers (GTID) to track current offset.
     * @return true if using global transaction identifiers, false if not.
     */
    boolean getIsGtidModeEnabled();
}
