/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.legacy;

import io.debezium.pipeline.metrics.StreamingChangeEventSourceMetricsMXBean;

/**
 * @author Randall Hauch
 *
 */
public interface BinlogReaderMetricsMXBean extends StreamingChangeEventSourceMetricsMXBean {

    /**
     * Name of the current MySQL binlog file being read by underlying mysql-binlog-client.
     */
    String getBinlogFilename();

    /**
     * Current MySQL binlog offset position being read by underlying mysql-binlog-client.
     */
    long getBinlogPosition();

    /**
     * Current MySQL Gtid being read by underlying mysql-binlog-client.
     */
    String getGtidSet();

    /**
     * Tracks the number of events skipped by underlying mysql-binlog-client, generally due to the client
     * being unable to properly deserialize the event.
     */
    long getNumberOfSkippedEvents();

    /**
     * Tracks the number of times the underlying mysql-binlog-client has been disconnected from MySQL.
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
     * Example - The connector sees a commit TX event without a matched begin TX event.
     */
    long getNumberOfNotWellFormedTransactions();

    /**
     * Tracks the number of transaction which contains events that contained more entries than could be contained
     * within the connectors {@see io.debezium.connector.mysql.EventBuffer} instance.
     */
    long getNumberOfLargeTransactions();

    /**
     * Tracks if the connector is running using Gtids to track current offset.
     * @return true if using Gtids, false if not.
     */
    boolean getIsGtidModeEnabled();
}
