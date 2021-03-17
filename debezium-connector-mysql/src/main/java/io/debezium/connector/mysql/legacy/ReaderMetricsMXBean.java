/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.legacy;

/**
 * Metrics that are common for both snapshot and binlog readers
 *
 * @author Jiri Pechanec
 *
 */
public interface ReaderMetricsMXBean {

    String[] getMonitoredTables();
}
