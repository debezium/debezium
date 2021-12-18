/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.legacy;

import io.debezium.pipeline.metrics.traits.SchemaMetricsMXBean;

/**
 * Metrics that are common for both snapshot and binlog readers
 *
 * @author Jiri Pechanec
 *
 */
public interface ReaderMetricsMXBean extends SchemaMetricsMXBean {

    /**
     * @deprecated Superseded by the 'Captured Tables' metric. Use {@link #getCapturedTables()}.
     * Scheduled for removal in a future release.
     */
    @Deprecated
    String[] getMonitoredTables();
}
