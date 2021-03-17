/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import io.debezium.pipeline.metrics.SnapshotChangeEventSourceMetricsMXBean;

/**
 * @author Randall Hauch
 */
public interface MySqlSnapshotChangeEventSourceMetricsMXBean extends SnapshotChangeEventSourceMetricsMXBean {

    boolean getHoldingGlobalLock();
}
