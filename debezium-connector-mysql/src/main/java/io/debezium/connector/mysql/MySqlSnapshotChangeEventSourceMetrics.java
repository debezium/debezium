/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import io.debezium.connector.base.ChangeEventQueueMetrics;
import io.debezium.connector.binlog.metrics.BinlogSnapshotChangeEventSourceMetrics;
import io.debezium.pipeline.source.spi.EventMetadataProvider;

/**
 * @author Randall Hauch
 *
 */
public class MySqlSnapshotChangeEventSourceMetrics extends BinlogSnapshotChangeEventSourceMetrics<MySqlPartition> {
    public MySqlSnapshotChangeEventSourceMetrics(MySqlTaskContext taskContext,
                                                 ChangeEventQueueMetrics changeEventQueueMetrics,
                                                 EventMetadataProvider eventMetadataProvider) {
        super(taskContext, changeEventQueueMetrics, eventMetadataProvider);
    }
}
