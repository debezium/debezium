/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb.metrics;

import io.debezium.connector.base.ChangeEventQueueMetrics;
import io.debezium.connector.binlog.metrics.BinlogSnapshotChangeEventSourceMetrics;
import io.debezium.connector.mariadb.MariaDbPartition;
import io.debezium.connector.mariadb.MariaDbTaskContext;
import io.debezium.pipeline.source.spi.EventMetadataProvider;

/**
 * Tracks the snapshot metrics specific for MariaDB.
 *
 * @author Chris Cranford
 */
public class MariaDbSnapshotChangeEventSourceMetrics extends BinlogSnapshotChangeEventSourceMetrics<MariaDbPartition> {
    public MariaDbSnapshotChangeEventSourceMetrics(MariaDbTaskContext taskContext,
                                                   ChangeEventQueueMetrics changeEventQueueMetrics,
                                                   EventMetadataProvider metadataProvider) {
        super(taskContext, changeEventQueueMetrics, metadataProvider);
    }
}
