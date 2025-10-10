/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb.metrics;

import io.debezium.connector.base.ChangeEventQueueMetrics;
import io.debezium.connector.binlog.metrics.BinlogStreamingChangeEventSourceMetrics;
import io.debezium.connector.mariadb.MariaDbDatabaseSchema;
import io.debezium.connector.mariadb.MariaDbPartition;
import io.debezium.connector.mariadb.MariaDbTaskContext;
import io.debezium.pipeline.metrics.CapturedTablesSupplier;
import io.debezium.pipeline.source.spi.EventMetadataProvider;

/**
 * Tracks the streaming metrics specific for MariaDB.
 *
 * @author Chris Cranford
 */
public class MariaDbStreamingChangeEventSourceMetrics
        extends BinlogStreamingChangeEventSourceMetrics<MariaDbDatabaseSchema, MariaDbPartition> {
    public MariaDbStreamingChangeEventSourceMetrics(MariaDbTaskContext taskContext,
                                                    ChangeEventQueueMetrics changeEventQueueMetrics,
                                                    EventMetadataProvider eventMetadataProvider,
                                                    CapturedTablesSupplier capturedTablesSupplier) {
        super(taskContext, changeEventQueueMetrics, eventMetadataProvider, capturedTablesSupplier);
    }
}
