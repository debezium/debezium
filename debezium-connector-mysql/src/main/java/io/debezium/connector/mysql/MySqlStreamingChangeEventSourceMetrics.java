/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import com.github.shyiko.mysql.binlog.BinaryLogClient;

import io.debezium.connector.base.ChangeEventQueueMetrics;
import io.debezium.connector.binlog.metrics.BinlogStreamingChangeEventSourceMetrics;
import io.debezium.pipeline.metrics.CapturedTablesSupplier;
import io.debezium.pipeline.source.spi.EventMetadataProvider;

/**
 * @author Randall Hauch
 */
public class MySqlStreamingChangeEventSourceMetrics
        extends BinlogStreamingChangeEventSourceMetrics<MySqlDatabaseSchema, MySqlPartition> {

    public MySqlStreamingChangeEventSourceMetrics(MySqlTaskContext taskContext,
                                                  ChangeEventQueueMetrics changeEventQueueMetrics,
                                                  EventMetadataProvider metadataProvider,
                                                  CapturedTablesSupplier capturedTablesSupplier,
                                                  BinaryLogClient client) {
        super(taskContext, changeEventQueueMetrics, metadataProvider, capturedTablesSupplier, client);
    }
}
