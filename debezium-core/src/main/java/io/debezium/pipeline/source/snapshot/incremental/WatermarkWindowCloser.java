/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot.incremental;

import java.sql.SQLException;

import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;

public interface WatermarkWindowCloser {

    void closeWindows(Partition partition, OffsetContext offsetContext, String chunkId) throws SQLException;
}
