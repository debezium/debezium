/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.metrics;

import io.debezium.pipeline.source.spi.DataChangeEventListener;
import io.debezium.pipeline.spi.Partition;

/**
 * Common API for all change event source metrics regardless of the connector phase.
 */
public interface ChangeEventSourceMetrics<P extends Partition> extends DataChangeEventListener<P> {

    void register();

    void unregister();
}
