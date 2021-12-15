/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.metrics;

import org.slf4j.Logger;

import io.debezium.pipeline.source.spi.DataChangeEventListener;

/**
 * Common API for all change event source metrics regardless of the connector phase.
 */
interface ChangeEventSourceMetrics extends DataChangeEventListener {

    void register(Logger logger);

    void unregister(Logger logger);
}
