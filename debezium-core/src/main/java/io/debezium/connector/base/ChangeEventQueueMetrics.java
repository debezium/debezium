/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.base;

public interface ChangeEventQueueMetrics {

    int totalCapacity();

    int remainingCapacity();

    long maxQueueSizeInBytes();

    long currentQueueSizeInBytes();
}
