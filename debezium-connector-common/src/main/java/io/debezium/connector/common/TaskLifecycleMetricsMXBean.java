/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.common;

/**
 * Exposes task lifecycle metrics via JMX.
 * Registered independently of the streaming/snapshot metrics so that
 * retry counts are visible even when the coordinator has not yet started.
 */
public interface TaskLifecycleMetricsMXBean {

    int getStartRetryCount();

    int getStartMaxRetries();

    int getPollRetryCount();

    String getTaskState();

    long getReplicationSlotLagInBytes();
}
