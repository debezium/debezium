/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.common;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.ThreadSafe;

/**
 * Thread-safe implementation of task lifecycle metrics.
 * Lives for the entire task lifetime, independent of coordinator/streaming metrics.
 */
@ThreadSafe
public class TaskLifecycleMetrics implements TaskLifecycleMetricsMXBean {

    private static final Logger LOGGER = LoggerFactory.getLogger(TaskLifecycleMetrics.class);

    private final AtomicInteger startRetryCount = new AtomicInteger();
    private final AtomicInteger startMaxRetries = new AtomicInteger(-1);
    private final AtomicInteger pollRetryCount = new AtomicInteger();
    private final AtomicReference<String> taskState = new AtomicReference<>("INITIAL");
    private final AtomicLong cachedReplicationSlotLagInBytes = new AtomicLong();
    private volatile LongSupplier replicationSlotLagSupplier;

    @Override
    public int getStartRetryCount() {
        return startRetryCount.get();
    }

    public void setStartRetryCount(int count) {
        startRetryCount.set(count);
    }

    @Override
    public int getStartMaxRetries() {
        return startMaxRetries.get();
    }

    public void setStartMaxRetries(int max) {
        startMaxRetries.set(max);
    }

    @Override
    public int getPollRetryCount() {
        return pollRetryCount.get();
    }

    public void setPollRetryCount(int count) {
        pollRetryCount.set(count);
    }

    @Override
    public String getTaskState() {
        return taskState.get();
    }

    public void setTaskState(String state) {
        taskState.set(state);
    }

    /**
     * Sets the supplier that computes replication slot lag on demand.
     * Called once during connector setup with a reference to the JDBC connection.
     */
    public void setReplicationSlotLagSupplier(LongSupplier supplier) {
        this.replicationSlotLagSupplier = supplier;
    }

    @Override
    public long getReplicationSlotLagInBytes() {
        LongSupplier supplier = replicationSlotLagSupplier;
        if (supplier != null) {
            try {
                long lag = supplier.getAsLong();
                cachedReplicationSlotLagInBytes.set(lag);
                return lag;
            }
            catch (Exception e) {
                LOGGER.debug("Failed to compute replication slot lag, returning cached value", e);
            }
        }
        return cachedReplicationSlotLagInBytes.get();
    }
}
