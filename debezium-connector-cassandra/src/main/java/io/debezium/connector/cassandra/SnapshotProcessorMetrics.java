/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import static io.debezium.connector.cassandra.CassandraConnectorTask.METRIC_REGISTRY_INSTANCE;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.codahale.metrics.Gauge;

public class SnapshotProcessorMetrics {
    private final AtomicInteger tableCount = new AtomicInteger();
    private final AtomicInteger remainingTableCount = new AtomicInteger();
    private final AtomicBoolean snapshotRunning = new AtomicBoolean();
    private final AtomicBoolean snapshotCompleted = new AtomicBoolean();
    private final AtomicBoolean snapshotAborted = new AtomicBoolean();
    private final AtomicLong startTime = new AtomicLong();
    private final AtomicLong stopTime = new AtomicLong();
    private final ConcurrentMap<String, Long> rowsScanned = new ConcurrentHashMap<>();

    public void registerMetrics() {
        METRIC_REGISTRY_INSTANCE.register("total-table-count", (Gauge<Integer>) this::getTotalTableCount);
        METRIC_REGISTRY_INSTANCE.register("remaining-table-count", (Gauge<Integer>) this::getRemainingTableCount);
        METRIC_REGISTRY_INSTANCE.register("snapshot-completed", (Gauge<Boolean>) this::snapshotCompleted);
        METRIC_REGISTRY_INSTANCE.register("snapshot-running", (Gauge<Boolean>) this::snapshotRunning);
        METRIC_REGISTRY_INSTANCE.register("snapshot-aborted", (Gauge<Boolean>) this::snapshotAborted);
        METRIC_REGISTRY_INSTANCE.register("row-scanned", (Gauge<Map<String, Long>>) this::rowsScanned);
        METRIC_REGISTRY_INSTANCE.register("snapshot-duration-in-seconds", (Gauge<Long>) this::snapshotDurationInSeconds);
    }

    public void unregisterMetrics() {
        METRIC_REGISTRY_INSTANCE.remove("total-table-count");
        METRIC_REGISTRY_INSTANCE.remove("remaining-table-count");
        METRIC_REGISTRY_INSTANCE.remove("snapshot-completed");
        METRIC_REGISTRY_INSTANCE.remove("snapshot-running");
        METRIC_REGISTRY_INSTANCE.remove("snapshot-aborted");
        METRIC_REGISTRY_INSTANCE.remove("row-scanned");
        METRIC_REGISTRY_INSTANCE.remove("snapshot-duration-in-seconds");
    }

    public void setTableCount(int value) {
        tableCount.set(value);
        remainingTableCount.set(value);
    }

    public void completeTable() {
        remainingTableCount.decrementAndGet();
    }

    public void startSnapshot() {
        snapshotRunning.set(true);
        snapshotCompleted.set(false);
        snapshotAborted.set(false);
        startTime.set(System.currentTimeMillis());
        stopTime.set(0L);
    }

    public void stopSnapshot() {
        snapshotCompleted.set(true);
        snapshotAborted.set(false);
        snapshotRunning.set(false);
        stopTime.set(System.currentTimeMillis());
    }

    public void abortSnapshot() {
        snapshotCompleted.set(false);
        snapshotAborted.set(true);
        snapshotRunning.set(false);
        stopTime.set(System.currentTimeMillis());
    }

    public void setRowsScanned(String key, Long value) {
        rowsScanned.put(key, value);
    }

    private int getTotalTableCount() {
        return tableCount.get();
    }

    private int getRemainingTableCount() {
        return remainingTableCount.get();
    }

    private boolean snapshotCompleted() {
        return snapshotCompleted.get();
    }

    private boolean snapshotRunning() {
        return snapshotRunning.get();
    }

    private boolean snapshotAborted() {
        return snapshotAborted.get();
    }

    private Map<String, Long> rowsScanned() {
        return rowsScanned;
    }

    private long snapshotDurationInSeconds() {
        long startMillis = startTime.get();
        if (startMillis == 0L) {
            return 0;
        }
        long stopMillis = stopTime.get();
        if (stopMillis <= 0L) {
            stopMillis = System.currentTimeMillis();
        }
        return (stopMillis - startMillis) / 1000L;
    }
}
