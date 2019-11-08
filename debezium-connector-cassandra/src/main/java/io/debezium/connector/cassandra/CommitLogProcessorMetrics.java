/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import static io.debezium.connector.cassandra.CassandraConnectorTask.METRIC_REGISTRY_INSTANCE;

import java.util.concurrent.atomic.AtomicLong;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;

public class CommitLogProcessorMetrics {
    private String commitLogFilename = null;
    private AtomicLong commitLogPosition = new AtomicLong(-1L);
    private Counter numberOfProcessedMutations;
    private Counter numberOfUnrecoverableErrors;

    public void registerMetrics() {
        METRIC_REGISTRY_INSTANCE.register("commitlog-filename", (Gauge<String>) this::getCommitLogFilename);
        METRIC_REGISTRY_INSTANCE.register("commitlog-position", (Gauge<Long>) this::getCommitLogPosition);
        METRIC_REGISTRY_INSTANCE.register("number-of-processed-mutations", new Counter());
        METRIC_REGISTRY_INSTANCE.register("number-of-unrecoverable-errors", new Counter());
    }

    public void unregisterMetrics() {
        METRIC_REGISTRY_INSTANCE.remove("commitlog-filename");
        METRIC_REGISTRY_INSTANCE.remove("commitlog-position");
        METRIC_REGISTRY_INSTANCE.remove("number-of-processed-mutations");
        METRIC_REGISTRY_INSTANCE.remove("number-of-unrecoverable-errors");
    }

    public void onSuccess() {
        if (numberOfProcessedMutations == null) {
            numberOfProcessedMutations = METRIC_REGISTRY_INSTANCE.counter("number-of-processed-mutations");
        }
        numberOfProcessedMutations.inc();
    }

    public void onUnrecoverableError() {
        if (numberOfUnrecoverableErrors == null) {
            numberOfUnrecoverableErrors = METRIC_REGISTRY_INSTANCE.counter("number-of-unrecoverable-errors");
        }
        numberOfUnrecoverableErrors.inc();
    }

    public String getCommitLogFilename() {
        return commitLogFilename;
    }

    public long getCommitLogPosition() {
        return commitLogPosition.get();
    }

    public void setCommitLogFilename(String name) {
        this.commitLogFilename = name;
        setCommitLogPosition(-1L);
    }

    public void setCommitLogPosition(long pos) {
        this.commitLogPosition.set(pos);
    }
}
