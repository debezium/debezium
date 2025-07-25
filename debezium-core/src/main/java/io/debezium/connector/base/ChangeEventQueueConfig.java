/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.base;

import java.time.Duration;
import java.util.function.Supplier;

import io.debezium.annotation.SingleThreadAccess;
import io.debezium.util.LoggingContext;

public class ChangeEventQueueConfig {

    private final Duration pollInterval;
    private final int maxBatchSize;
    private final int maxQueueSize;
    private final long maxQueueSizeInBytes;

    private final Supplier<LoggingContext.PreviousContext> loggingContextSupplier;
    @SingleThreadAccess("producer thread")
    private boolean buffering;

    private ChangeEventQueueConfig(Duration pollInterval, int maxQueueSize, int maxBatchSize, Supplier<LoggingContext.PreviousContext> loggingContextSupplier,
                                   long maxQueueSizeInBytes, boolean buffering) {
        this.pollInterval = pollInterval;
        this.maxBatchSize = maxBatchSize;
        this.maxQueueSize = maxQueueSize;
        this.loggingContextSupplier = loggingContextSupplier;
        this.buffering = buffering;
        this.maxQueueSizeInBytes = maxQueueSizeInBytes;
    }

    public Supplier<LoggingContext.PreviousContext> getLoggingContextSupplier() {
        return loggingContextSupplier;
    }

    public boolean isBuffering() {
        return buffering;
    }

    public Duration getPollInterval() {
        return pollInterval;
    }

    public int getMaxBatchSize() {
        return maxBatchSize;
    }

    public int getMaxQueueSize() {
        return maxQueueSize;
    }

    public long getMaxQueueSizeInBytes() {
        return maxQueueSizeInBytes;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private Duration pollInterval;
        private int maxQueueSize;
        private int maxBatchSize;
        private Supplier<LoggingContext.PreviousContext> loggingContextSupplier;
        private long maxQueueSizeInBytes;
        private boolean buffering;

        public Builder pollInterval(Duration pollInterval) {
            this.pollInterval = pollInterval;
            return this;
        }

        public Builder maxQueueSize(int maxQueueSize) {
            this.maxQueueSize = maxQueueSize;
            return this;
        }

        public Builder maxBatchSize(int maxBatchSize) {
            this.maxBatchSize = maxBatchSize;
            return this;
        }

        public Builder loggingContextSupplier(Supplier<LoggingContext.PreviousContext> loggingContextSupplier) {
            this.loggingContextSupplier = loggingContextSupplier;
            return this;
        }

        public Builder buffering() {
            this.buffering = true;
            return this;
        }

        public Builder maxQueueSizeInBytes(long maxQueueSizeInBytes) {
            this.maxQueueSizeInBytes = maxQueueSizeInBytes;
            return this;
        }

        public ChangeEventQueueConfig build() {
            return new ChangeEventQueueConfig(pollInterval, maxQueueSize, maxBatchSize, loggingContextSupplier, maxQueueSizeInBytes, buffering);
        }
    }

}
