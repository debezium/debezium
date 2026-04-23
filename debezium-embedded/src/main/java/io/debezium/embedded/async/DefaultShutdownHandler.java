/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded.async;

import io.debezium.embedded.EmbeddedEngineChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.DebeziumEngine.ShutdownStrategy;

public class DefaultShutdownHandler<R> implements ShutdownHandler<R> {
    private final ShutdownStrategy<R> shutdownStrategy;
    private final Runnable shutdown;
    private final DebeziumEngine.RecordCommitter committer;

    private DefaultShutdownHandler(ShutdownStrategy<R> shutdownStrategy, Runnable shutdown, DebeziumEngine.RecordCommitter committer) {
        this.shutdownStrategy = shutdownStrategy;
        this.shutdown = shutdown;
        this.committer = committer;
    }

    @Override
    public void evaluate(R record) {
        if (shutdownStrategy.test(record)) {
            try {
                if (record instanceof EmbeddedEngineChangeEvent<?, ?, ?>) {
                    committer.markProcessed(((EmbeddedEngineChangeEvent<?, ?, ?>) record).sourceRecord());
                    committer.markBatchFinished();
                }
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            shutdown.run();
        }
    }

    public static <R> ShutdownHandler<R> create(ShutdownStrategy<R> shutdownStrategy, Runnable shutdown, DebeziumEngine.RecordCommitter committer) {
        if (shutdownStrategy == null) {
            return ignore -> {
            };
        }

        return new DefaultShutdownHandler<>(shutdownStrategy, shutdown, committer);
    }
}
