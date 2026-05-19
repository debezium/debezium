/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded.async;

import java.util.Map;
import java.util.Optional;

import io.debezium.embedded.EmbeddedEngineChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.DebeziumEngine.ShutdownStrategy;

public class DefaultShutdownHandler<R> implements ShutdownHandler<R> {
    private final ShutdownStrategy<DebeziumEngine.ShutdownContext<R>> shutdownStrategy;
    private final Runnable shutdown;
    private final DebeziumEngine.RecordCommitter committer;
    private final Map<String, String> configuration;

    private DefaultShutdownHandler(ShutdownStrategy<DebeziumEngine.ShutdownContext<R>> shutdownStrategy, Runnable shutdown, DebeziumEngine.RecordCommitter committer,
                                   Map<String, String> configuration) {
        this.shutdownStrategy = shutdownStrategy;
        this.shutdown = shutdown;
        this.committer = committer;
        this.configuration = configuration;
    }

    @Override
    public void evaluate(R record) {
        if (shutdownStrategy.test(createContext(record))) {
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

    private DebeziumEngine.ShutdownContext<R> createContext(R record) {
        return new DebeziumEngine.ShutdownContext<>() {
            @Override
            public Optional<String> configuration(String key) {
                return Optional.ofNullable(configuration.get(key));
            }

            @Override
            public R record() {
                return record;
            }
        };
    }

    public static <R> ShutdownHandler<R> create(ShutdownStrategy<DebeziumEngine.ShutdownContext<R>> shutdownStrategy, Runnable shutdown,
                                                DebeziumEngine.RecordCommitter committer,
                                                Map<String, String> configuration) {
        if (shutdownStrategy == null) {
            return ignore -> {
            };
        }

        return new DefaultShutdownHandler<>(shutdownStrategy, shutdown, committer, configuration);
    }
}
