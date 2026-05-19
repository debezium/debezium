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

/**
 * Default implementation of {@link ShutdownHandler} that delegates to a {@link io.debezium.engine.DebeziumEngine.ShutdownStrategy}.
 *
 * <p>For each record evaluated, the handler builds a {@link io.debezium.engine.DebeziumEngine.Shutdown.ShutdownContext}
 * and passes it to the strategy. When the strategy signals shutdown, the handler commits the triggering record's
 * source offset (if it is an {@link EmbeddedEngineChangeEvent}) and then invokes the engine's shutdown runnable.
 *
 * <p>Use the {@link #create} factory method to obtain instances; when no strategy is provided, a no-op handler
 * is returned so callers never need to guard against {@code null}.
 *
 * @param <R> the record type produced by the engine
 * @see ShutdownHandler
 * @see io.debezium.engine.DebeziumEngine.ShutdownStrategy
 */
public class DefaultShutdownHandler<R> implements ShutdownHandler<R> {
    private final ShutdownStrategy<DebeziumEngine.Shutdown.ShutdownContext<R>> shutdownStrategy;
    private final Runnable shutdown;
    private final DebeziumEngine.RecordCommitter committer;
    private final Map<String, String> configuration;

    private DefaultShutdownHandler(ShutdownStrategy<DebeziumEngine.Shutdown.ShutdownContext<R>> shutdownStrategy, Runnable shutdown,
                                   DebeziumEngine.RecordCommitter committer,
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

    private DebeziumEngine.Shutdown.ShutdownContext<R> createContext(R record) {
        return new DebeziumEngine.Shutdown.ShutdownContext<>() {
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

    /**
     * Creates a {@link ShutdownHandler} for the given strategy.
     *
     * <p>When {@code shutdownStrategy} is {@code null}, a no-op handler is returned so that callers
     * do not need to perform null checks before invoking {@link ShutdownHandler#evaluate(Object)}.
     *
     * @param shutdownStrategy the strategy to evaluate; may be null
     * @param shutdown the runnable invoked to stop the engine when the strategy signals shutdown; never null
     * @param committer the committer used to mark the triggering record as processed; never null
     * @param configuration the connector configuration map exposed through the shutdown context; never null
     * @param <R> the record type produced by the engine
     * @return a {@link ShutdownHandler}; never null
     */
    public static <R> ShutdownHandler<R> create(ShutdownStrategy<DebeziumEngine.Shutdown.ShutdownContext<R>> shutdownStrategy, Runnable shutdown,
                                                DebeziumEngine.RecordCommitter committer,
                                                Map<String, String> configuration) {
        if (shutdownStrategy == null) {
            return ignore -> {
            };
        }

        return new DefaultShutdownHandler<>(shutdownStrategy, shutdown, committer, configuration);
    }
}
