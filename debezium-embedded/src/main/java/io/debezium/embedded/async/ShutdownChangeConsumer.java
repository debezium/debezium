/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded.async;

import java.util.List;
import java.util.function.BiFunction;

import io.debezium.engine.DebeziumEngine;

public class ShutdownChangeConsumer<R> implements DebeziumEngine.ChangeConsumer<R> {

    private final ShutdownHandler<R> before;
    private final ShutdownHandler<R> after;
    private final DebeziumEngine.ChangeConsumer<R> consumer;

    public ShutdownChangeConsumer(ShutdownHandler<R> before,
                                  ShutdownHandler<R> after,
                                  DebeziumEngine.ChangeConsumer<R> consumer) {
        this.before = before;
        this.after = after;
        this.consumer = consumer;
    }

    @Override
    public void handleBatch(List<R> records, DebeziumEngine.RecordCommitter<R> committer) throws InterruptedException {
        records.forEach(before::evaluate);
        consumer.handleBatch(records, committer);
        records.forEach(after::evaluate);
    }

    public static <R> BiFunction<DebeziumEngine.ChangeConsumer<R>, DebeziumEngine.RecordCommitter, DebeziumEngine.ChangeConsumer<R>> create(DebeziumShutdown<R> shutdown,
                                                                                                                                            Runnable runner) {
        return (consumer, committer) -> {
            if (shutdown == null) {
                return consumer;
            }

            return new ShutdownChangeConsumer<>(
                    DefaultShutdownHandler.create(shutdown.before(), runner, committer),
                    DefaultShutdownHandler.create(shutdown.after(), runner, committer),
                    consumer);
        };
    }
}
