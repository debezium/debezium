/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded.async;

import java.util.List;

import io.debezium.engine.DebeziumEngine;

/**
 * A {@link io.debezium.engine.DebeziumEngine.ChangeConsumer} decorator that brackets batch processing
 * with shutdown evaluation.
 *
 * <p>For each batch, the {@code before} handler is evaluated for every record first, then the wrapped
 * consumer processes the whole batch, and finally the {@code after} handler is evaluated for every record.
 * Unlike {@link ShutdownConsumer}, all records in the batch are always forwarded to the consumer regardless
 * of the engine's consumption state, because batch boundaries are managed by the committer.
 *
 * <p>Either handler may be a no-op (see {@link DefaultShutdownHandler#create}) when no strategy is
 * configured for that position.
 *
 * @param <R> the record type produced by the engine
 * @see ShutdownHandler
 * @see io.debezium.engine.DebeziumEngine.ChangeConsumer
 */
public class ShutdownChangeConsumer<R> implements DebeziumEngine.ChangeConsumer<R> {

    private final ShutdownHandler<R> before;
    private final ShutdownHandler<R> after;
    private final DebeziumEngine.ChangeConsumer<R> consumer;

    /**
     * Creates a new {@code ShutdownChangeConsumer}.
     *
     * @param before   the handler evaluated for each record before the batch is processed; never null
     * @param after    the handler evaluated for each record after the batch is processed; never null
     * @param consumer the user-supplied batch consumer; never null
     */
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

}
