/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded.async;

import java.util.function.Consumer;

/**
 * A {@link Consumer} decorator that brackets a user-supplied consumer with shutdown evaluation.
 *
 * <p>For each record, the {@code before} handler is evaluated first. If the engine is still
 * consuming (checked via {@link Watcher}), the record is forwarded to the wrapped consumer.
 * The {@code after} handler is evaluated last, regardless of whether the consumer ran.
 *
 * <p>Either handler may be a no-op (see {@link DefaultShutdownHandler#create}) when no strategy
 * is configured for that position.
 *
 * @param <R> the record type produced by the engine
 * @see ShutdownHandler
 * @see Watcher
 */
public class ShutdownConsumer<R> implements Consumer<R> {

    private final ShutdownHandler<R> before;
    private final ShutdownHandler<R> after;
    private final Consumer<R> consumer;
    private final Watcher watcher;

    /**
     * Creates a new {@code ShutdownConsumer}.
     *
     * @param before   the handler evaluated before the consumer; never null
     * @param after    the handler evaluated after the consumer; never null
     * @param consumer the user-supplied record consumer; never null
     * @param watcher  provides access to the engine's current consumption state; never null
     */
    public ShutdownConsumer(ShutdownHandler<R> before,
                            ShutdownHandler<R> after,
                            Consumer<R> consumer, Watcher watcher) {
        this.before = before;
        this.after = after;
        this.consumer = consumer;
        this.watcher = watcher;
    }

    @Override
    public void accept(R record) {
        before.evaluate(record);
        if (watcher.engine().isConsuming()) {
            consumer.accept(record);
        }
        after.evaluate(record);
    }

}
