/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded.async;

import java.util.function.Consumer;

public class ShutdownConsumer<R> implements Consumer<R> {

    private final ShutdownHandler<R> before;
    private final ShutdownHandler<R> after;
    private final Consumer<R> consumer;
    private final Watcher watcher;

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
