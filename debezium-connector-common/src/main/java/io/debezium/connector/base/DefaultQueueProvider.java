/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.base;

import java.util.ArrayDeque;
import java.util.Map;
import java.util.Queue;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.pipeline.Sizeable;

/**
 * Default implementation of {@link QueueProvider} using an {@link ArrayDeque}.
 *
 * @param <T> the type of elements in the queue, must implement {@link Sizeable}
 */
public class DefaultQueueProvider<T extends Sizeable> implements QueueProvider<T> {

    private Queue<T> queue;

    @Override
    public String name() {
        return "memory";
    }

    @Override
    public void configure(Map<String, ?> properties) {
        final Configuration config = Configuration.from(properties);
        this.queue = new ArrayDeque<>(config.getInteger(CommonConnectorConfig.MAX_QUEUE_SIZE));
    }

    @Override
    public void enqueue(T record) throws InterruptedException {
        queue.add(record);
    }

    @Override
    public T poll() throws InterruptedException {
        return queue.poll();
    }

    @Override
    public int size() {
        return queue.size();
    }
}
