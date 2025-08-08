/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.base;

import java.util.ArrayDeque;
import java.util.Map;
import java.util.Queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.pipeline.Sizeable;

public class DefaultQueueProvider<T extends Sizeable> implements QueueProvider<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultQueueProvider.class);

    private Queue<T> queue;
    private volatile boolean closed = false;

    public DefaultQueueProvider(int maxQueueSize) {
        this.queue = new ArrayDeque<>(maxQueueSize);
    }

    @Override
    public void configure(Map<String, ?> properties) {
        this.queue = new ArrayDeque<>();
    }

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

    @Override
    public void close() {
        closed = true;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }
}
