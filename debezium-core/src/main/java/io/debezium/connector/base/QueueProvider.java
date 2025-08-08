/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.base;

import io.debezium.spi.common.Configurable;

public interface QueueProvider<T> extends Configurable {

    void enqueue(T event) throws InterruptedException;

    T poll() throws InterruptedException;

    int size();

    void close();

    boolean isClosed();
}