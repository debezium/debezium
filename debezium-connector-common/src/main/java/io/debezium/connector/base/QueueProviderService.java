/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.base;

import io.debezium.annotation.Immutable;
import io.debezium.annotation.ThreadSafe;
import io.debezium.service.Service;

/**
 * Service that provides the resolved {@link QueueProvider} instance for buffering change events.
 *
 * @author Chris Cranford
 */
@ThreadSafe
public class QueueProviderService implements Service {

    @Immutable
    private final QueueProvider<?> queueProvider;

    public QueueProviderService(QueueProvider<?> queueProvider) {
        this.queueProvider = queueProvider;
    }

    @SuppressWarnings("unchecked")
    public <T> QueueProvider<T> getQueueProvider() {
        return (QueueProvider<T>) queueProvider;
    }
}