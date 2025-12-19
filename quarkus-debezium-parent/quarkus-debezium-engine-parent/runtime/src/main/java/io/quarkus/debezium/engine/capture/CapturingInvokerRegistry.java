/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine.capture;

import io.debezium.runtime.CapturingEvent;

/**
 * It should return an Invoker<T> based on the value of T
 * @param <T> the event
 */
public interface CapturingInvokerRegistry<T> {
    CapturingInvoker<T> get(CapturingEvent identifier);
}
