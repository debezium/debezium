/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine.capture;

import io.debezium.runtime.Capturing;
import io.debezium.runtime.CapturingEvent;

/**
 *
 * Invoker assigned to any annotated class with method {@link Capturing}
 *
 */
public interface CapturingInvoker<T> {

    /**
     *
     * @return the destination that triggers the handler
     */
    String destination();

    /**
     * @param event captured by Debezium
     */
    void capture(T event);

    /**
     *
     * @return the capturing group assigned
     */
    String group();

    static String generateKey(CapturingInvoker invoker) {
        return invoker.group() + "_" + invoker.destination();
    }

    static String getKey(CapturingEvent event) {
        return event.group() + "_" + event.destination();
    }

    static String getAllDestinations(CapturingEvent event) {
        return event.group() + "_" + Capturing.ALL;
    }
}
