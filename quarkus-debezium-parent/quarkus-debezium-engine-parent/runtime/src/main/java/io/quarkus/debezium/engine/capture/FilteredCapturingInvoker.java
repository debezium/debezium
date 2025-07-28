/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine.capture;

import io.debezium.runtime.Capturing;

/**
 *
 * Invoker assigned to any annotated class with method {@link Capturing}
 *
 */
public interface FilteredCapturingInvoker<T> extends CapturingInvoker<T> {

    /**
     *
     * @return the destination that triggers the handler
     */
    String destination();

    /**
     * @param event captured by Debezium
     */
    @Override
    void capture(T event);
}
