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
public interface CapturingInvoker<T> {

    /**
     * @param event event captured by Debezium
     */
    void capture(T event);
}
