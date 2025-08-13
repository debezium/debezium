/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine.capture;

import io.debezium.runtime.Capturing;

/**
 * Interface used to generate Invokers that are assigned to methods annotated with {@link Capturing}
 * that uses a mapped Object as event
 */
public interface CapturingObjectInvoker extends CapturingInvoker<Object> {

    @Override
    void capture(Object event);
}
