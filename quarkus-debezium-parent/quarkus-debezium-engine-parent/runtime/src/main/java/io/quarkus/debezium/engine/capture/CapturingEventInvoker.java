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
 * Invoker assigned to any annotated class with method {@link Capturing} and events {@link CapturingEvent} or serialized event
 *
 */
public interface CapturingEventInvoker extends FilteredCapturingInvoker<CapturingEvent<Object>> {

    @Override
    void capture(CapturingEvent<Object> event);
}
