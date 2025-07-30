/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine.capture;

public interface CapturingObjectInvoker extends CapturingInvoker<Object> {
    /**
     *
     * @return the destination that triggers the handler
     */
    String destination();

    @Override
    void capture(Object event);
}
