/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.spi;

public interface ChangeEventSource {

    public interface ChangeEventSourceContext {

        /**
         * Whether this source is running or has been requested to stop.
         */
        boolean isRunning();
    }
}
