/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.outbox.quarkus.internal;

import io.quarkus.arc.Arc;
import io.quarkus.runtime.annotations.Recorder;

/**
 * Recorder that configures the {@link EventDispatcher} at runtime.
 *
 * @author Chris Cranford
 */
@Recorder
public class DebeziumOutboxRecorder {
    public void configureRuntimeProperties(DebeziumOutboxRuntimeConfig outboxRuntimeConfig) {
        EventDispatcher dispatcher = Arc.container().instance(EventDispatcher.class).get();
        dispatcher.setOutboxRuntimeProperties(outboxRuntimeConfig);
    }
}
