/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.quarkus.debezium.engine;

import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

import io.debezium.common.annotation.Incubating;
import io.debezium.engine.DebeziumEngine.Signaler;

/**
 * The Debezium engine abstraction in the Quarkus CDI
 * <p>
 * The Engine is submitted to an {@link Executor} or {@link ExecutorService} for execution by a single thread
 */
@Incubating
public interface Debezium {

    /**
     * @return engine's signaler, if it supports signaling
     * @throws UnsupportedOperationException if signaling is not supported by this engine
     */
    Signaler signaler();

    /**
     * @return engine's configuration
     */
    Map<String, String> configuration();

    /**
     * @return engine's status information
     */
    DebeziumManifest manifest();
}
