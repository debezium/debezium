/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.runtime;

/**
 * Contextual information associated to a running {@link Debezium} engine
 */
public interface DebeziumContext {

    /**
     * @return the {@link EngineManifest} for the running {@link Debezium} engine
     */
    EngineManifest manifest();
}
