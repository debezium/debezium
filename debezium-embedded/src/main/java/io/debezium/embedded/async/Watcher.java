/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded.async;

import io.debezium.common.annotation.Incubating;
import io.debezium.engine.DebeziumEngine;

/**
 * Provides runtime observability into the state of a {@link DebeziumEngine}.
 * This interface allows internal components to monitor whether the engine is
 * actively processing records.
 *
 * @since 3.6.0
 */
@Incubating
public interface Watcher {

    /**
     * Provides access to engine-level state monitoring.
     *
     * @return the engine watcher; never null
     */
    EngineWatcher engine();

    /**
     * Monitors the consumption state of the engine.
     */
    interface EngineWatcher {

        /**
         * Checks whether the engine is currently in the consuming/polling state,
         * actively reading and processing records from the source.
         *
         * @return {@code true} if the engine is actively consuming records from the
         *         source database; {@code false} if the engine is in any other state
         *         (initializing, starting tasks, stopping, stopped, etc.)
         */
        boolean isPolling();
    }

}
