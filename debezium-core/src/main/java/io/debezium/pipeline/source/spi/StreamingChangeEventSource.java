/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.spi;

import java.util.Map;

/**
 * A change event source that emits events from a DB log, such as MySQL's binlog or similar.
 *
 * @author Gunnar Morling
 */
public interface StreamingChangeEventSource extends ChangeEventSource {

    /**
     * Executes this source. Implementations should regularly check via the given context if they should stop. If that's
     * the case, they should abort their processing and perform any clean-up needed, such as rolling back pending
     * transactions, releasing locks etc.
     *
     * @param context
     *            contextual information for this source's execution
     * @return an indicator to the position at which the snapshot was taken
     * @throws InterruptedException
     *             in case the snapshot was aborted before completion
     */
    void execute(ChangeEventSourceContext context) throws InterruptedException;

    /**
     * Commits the given offset with the source database. Used by some connectors
     * like Postgres and Oracle to indicate how far the source TX log can be
     * discarded.
     */
    default void commitOffset(Map<String, ?> offset) {
    }
}
