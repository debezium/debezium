/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.spi;

import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;
import io.debezium.pipeline.spi.SnapshotResult;

/**
 * A change event source that emits events for taking a consistent snapshot of the captured tables, which may include
 * schema and data information.
 *
 * @author Gunnar Morling
 */
public interface SnapshotChangeEventSource<P extends Partition, O extends OffsetContext> extends ChangeEventSource {

    /**
     * Executes this source. Implementations should regularly check via the given context if they should stop. If that's
     * the case, they should abort their processing and perform any clean-up needed, such as rolling back pending
     * transactions, releasing locks etc.
     *
     * @param context
     *            contextual information for this source's execution
     * @param partition
     *            the source partition from which the snapshot should be taken
     * @param previousOffset
     *            previous offset restored from Kafka
     * @return an indicator to the position at which the snapshot was taken
     * @throws InterruptedException
     *             in case the snapshot was aborted before completion
     */
    SnapshotResult<O> execute(ChangeEventSourceContext context, P partition, O previousOffset) throws InterruptedException;
}
