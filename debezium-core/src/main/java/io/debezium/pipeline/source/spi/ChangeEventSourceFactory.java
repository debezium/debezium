/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.spi;

import java.util.Optional;

import io.debezium.bean.spi.BeanRegistry;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotChangeEventSource;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;
import io.debezium.spi.schema.DataCollectionId;

/**
 * A factory for creating {@link ChangeEventSource}s specific to one database.
 *
 * @author Gunnar Morling
 */
public interface ChangeEventSourceFactory<P extends Partition, O extends OffsetContext> {

    /**
     * Returns a snapshot change event source that may emit change events for schema and/or data changes. Depending on
     * the snapshot mode, a given source may decide to do nothing at all if a previous offset is given. In this case it
     * should return that given offset context from its
     * {@link StreamingChangeEventSource#execute(ChangeEventSource.ChangeEventSourceContext, Partition, io.debezium.pipeline.spi.OffsetContext)}
     * method.
     *
     * @param beanRegistry
     * @param snapshotProgressListener
     *            A listener called for changes in the state of snapshot. May be {@code null}.
     *
     * @return A snapshot change event source
     */
    SnapshotChangeEventSource<P, O> getSnapshotChangeEventSource(SnapshotProgressListener<P> snapshotProgressListener, NotificationService<P, O> notificationService,
                                                                 BeanRegistry beanRegistry);

    /**
     * Returns a streaming change event source that starts streaming at the given offset.
     */
    StreamingChangeEventSource<P, O> getStreamingChangeEventSource();

    /**
     * Returns and incremental snapshot change event source that can run in parallel with streaming
     * and read and send data collection content in chunk.
     *
     * @param offsetContext
     *            A context representing a restored offset from an earlier run of this connector. May be {@code null}.
     * @param snapshotProgressListener
     *            A listener called for changes in the state of snapshot. May be {@code null}.
     *
     * @return An incremental snapshot change event source
     */
    default Optional<IncrementalSnapshotChangeEventSource<P, ? extends DataCollectionId>> getIncrementalSnapshotChangeEventSource(O offsetContext,
                                                                                                                                  SnapshotProgressListener<P> snapshotProgressListener,
                                                                                                                                  DataChangeEventListener<P> dataChangeEventListener,
                                                                                                                                  NotificationService<P, O> notificationService) {
        return Optional.empty();
    }
}
