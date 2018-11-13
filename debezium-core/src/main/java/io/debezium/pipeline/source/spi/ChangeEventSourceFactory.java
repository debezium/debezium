/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.spi;

import io.debezium.pipeline.spi.OffsetContext;

/**
 * A factory for creating {@link ChangeEventSource}s specific to one database.
 *
 * @author Gunnar Morling
 */
public interface ChangeEventSourceFactory {

    /**
     * Returns a snapshot change event source that may emit change events for schema and/or data changes. Depending on
     * the snapshot mode, a given source may decide to do nothing at all if a previous offset is given. In this case it
     * should return that given offset context from its
     * {@link StreamingChangeEventSource#execute(io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext)}
     * method.
     *
     * @param offsetContext
     *            A context representing a restored offset from an earlier run of this connector. May be {@code null}.
     * @param snapshotProgressListener
     *            A listener called for changes in the state of snapshot. May be {@code null}.
     *
     * @return A snapshot change event source
     */
    SnapshotChangeEventSource getSnapshotChangeEventSource(OffsetContext offsetContext, SnapshotProgressListener snapshotProgressListener);

    /**
     * Returns a streaming change event source that starts streaming at the given offset.
     */
    StreamingChangeEventSource getStreamingChangeEventSource(OffsetContext offsetContext);
}
