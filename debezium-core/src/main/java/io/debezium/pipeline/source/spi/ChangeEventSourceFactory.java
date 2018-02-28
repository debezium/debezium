/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.spi;

import io.debezium.pipeline.spi.OffsetContext;

/**
 * A factory for creating @l# {@link ChangeEventSource}s specific to one database.
 *
 * @author Gunnar Morling
 */
public interface ChangeEventSourceFactory {

    SnapshotChangeEventSource getSnapshotChangeEventSource();

    StreamingChangeEventSource getStreamingChangeEventSource(OffsetContext offsetContext);
}
