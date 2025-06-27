/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb;

import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.pipeline.source.spi.ChangeEventSourceFactory;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;

/**
 * Factory to provide the changefeed polling source for CockroachDB.
 *
 * @author Virag Tripathi
 */
public class CockroachDBChangefeedSourceFactory implements ChangeEventSourceFactory<CockroachDBPartition, CockroachDBOffsetContext> {

    private final CockroachDBConnectorConfig config;

    public CockroachDBChangefeedSourceFactory(CockroachDBConnectorConfig config) {
        this.config = config;
    }

    @Override
    public SnapshotChangeEventSource<CockroachDBPartition, CockroachDBOffsetContext> getSnapshotChangeEventSource() {
        return new SnapshotChangeEventSource<>() {
            @Override
            public SnapshotChangeEventSource.ChangeEventSourceResult execute(
                                                                             SnapshotChangeEventSource.SnapshotContext<CockroachDBOffsetContext> ctx,
                                                                             CockroachDBPartition partition,
                                                                             CockroachDBOffsetContext offsetContext,
                                                                             io.debezium.pipeline.source.spi.EventDispatcher<CockroachDBPartition, ?> dispatcher)
                    throws InterruptedException {
                return SnapshotChangeEventSource.ChangeEventSourceResult.skipped();
            }
        };
    }

    @Override
    public ChangeEventSource<CockroachDBPartition, CockroachDBOffsetContext> getStreamingChangeEventSource() {
        return new CockroachDBChangefeedClient(config);
    }
}
