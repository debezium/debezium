/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.cockroachdb;

import java.util.Optional;

import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.pipeline.source.spi.ChangeEventSourceFactory;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.OffsetContext;

/**
 * A factory that produces the change event sources used by the CockroachDB connector.
 * <p>
 * This includes the optional snapshot event source (currently not implemented),
 * and the streaming event source that continuously polls CockroachDB changefeeds
 * and emits records into Kafka.
 *
 * It also exposes offset context loader and source info struct maker
 * for use in the connector runtime.
 *
 * @author Virag Tripathi
 */
public class CockroachDBChangeEventSourceFactory
        implements ChangeEventSourceFactory<CockroachDBPartition, CockroachDBOffsetContext> {

    private final CockroachDBConnectorConfig config;
    private final CockroachDBSourceRecordMapper recordMapper;

    public CockroachDBChangeEventSourceFactory(
                                               CockroachDBConnectorConfig config,
                                               CockroachDBSourceRecordMapper recordMapper) {
        this.config = config;
        this.recordMapper = recordMapper;
    }

    @Override
    public SnapshotChangeEventSource<CockroachDBPartition, CockroachDBOffsetContext> getSnapshotChangeEventSource(
                                                                                                                  SnapshotProgressListener<CockroachDBPartition> snapshotProgressListener,
                                                                                                                  NotificationService<CockroachDBPartition, CockroachDBOffsetContext> notificationService) {
        // For now, no snapshot support for CockroachDB connector
        return null;
    }

    @Override
    public ChangeEventSource<CockroachDBPartition, CockroachDBOffsetContext> getStreamingChangeEventSource() {
        return new CockroachDBChangefeedClient(config, recordMapper);
    }

    @Override
    public OffsetContext.Loader<CockroachDBOffsetContext> getOffsetContextLoader() {
        return new CockroachDBOffsetContext.Loader(config);
    }

    @Override
    public Optional<SourceInfoStructMaker<CockroachDBOffsetContext>> getSourceInfoStructMaker() {
        return Optional.of(new CockroachDBSourceInfoStructMaker());
    }
}
