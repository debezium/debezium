/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.engine.source;

import java.util.Map;

import io.debezium.embedded.SourceRecordTransformations;
import io.debezium.engine.spi.OffsetCommitPolicy;
import io.debezium.spi.storage.OffsetStorageReader;
import io.debezium.spi.storage.OffsetStorageWriter;

/**
 * Implementation of {@link DebeziumSourceTaskContext} which holds references to objects which spans whole task life-cycle.
 *
 * @author vjuranek
 */
public class EngineSourceTaskContext implements DebeziumSourceTaskContext {

    private final Map<String, String> config;
    private final OffsetStorageReader offsetReader;
    private final OffsetStorageWriter offsetWriter;
    private final OffsetCommitPolicy offsetCommitPolicy;
    private final io.debezium.util.Clock clock;
    private final SourceRecordTransformations transformations;
    private final EngineTaskId connectorTaskId;

    public EngineSourceTaskContext(
                                   final Map<String, String> config,
                                   final OffsetStorageReader offsetReader,
                                   final OffsetStorageWriter offsetWriter,
                                   final OffsetCommitPolicy offsetCommitPolicy,
                                   final io.debezium.util.Clock clock,
                                   final SourceRecordTransformations transformations,
                                   final EngineTaskId connectorTaskId) {
        this.config = config;
        this.offsetReader = offsetReader;
        this.offsetWriter = offsetWriter;
        this.offsetCommitPolicy = offsetCommitPolicy;
        this.clock = clock;
        this.transformations = transformations;
        this.connectorTaskId = connectorTaskId;
    }

    @Override
    public Map<String, String> config() {
        return config;
    }

    @Override
    public OffsetStorageReader offsetStorageReader() {
        return offsetReader;
    }

    @Override
    public OffsetStorageWriter offsetStorageWriter() {
        return offsetWriter;
    }

    @Override
    public OffsetCommitPolicy offsetCommitPolicy() {
        return offsetCommitPolicy;
    }

    @Override
    public io.debezium.util.Clock clock() {
        return clock;
    }

    @Override
    public SourceRecordTransformations transformations() {
        return transformations;
    }

    @Override
    public EngineTaskId connectorTaskId() {
        return connectorTaskId;
    }
}
