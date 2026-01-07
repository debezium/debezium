/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.engine.source;

import java.util.Map;

import org.apache.kafka.common.metrics.PluginMetrics;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.apache.kafka.connect.storage.OffsetStorageWriter;

import io.debezium.embedded.Transformations;
import io.debezium.engine.spi.OffsetCommitPolicy;

/**
 * Implementation of {@link DebeziumSourceTaskContext} which holds references to objects which spans whole task life-cycle.
 * At the same time implements also Kafka Connect {@link SourceTaskContext} as current implementation of
 * {@link DebeziumSourceTask} only wraps Kafka Connect {@link org.apache.kafka.connect.source.SourceTask}.
 *
 * @author vjuranek
 */
public class EngineSourceTaskContext implements DebeziumSourceTaskContext, SourceTaskContext {

    private final Map<String, String> config;
    private final OffsetStorageReader offsetReader;
    private final OffsetStorageWriter offsetWriter;
    private final OffsetCommitPolicy offsetCommitPolicy;
    private final io.debezium.util.Clock clock;
    private final Transformations transformations;

    public EngineSourceTaskContext(
                                   final Map<String, String> config,
                                   final OffsetStorageReader offsetReader,
                                   final OffsetStorageWriter offsetWriter,
                                   final OffsetCommitPolicy offsetCommitPolicy,
                                   final io.debezium.util.Clock clock,
                                   final Transformations transformations) {
        this.config = config;
        this.offsetReader = offsetReader;
        this.offsetWriter = offsetWriter;
        this.offsetCommitPolicy = offsetCommitPolicy;
        this.clock = clock;
        this.transformations = transformations;
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
    public Transformations transformations() {
        return transformations;
    }

    @Override
    public Map<String, String> configs() {
        // we don't support config changes on the fly yet
        return null;
    }

    @Override
    public PluginMetrics pluginMetrics() {
        // we don't support metrics yet
        return null;
    }
}
