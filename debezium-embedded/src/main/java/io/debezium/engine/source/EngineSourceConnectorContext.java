/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.engine.source;

import org.apache.kafka.common.metrics.PluginMetrics;
import org.apache.kafka.connect.source.SourceConnectorContext;
import org.apache.kafka.connect.storage.OffsetBackingStore;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.apache.kafka.connect.storage.OffsetStorageWriter;

import io.debezium.embedded.async.AsyncEmbeddedEngine;

/**
 * Implementation of {@link DebeziumSourceConnectorContext} which holds references to objects which spans whole connector life-cycle.
 * At the same time implements also Kafka Connect {@link SourceConnectorContext} as current implementation of
 * {@link DebeziumSourceConnector} only wraps Kafka Connect {@link org.apache.kafka.connect.source.SourceConnector}.
 *
 * @author vjuranek
 */
public class EngineSourceConnectorContext implements DebeziumSourceConnectorContext, SourceConnectorContext {

    private final AsyncEmbeddedEngine engine;
    private final OffsetBackingStore offsetStore;
    private final OffsetStorageReader offsetReader;
    private final OffsetStorageWriter offsetWriter;

    public EngineSourceConnectorContext(final AsyncEmbeddedEngine engine, final OffsetBackingStore offsetStore, final OffsetStorageReader offsetReader,
                                        final OffsetStorageWriter offsetWriter) {
        this.engine = engine;
        this.offsetStore = offsetStore;
        this.offsetReader = offsetReader;
        this.offsetWriter = offsetWriter;
    }

    @Override
    public OffsetBackingStore offsetStore() {
        return offsetStore;
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
    public void requestTaskReconfiguration() {
        // no-op, we don't support config changes on the fly yet
    }

    @Override
    public void raiseError(Exception e) {
        // no-op, only for Kafka compatibility
    }

    @Override
    public PluginMetrics pluginMetrics() {
        // no-op, only for Kafka compatibility
        return null;
    }
}
