/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.engine.source;

import io.debezium.embedded.async.AsyncEmbeddedEngine;
import io.debezium.spi.storage.OffsetStorageReader;
import io.debezium.spi.storage.OffsetStorageWriter;
import io.debezium.spi.storage.OffsetStore;

/**
 * Implementation of {@link DebeziumSourceConnectorContext} which holds references to objects which spans whole connector life-cycle.
 *
 * @author vjuranek
 */
public class EngineSourceConnectorContext implements DebeziumSourceConnectorContext {

    private final AsyncEmbeddedEngine engine;
    private final OffsetStore offsetStore;
    private final OffsetStorageReader offsetReader;
    private final OffsetStorageWriter offsetWriter;

    public EngineSourceConnectorContext(final AsyncEmbeddedEngine engine, final OffsetStore offsetStore, final OffsetStorageReader offsetReader,
                                        final OffsetStorageWriter offsetWriter) {
        this.engine = engine;
        this.offsetStore = offsetStore;
        this.offsetReader = offsetReader;
        this.offsetWriter = offsetWriter;
    }

    @Override
    public OffsetStore offsetStore() {
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
}
