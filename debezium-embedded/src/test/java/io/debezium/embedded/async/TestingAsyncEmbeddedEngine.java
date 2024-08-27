/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded.async;

import java.io.IOException;
import java.util.function.Consumer;

import org.apache.kafka.connect.source.SourceTask;

import io.debezium.embedded.TestingDebeziumEngine;

/**
 * Implementation of {@link TestingDebeziumEngine} for {@link AsyncEngineConfig}.
 *
 * @author vjuranek
 */
public class TestingAsyncEmbeddedEngine<T> implements TestingDebeziumEngine<T> {
    private final AsyncEmbeddedEngine<T> engine;

    public TestingAsyncEmbeddedEngine(AsyncEmbeddedEngine<T> engine) {
        this.engine = engine;
    }

    @Override
    public void run() {
        engine.run();
    }

    @Override
    public void close() throws IOException {
        engine.close();
    }

    @Override
    public void runWithTask(Consumer<SourceTask> consumer) {
        engine.runWithTask(consumer);
    }

    @Override
    public Signaler getSignaler() {
        return engine.getSignaler();
    }
}
