/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded.async;

import java.io.IOException;
import java.util.function.Consumer;

import io.debezium.embedded.TestingDebeziumEngine;

/**
 * Implementation of {@link TestingDebeziumEngine} for {@link AsyncEngineConfig}.
 *
 * @author vjuranek
 */
public class TestingAsyncEmbeddedEngine implements TestingDebeziumEngine {
    private final AsyncEmbeddedEngine engine;

    public TestingAsyncEmbeddedEngine(AsyncEmbeddedEngine engine) {
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
    public void runWithTask(Consumer consumer) {
        engine.runWithTask(consumer);
    }
}
