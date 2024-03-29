/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded;

import java.io.IOException;
import java.util.function.Consumer;

/**
 * Implementation of {@link TestingDebeziumEngine} for {@link EmbeddedEngine}.
 */
public class TestingEmbeddedEngine implements TestingDebeziumEngine {

    private final EmbeddedEngine engine;

    public TestingEmbeddedEngine(EmbeddedEngine engine) {
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
