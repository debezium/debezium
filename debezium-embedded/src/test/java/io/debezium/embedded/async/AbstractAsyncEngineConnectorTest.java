/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded.async;

import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.embedded.TestingDebeziumEngine;
import io.debezium.engine.DebeziumEngine;

/**
 * Base class for testing connectors using {@link AsyncEmbeddedEngine}.
 *
 * @author vjuranek
 */
public class AbstractAsyncEngineConnectorTest extends AbstractConnectorTest {

    @Override
    protected DebeziumEngine.Builder createEngineBuilder() {
        return new AsyncEmbeddedEngine.AsyncEngineBuilder();
    }

    @Override
    protected TestingDebeziumEngine createEngine(DebeziumEngine.Builder builder) {
        return new TestingAsyncEmbeddedEngine((AsyncEmbeddedEngine) builder.build());
    }

    protected DebeziumEngine.Signaler getSignaler() {
        return engine.getSignaler();
    }
}
