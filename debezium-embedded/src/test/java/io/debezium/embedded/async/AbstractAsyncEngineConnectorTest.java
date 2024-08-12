/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded.async;

import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.embedded.TestingDebeziumEngine;
import io.debezium.engine.DebeziumEngine;
import io.debezium.pipeline.signal.SignalRecord;

/**
 * Base class for testing connectors using {@link AsyncEmbeddedEngine}.
 *
 * @author vjuranek
 */
public class AbstractAsyncEngineConnectorTest extends AbstractConnectorTest {

    protected DebeziumEngine.Signaler<SignalRecord> signaler;

    @Override
    protected DebeziumEngine.Builder createEngineBuilder() {
        this.signaler = new AsyncEngineSignaler();

        return new AsyncEmbeddedEngine.AsyncEngineBuilder()
                .using(signaler);
    }

    @Override
    protected TestingDebeziumEngine createEngine(DebeziumEngine.Builder builder) {
        return new TestingAsyncEmbeddedEngine((AsyncEmbeddedEngine) builder.build());
    }
}
