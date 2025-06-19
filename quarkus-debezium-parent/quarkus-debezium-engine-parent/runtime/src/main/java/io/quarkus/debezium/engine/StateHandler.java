/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine;

import io.debezium.engine.DebeziumEngine;
import io.debezium.runtime.Debezium;
import io.debezium.runtime.DebeziumStatus;

public interface StateHandler {
    DebeziumEngine.ConnectorCallback connectorCallback();

    DebeziumEngine.CompletionCallback completionCallback();

    DebeziumStatus get();

    void setDebeziumEngine(Debezium engine);
}
