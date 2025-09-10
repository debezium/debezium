/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine;

import io.debezium.engine.DebeziumEngine.CompletionCallback;
import io.debezium.engine.DebeziumEngine.ConnectorCallback;
import io.debezium.runtime.CaptureGroup;
import io.debezium.runtime.Debezium;
import io.debezium.runtime.DebeziumStatus;

public interface StateHandler {
    ConnectorCallback connectorCallback(CaptureGroup captureGroup, Debezium engine);

    CompletionCallback completionCallback(CaptureGroup captureGroup, Debezium engine);

    DebeziumStatus get(CaptureGroup captureGroup);

}
