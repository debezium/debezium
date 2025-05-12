/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine;

import io.debezium.engine.DebeziumEngine;
import io.debezium.runtime.DebeziumManifest;

public interface ManifestHandler {
    DebeziumEngine.ConnectorCallback connectorCallback();

    DebeziumEngine.CompletionCallback completionCallback();

    DebeziumManifest get();
}
