/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.engine.source;

import io.debezium.common.annotation.Incubating;

/**
 * Identifies a connector task within the embedded engine.
 *
 * @author Debezium Authors
 */
@Incubating
public record EngineTaskId(String connectorName, int taskId) {

    @Override
    public String toString() {
        return connectorName + "-" + taskId;
    }
}
