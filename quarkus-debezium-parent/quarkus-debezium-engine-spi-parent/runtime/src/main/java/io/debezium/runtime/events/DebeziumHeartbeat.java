/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.runtime.events;

import java.util.Map;

import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.runtime.Connector;
import io.debezium.runtime.DebeziumStatus;

public record DebeziumHeartbeat(Connector connector, DebeziumStatus status,
        Map<String, ?> partition, OffsetContext offset) {
}
