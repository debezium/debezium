/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.sink.batch;

import io.debezium.sink.DebeziumSinkRecord;
import io.debezium.sink.SinkConnectorConfig;

/**
 * A buffer of {@link DebeziumSinkRecord}
 *
 * @author rk3rn3r
 */
public class KeylessBuffer extends AbstractBuffer implements Buffer {

    public KeylessBuffer(SinkConnectorConfig connectorConfig) {
        super(connectorConfig);
    }
}
