/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.outbox.quarkus;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageCodec;

public class DebeziumCustomCodec implements MessageCodec<ExportedEvent<?, ?>, ExportedEvent<?, ?>> {
    private final String name;

    public DebeziumCustomCodec() {
        this.name = "DebeziumCustomCodec";
    }

    public DebeziumCustomCodec(String name) {
        this.name = name;
    }

    @Override
    public void encodeToWire(Buffer buffer, ExportedEvent<?, ?> exportedEvent) {
        throw new UnsupportedOperationException("LocalEventBusCodec cannot only be used for local delivery");
    }

    @Override
    public ExportedEvent<?, ?> decodeFromWire(int i, Buffer buffer) {
        throw new UnsupportedOperationException("LocalEventBusCodec cannot only be used for local delivery");
    }

    @Override
    public ExportedEvent<?, ?> transform(ExportedEvent<?, ?> exportedEvent) {
        return exportedEvent;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public byte systemCodecID() {
        return -1;
    }

}
