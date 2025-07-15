/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.heartbeat;

import java.util.Map;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Event;
import jakarta.inject.Inject;

import io.debezium.heartbeat.Heartbeat;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.runtime.Debezium;
import io.debezium.runtime.events.DebeziumHeartbeat;

@ApplicationScoped
public class QuarkusHeartbeatEmitter implements Heartbeat {

    private final Debezium debezium;
    private final Event<DebeziumHeartbeat> heartBeat;

    @Inject
    public QuarkusHeartbeatEmitter(Debezium debezium, Event<DebeziumHeartbeat> heartBeat) {
        this.debezium = debezium;
        this.heartBeat = heartBeat;
    }

    @Override
    public void emit(Map<String, ?> partition, OffsetContext offset) {
        heartBeat.fire(new DebeziumHeartbeat(
                debezium.connector(),
                debezium.status(),
                partition,
                offset));
    }

    @Override
    public boolean isEnabled() {
        return true;
    }
}
