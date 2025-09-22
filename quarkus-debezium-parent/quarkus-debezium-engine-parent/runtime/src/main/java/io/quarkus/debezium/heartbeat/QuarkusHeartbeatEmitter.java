/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.heartbeat;

import java.util.List;
import java.util.Map;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Event;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.heartbeat.Heartbeat;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.runtime.DebeziumConnectorRegistry;
import io.debezium.runtime.events.DebeziumHeartbeat;
import io.debezium.runtime.events.Engine;
import io.quarkus.debezium.engine.DebeziumThreadHandler;

@ApplicationScoped
public class QuarkusHeartbeatEmitter implements Heartbeat {

    private static final Logger LOGGER = LoggerFactory.getLogger(QuarkusHeartbeatEmitter.class.getName());

    private final List<DebeziumConnectorRegistry> registries;
    private final Event<DebeziumHeartbeat> heartbeat;

    @Inject
    public QuarkusHeartbeatEmitter(Instance<DebeziumConnectorRegistry> registries, Event<DebeziumHeartbeat> heartbeat) {
        this.registries = registries
                .stream()
                .toList();
        this.heartbeat = heartbeat;
    }

    public QuarkusHeartbeatEmitter(List<DebeziumConnectorRegistry> registries, Event<DebeziumHeartbeat> heartbeat) {
        this.registries = registries;
        this.heartbeat = heartbeat;
    }

    @Override
    public void emit(Map<String, ?> partition, OffsetContext offset) {
        heartbeat
                .select(Engine.Literal.of(DebeziumThreadHandler.context().manifest().id()))
                .fire(new DebeziumHeartbeat(
                        this.registries.getFirst().engines().getFirst().connector(),
                        this.registries.getFirst().engines().getFirst().status(),
                        partition,
                        offset.getOffset()));
    }

    @Override
    public boolean isEnabled() {
        return true;
    }
}
