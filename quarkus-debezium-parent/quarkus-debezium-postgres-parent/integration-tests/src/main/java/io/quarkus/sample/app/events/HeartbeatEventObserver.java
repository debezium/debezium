/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.sample.app.events;

import java.util.concurrent.atomic.AtomicReference;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;

import io.debezium.runtime.events.DebeziumHeartbeat;
import io.debezium.runtime.events.Engine;

@ApplicationScoped
public class HeartbeatEventObserver {

    private final AtomicReference<DebeziumHeartbeat> defaultHeartbeats = new AtomicReference<>();
    private final AtomicReference<DebeziumHeartbeat> alternativeHeartbeats = new AtomicReference<>();

    public void observeDefaultHeartbeat(@Observes @Engine("default") DebeziumHeartbeat heartbeat) {
        defaultHeartbeats.set(heartbeat);
    }

    public void observeAlternativeHeartbeat(@Observes @Engine("alternative") DebeziumHeartbeat heartbeat) {
        alternativeHeartbeats.set(heartbeat);
    }

    public DebeziumHeartbeat get(String engine) {
        if (engine.equals("default")) {
            return defaultHeartbeats.get();
        }

        return alternativeHeartbeats.get();
    }
}
