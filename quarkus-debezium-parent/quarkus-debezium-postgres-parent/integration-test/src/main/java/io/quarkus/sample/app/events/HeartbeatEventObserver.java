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

@ApplicationScoped
public class HeartbeatEventObserver {

    private final AtomicReference<DebeziumHeartbeat> reference = new AtomicReference<>();

    public void observe(@Observes DebeziumHeartbeat heartbeat) {
        reference.set(heartbeat);
    }

    public DebeziumHeartbeat get() {
        return reference.get();
    }
}
