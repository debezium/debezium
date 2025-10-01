/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.sample.app.resources;

import jakarta.inject.Inject;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;

import io.debezium.runtime.events.DebeziumHeartbeat;
import io.quarkus.sample.app.events.HeartbeatEventObserver;

@Path("/heartbeat")
public class HeartbeatEventResource {
    private final HeartbeatEventObserver observer;

    @Inject
    public HeartbeatEventResource(HeartbeatEventObserver observer) {
        this.observer = observer;
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public DebeziumHeartbeat get(@QueryParam("engine") @DefaultValue("default") String engine) {
        return observer.get(engine);
    }

}
