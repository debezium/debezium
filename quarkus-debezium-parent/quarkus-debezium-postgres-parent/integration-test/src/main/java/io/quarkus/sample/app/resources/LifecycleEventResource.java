/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.quarkus.sample.app.resources;

import java.util.List;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;

import io.debezium.runtime.DebeziumConnectorRegistry;
import io.quarkus.sample.app.events.LifecycleEventObserver;

/**
 * @author Chris Cranford
 */
@Path("/lifecycle-events")
@ApplicationScoped
public class LifecycleEventResource {

    @Inject
    private DebeziumConnectorRegistry registry;

    @Inject
    LifecycleEventObserver observer;

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public List<String> getEvents(@QueryParam("engine") @DefaultValue("default") String engine) {
        // Returns a list of all observed event names
        return observer.getLifecycleEvents(engine)
                .stream()
                .map(o -> o.getClass().getName())
                .toList();
    }

}
