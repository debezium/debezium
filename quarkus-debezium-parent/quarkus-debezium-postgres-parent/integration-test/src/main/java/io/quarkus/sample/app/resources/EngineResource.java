/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.quarkus.sample.app.resources;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.core.Response;

import io.debezium.runtime.DebeziumConnectorRegistry;
import io.debezium.runtime.DebeziumStatus;
import io.quarkus.sample.app.dto.EngineManifest;

@Path("engine")
@ApplicationScoped
public class EngineResource {

    private final DebeziumConnectorRegistry registry;

    public EngineResource(DebeziumConnectorRegistry registry) {
        this.registry = registry;
    }

    @GET
    @Path("manifest")
    public Response engines() {
        return Response.ok(registry.engines()
                .stream()
                .map(engine -> new EngineManifest(engine.captureGroup().id(), engine.connector().name()))
                .toList()).build();
    }

    @GET
    @Path("status")
    public DebeziumStatus getState() {
        return registry.engines().getFirst().status();
    }
}
