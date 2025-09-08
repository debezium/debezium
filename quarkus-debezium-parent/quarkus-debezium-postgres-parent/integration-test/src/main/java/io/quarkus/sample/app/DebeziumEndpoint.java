/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.sample.app;

import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.core.Response;

import io.debezium.runtime.DebeziumConnectorRegistry;
import io.debezium.runtime.DebeziumStatus;

@Path("/api/debezium")
public class DebeziumEndpoint {

    @Inject
    private DebeziumConnectorRegistry registry;

    @Inject
    private CaptureService captureService;

    @GET
    @Path("status")
    public DebeziumStatus getState() {
        return registry.engines().getFirst().status();
    }

    @GET
    @Path("captured")
    public Response capture() {
        if (captureService.isInvoked()) {
            return Response.status(Response.Status.FOUND).build();
        }

        return Response.status(Response.Status.NOT_FOUND).build();
    }

    @GET
    @Path("products")
    public Response products() {
        if (captureService.products().isEmpty()) {
            return Response.status(Response.Status.NOT_FOUND).build();
        }
        return Response.ok(captureService.products()).build();
    }

    @GET
    @Path("engines")
    public Response product() {
        return Response.ok(registry.engines()
                .stream()
                .map(engine -> new EngineManifest(engine.captureGroup().id(), engine.connector().name()))
                .toList()).build();
    }

    record EngineManifest(String group, String connector) {

    }
}
