/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.quarkus.sample.app.events;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

@Path("/notifications")
@ApplicationScoped
public class SnapshotEventResource {

    private final SnapshotEventObserver snapshotEventObserver;

    public SnapshotEventResource(SnapshotEventObserver snapshotEventObserver) {
        this.snapshotEventObserver = snapshotEventObserver;
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Response notification() {
        if (snapshotEventObserver.getSnapshotEvents().isEmpty()) {
            return Response.status(Response.Status.NOT_FOUND).build();
        }

        return Response.ok(snapshotEventObserver
                .getSnapshotEvents()
                .stream()
                .map(a -> a.getClass().getName()))
                .build();
    }
}
