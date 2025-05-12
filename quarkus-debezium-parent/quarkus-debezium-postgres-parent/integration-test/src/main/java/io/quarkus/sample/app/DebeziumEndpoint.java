/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.sample.app;

import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;

import io.debezium.runtime.Debezium;
import io.debezium.runtime.DebeziumManifest;

@Path("/api/debezium")
public class DebeziumEndpoint {

    @Inject
    private Debezium debezium;

    @GET
    @Path("manifest")
    public DebeziumManifest getManifest() {
        return debezium.manifest();
    }
}
