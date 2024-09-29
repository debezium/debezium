/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.outbox.test.quarkus.remote;

import java.net.URI;

import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import org.eclipse.microprofile.config.ConfigProvider;

import io.quarkus.rest.client.reactive.QuarkusRestClientBuilder;

@Path("/")
public interface RemoteConnectorApi {

    String CONNECTOR_NAME = "outbox";

    @POST
    @Path("/connectors")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    void registerOutboxConnector(DebeziumConnectorConfiguration connectorConfiguration);

    @GET
    @Path("/connectors/" + CONNECTOR_NAME + "/status")
    @Produces(MediaType.APPLICATION_JSON)
    DebeziumConnectorStatus outboxConnectorStatus();

    @DELETE
    @Path("/connectors/" + CONNECTOR_NAME)
    @Produces(MediaType.APPLICATION_JSON)
    void deleteOutboxConnector();

    static RemoteConnectorApi createInstance() {
        final String debeziumQuarkusOutboxConnectorHostUrl = ConfigProvider.getConfig()
                .getValue("debezium.quarkus.outbox.connector.host.url", String.class);

        return QuarkusRestClientBuilder.newBuilder()
                .baseUri(URI.create(debeziumQuarkusOutboxConnectorHostUrl))
                .build(RemoteConnectorApi.class);
    }
}
