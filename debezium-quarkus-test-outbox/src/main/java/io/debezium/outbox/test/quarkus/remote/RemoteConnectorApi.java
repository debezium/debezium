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

/**
 * REST API interface for interacting with the Debezium connector.
 */
@Path("/")
public interface RemoteConnectorApi {

    String CONNECTOR_NAME = "outbox";

    /**
     * Registers the outbox connector with the provided configuration.
     *
     * @param connectorConfiguration the configuration object for the Debezium connector
     */
    @POST
    @Path("/connectors")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    void registerOutboxConnector(DebeziumConnectorConfiguration connectorConfiguration);

    /**
     * Retrieves the status of the outbox connector.
     *
     * @return the status of the Debezium outbox connector
     */
    @GET
    @Path("/connectors/" + CONNECTOR_NAME + "/status")
    @Produces(MediaType.APPLICATION_JSON)
    DebeziumConnectorStatus outboxConnectorStatus();

    /**
     * Deletes the outbox connector.
     */
    @DELETE
    @Path("/connectors/" + CONNECTOR_NAME)
    @Produces(MediaType.APPLICATION_JSON)
    void deleteOutboxConnector();

    /**
     * Creates an instance of RemoteConnectorApi using the base URL from the configuration.
     *
     * @return a new instance of RemoteConnectorApi
     */
    static RemoteConnectorApi createInstance() {
        final String debeziumQuarkusOutboxConnectorHostUrl = ConfigProvider.getConfig()
                .getValue("debezium.quarkus.outbox.connector.host.url", String.class);

        return QuarkusRestClientBuilder.newBuilder()
                .baseUri(URI.create(debeziumQuarkusOutboxConnectorHostUrl))
                .build(RemoteConnectorApi.class);
    }
}
