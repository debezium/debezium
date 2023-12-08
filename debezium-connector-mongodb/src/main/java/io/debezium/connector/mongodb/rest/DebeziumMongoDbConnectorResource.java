/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.rest;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import io.debezium.connector.mongodb.Module;
import io.debezium.connector.mongodb.MongoDbConnector;
import io.debezium.rest.ConnectionValidationResource;
import io.debezium.rest.FilterValidationResource;
import io.debezium.rest.SchemaResource;

/**
 * A JAX-RS Resource class defining endpoints of the Debezium MongoDB Connect REST Extension
 *
 */
@Path(DebeziumMongoDbConnectorResource.BASE_PATH)
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class DebeziumMongoDbConnectorResource
        implements SchemaResource, ConnectionValidationResource<MongoDbConnector>, FilterValidationResource<MongoDbConnector> {

    public static final String BASE_PATH = "/debezium/mongodb";
    public static final String VERSION_ENDPOINT = "/version";

    @GET
    @Path(VERSION_ENDPOINT)
    public String getConnectorVersion() {
        return Module.version();
    }

    @Override
    public MongoDbConnector getConnector() {
        return new MongoDbConnector();
    }

    @Override
    public String getSchemaFilePath() {
        return "/META-INF/resources/mongodb.json";
    }

}
