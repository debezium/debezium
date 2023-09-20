/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver.rest;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.apache.kafka.connect.connector.Connector;

import io.debezium.connector.sqlserver.Module;
import io.debezium.connector.sqlserver.SqlServerConnector;
import io.debezium.rest.ConnectionValidationResource;
import io.debezium.rest.SchemaResource;

/**
 * A JAX-RS Resource class defining endpoints of the Debezium SQL Server Connect REST Extension
 *
 */
@Path(DebeziumSqlServerConnectorResource.BASE_PATH)
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class DebeziumSqlServerConnectorResource implements SchemaResource, ConnectionValidationResource {

    public static final String BASE_PATH = "/debezium/sqlserver";
    public static final String VERSION_ENDPOINT = "/version";

    @GET
    @Path(VERSION_ENDPOINT)
    public String getConnectorVersion() {
        return Module.version();
    }

    @Override
    public Connector getConnector() {
        return new SqlServerConnector();
    }

    @Override
    public String getSchemaFilePath() {
        return "/META-INF/resources/sqlserver.json";
    }

}
