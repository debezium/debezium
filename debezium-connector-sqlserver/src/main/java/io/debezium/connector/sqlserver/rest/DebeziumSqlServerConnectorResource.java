/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver.rest;

import java.util.List;
import java.util.stream.Collectors;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import io.debezium.config.Configuration;
import io.debezium.connector.sqlserver.Module;
import io.debezium.connector.sqlserver.SqlServerConnector;
import io.debezium.rest.ConnectionValidationResource;
import io.debezium.rest.FilterValidationResource;
import io.debezium.rest.SchemaResource;
import io.debezium.rest.model.DataCollection;

/**
 * A JAX-RS Resource class defining endpoints of the Debezium SQL Server Connect REST Extension
 *
 */
@Path(DebeziumSqlServerConnectorResource.BASE_PATH)
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class DebeziumSqlServerConnectorResource
        implements SchemaResource, ConnectionValidationResource<SqlServerConnector>, FilterValidationResource<SqlServerConnector> {

    public static final String BASE_PATH = "/debezium/sqlserver";
    public static final String VERSION_ENDPOINT = "/version";

    @GET
    @Path(VERSION_ENDPOINT)
    public String getConnectorVersion() {
        return Module.version();
    }

    @Override
    public SqlServerConnector getConnector() {
        return new SqlServerConnector();
    }

    @Override
    public String getSchemaFilePath() {
        return "/META-INF/resources/sqlserver.json";
    }

    @Override
    public List<DataCollection> getMatchingCollections(Configuration configuration) {
        return getConnector().getMatchingCollections(configuration).stream()
                .map(tableId -> new DataCollection(tableId.catalog(), tableId.schema(), tableId.table()))
                .collect(Collectors.toList());
    }
}
