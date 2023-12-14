/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.rest;

import java.util.List;
import java.util.stream.Collectors;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import io.debezium.config.Configuration;
import io.debezium.connector.mysql.Module;
import io.debezium.connector.mysql.MySqlConnector;
import io.debezium.rest.ConnectionValidationResource;
import io.debezium.rest.FilterValidationResource;
import io.debezium.rest.SchemaResource;
import io.debezium.rest.model.DataCollection;

/**
 * A JAX-RS Resource class defining endpoints of the Debezium MySQL Connect REST Extension
 *
 */
@Path(DebeziumMySqlConnectorResource.BASE_PATH)
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class DebeziumMySqlConnectorResource
        implements SchemaResource, ConnectionValidationResource<MySqlConnector>, FilterValidationResource<MySqlConnector> {

    public static final String BASE_PATH = "/debezium/mysql";
    public static final String VERSION_ENDPOINT = "/version";

    @Override
    public String getSchemaFilePath() {
        return "/META-INF/resources/mysql.json";
    }

    @Override
    public MySqlConnector getConnector() {
        return new MySqlConnector();
    }

    @GET
    @Path(VERSION_ENDPOINT)
    public String getConnectorVersion() {
        return Module.version();
    }

    @Override
    public List<DataCollection> getMatchingCollections(Configuration configuration) {
        return getConnector().getMatchingCollections(configuration).stream()
                .map(tableId -> new DataCollection(tableId.catalog(), tableId.table()))
                .collect(Collectors.toList());
    }
}
