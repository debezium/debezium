/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.rest;

import java.util.List;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.apache.kafka.connect.connector.Connector;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.Module;
import io.debezium.connector.oracle.OracleConnector;
import io.debezium.rest.ConnectionValidationResource;
import io.debezium.rest.FilterValidationResource;
import io.debezium.rest.SchemaResource;
import io.debezium.rest.model.DataCollection;

/**
 * A JAX-RS Resource class defining endpoints of the Debezium Oracle Connect REST Extension
 *
 */
@Path(DebeziumOracleConnectorResource.BASE_PATH)
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class DebeziumOracleConnectorResource implements SchemaResource, ConnectionValidationResource, FilterValidationResource {

    public static final String BASE_PATH = "/debezium/oracle";
    public static final String VERSION_ENDPOINT = "/version";

    @Override
    public String getSchemaFilePath() {
        return "/META-INF/resources/oracle.json";
    }

    @Override
    public Connector getConnector() {
        return new OracleConnector();
    }

    @Override
    public List<DataCollection> getMatchingCollections(Configuration configuration) {
        return ((OracleConnector) getConnector()).getMatchingCollections(configuration);
    }

    @GET
    @Path(VERSION_ENDPOINT)
    public String getConnectorVersion() {
        return Module.version();
    }
}
