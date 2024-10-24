/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb.rest;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.management.MalformedObjectNameException;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.apache.kafka.connect.health.ConnectClusterState;

import io.debezium.config.Configuration;
import io.debezium.connector.mariadb.MariaDbConnector;
import io.debezium.connector.mariadb.Module;
import io.debezium.metadata.CollectionId;
import io.debezium.rest.ConnectionValidationResource;
import io.debezium.rest.FilterValidationResource;
import io.debezium.rest.MetricsResource;
import io.debezium.rest.SchemaResource;
import io.debezium.rest.model.MetricsDescriptor;

/**
 * A JAX-RS resource class defining endpoints of the Debezium MariaDB Connect REST extension.
 *
 * @author Chris Cranford
 */
@Path(DebeziumMariaDbConnectorResource.BASE_PATH)
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class DebeziumMariaDbConnectorResource
        implements SchemaResource, ConnectionValidationResource, FilterValidationResource, MetricsResource {

    public static final String BASE_PATH = "/debezium/mariadb";
    public static final String VERSION_ENDPOINT = "/version";
    private final ConnectClusterState connectClusterState;

    public DebeziumMariaDbConnectorResource(ConnectClusterState connectClusterState) {
        this.connectClusterState = connectClusterState;
    }

    @Override
    public String getSchemaFilePath() {
        return "/META-INF/resources/mariadb.json";
    }

    @Override
    public MariaDbConnector getConnector() {
        return new MariaDbConnector();
    }

    @Override
    public MetricsDescriptor getMetrics(String connectorName) throws MalformedObjectNameException {
        Map<String, String> connectorConfig = connectClusterState.connectorConfig(connectorName);
        return queryMetrics(connectorConfig, connectorName, Module.contextName().toLowerCase(), "streaming");
    }

    @GET
    @Path(VERSION_ENDPOINT)
    public String getConnectorVersion() {
        return Module.version();
    }

    @Override
    public List<CollectionId> getMatchingCollections(Configuration configuration) {
        return getConnector().getMatchingCollections(configuration).stream()
                .map(tableId -> new CollectionId(tableId.catalog(), tableId.table()))
                .collect(Collectors.toList());
    }
}
