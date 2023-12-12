/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.rest;

import java.util.List;
import java.util.Map;

import java.util.stream.Collectors;

import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.apache.kafka.connect.health.ConnectClusterState;

import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.Module;
import io.debezium.connector.postgresql.PostgresConnector;
import io.debezium.rest.ConnectionValidationResource;
import io.debezium.rest.FilterValidationResource;
import io.debezium.rest.MetricsResource;
import io.debezium.rest.SchemaResource;
import io.debezium.rest.model.DataCollection;
import io.debezium.rest.model.MetricsDescriptor;

/**
 * A JAX-RS Resource class defining endpoints of the Debezium Postgres Connect REST Extension
 *
 */
@Path(DebeziumPostgresConnectorResource.BASE_PATH)
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class DebeziumPostgresConnectorResource
        implements SchemaResource, ConnectionValidationResource<PostgresConnector>, FilterValidationResource<PostgresConnector>, MetricsResource {

    public static final String BASE_PATH = "/debezium/postgres";
    public static final String VERSION_ENDPOINT = "/version";
    private final ConnectClusterState connectClusterState;

    public DebeziumPostgresConnectorResource(ConnectClusterState connectClusterState) {
        this.connectClusterState = connectClusterState;
    }

    @GET
    @Path(VERSION_ENDPOINT)
    public String getConnectorVersion() {
        return Module.version();
    }

    @Override
    public PostgresConnector getConnector() {
        return new PostgresConnector();
    }

    @Override
    public MetricsDescriptor getMetrics(String connectorName, MBeanServer mBeanServer) throws MalformedObjectNameException {
        Map<String, String> connectorConfig = connectClusterState.connectorConfig(connectorName);
        String serverName = connectorConfig.get("topic.prefix");
        String tasksMax = connectorConfig.get("tasks.max");

        return queryMetrics(connectorName, Module.logicalName(), "streaming", mBeanServer, serverName, tasksMax, null);
    }

    public String getSchemaFilePath() {
        return "/META-INF/resources/postgres.json";
    }

    @Override
    public List<DataCollection> getMatchingCollections(Configuration configuration) {
        return getConnector().getMatchingCollections(configuration).stream()
                .map(tableId -> new DataCollection(tableId.schema(), tableId.table()))
                .collect(Collectors.toList());
    }
}
