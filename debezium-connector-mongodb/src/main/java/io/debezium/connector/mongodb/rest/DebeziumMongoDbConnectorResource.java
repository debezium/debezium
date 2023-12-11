/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.rest;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.apache.kafka.connect.health.ConnectClusterState;

import io.debezium.config.Configuration;
import io.debezium.connector.mongodb.Module;
import io.debezium.connector.mongodb.MongoDbConnector;
import io.debezium.rest.ConnectionValidationResource;
import io.debezium.rest.FilterValidationResource;
import io.debezium.rest.MetricsResource;
import io.debezium.rest.SchemaResource;
import io.debezium.rest.metrics.MetricsAttributes;
import io.debezium.rest.metrics.MetricsDescriptor;
import io.debezium.rest.model.DataCollection;

/**
 * A JAX-RS Resource class defining endpoints of the Debezium MongoDB Connect REST Extension
 *
 */
@Path(DebeziumMongoDbConnectorResource.BASE_PATH)
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class DebeziumMongoDbConnectorResource
        implements SchemaResource, ConnectionValidationResource<MongoDbConnector>, FilterValidationResource<MongoDbConnector>, MetricsResource {

    public static final String BASE_PATH = "/debezium/mongodb";
    public static final String VERSION_ENDPOINT = "/version";
    private final ConnectClusterState connectClusterState;

    public DebeziumMongoDbConnectorResource(ConnectClusterState connectClusterState) {
        this.connectClusterState = connectClusterState;
    }

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
    public MetricsDescriptor getMetrics(String connectorName, MBeanServer mBeanServer)
            throws MalformedObjectNameException {
        Map<String, String> connectorConfig = connectClusterState.connectorConfig(connectorName);
        String serverName = connectorConfig.get("topic.prefix");
        String tasksMax = connectorConfig.get("tasks.max");

        MetricsDescriptor.Connector connector = null;
        List<MetricsDescriptor.Task> tasks = new ArrayList<>();

        for (int task = 0; task < Integer.parseInt(tasksMax); task++) {
            ObjectName objectName = getObjectNameWithTask("mongodb", "streaming", serverName, String.valueOf(task));
            Map<String, String> connectionAttributes = getAttributes(MetricsAttributes.getConnectionAttributes(), objectName, connectorName, mBeanServer);
            Map<String, String> connectorAttributes = getAttributes(MetricsAttributes.getConnectorAttributes(), objectName, connectorName, mBeanServer);

            connector = new MetricsDescriptor.Connector(connectionAttributes);
            tasks.add(new MetricsDescriptor.Task(task, List.of(new MetricsDescriptor.Database("", connectorAttributes))));
        }
        return new MetricsDescriptor(connectorName, tasksMax, connector, tasks);
    }

    @Override
    public String getSchemaFilePath() {
        return "/META-INF/resources/mongodb.json";
    }

    @Override
    public List<DataCollection> getMatchingCollections(Configuration configuration) {
        return getConnector().getMatchingCollections(configuration).stream()
                .map(collectionId -> new DataCollection(collectionId.replicaSetName(), collectionId.dbName(), collectionId.name()))
                .collect(Collectors.toList());
    }
}
