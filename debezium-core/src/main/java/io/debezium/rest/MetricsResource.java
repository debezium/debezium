/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.rest;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.IntrospectionException;
import javax.management.MBeanException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;

import org.apache.kafka.connect.connector.Connector;

import io.debezium.rest.model.MetricsAttributes;
import io.debezium.rest.model.MetricsDescriptor;

public interface MetricsResource extends MetricsAttributes {
    String CONNECTOR_METRICS_ENDPOINT = "/connectors/{connector-name}/metrics";
    MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();

    Connector getConnector();

    MetricsDescriptor getMetrics(String connectorName)
            throws MalformedObjectNameException, InstanceNotFoundException, IntrospectionException, ReflectionException, AttributeNotFoundException, MBeanException;

    default ObjectName getObjectName(String connector, String context, String serverName) throws MalformedObjectNameException {
        return new ObjectName(String.format("debezium.%s:type=connector-metrics,context=%s,server=%s", connector, context, serverName));
    }

    default ObjectName getObjectName(String connector, String context, String serverName, String task) throws MalformedObjectNameException {
        return new ObjectName(
                String.format("debezium.%s:type=connector-metrics,context=%s,server=%s,task=%s", connector, context, serverName, task));
    }

    default ObjectName getObjectName(String connector, String context, String serverName, String task, String databaseName)
            throws MalformedObjectNameException {
        return new ObjectName(
                String.format("debezium.%s:type=connector-metrics,context=%s,server=%s,task=%s,database=%s", connector, context, serverName, task, databaseName));
    }

    default Map<String, String> getAttributes(List<String> attributes, ObjectName objectName, String connectorName, MBeanServer mBeanServer) {
        return attributes.stream()
                .collect(Collectors.toMap(
                        attribute -> attribute,
                        attribute -> getAttributeValue(attribute, objectName, connectorName, mBeanServer)));
    }

    default String getAttributeValue(String attribute, ObjectName objectName, String connectorName, MBeanServer mBeanServer) {
        try {
            return mBeanServer.getAttribute(objectName, attribute).toString();
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to get attribute " + attribute + " for connector " + connectorName + e);
        }
    }

    default MetricsDescriptor queryMetrics(Map<String, String> connectorConfig, String connectorName, String connector, String context)
            throws MalformedObjectNameException {

        String serverName = connectorConfig.get("topic.prefix");
        String tasksMax = connectorConfig.get("tasks.max");
        String databaseNames = connectorConfig.get("database.names");
        String[] namespaces = null;
        if (databaseNames != null) {
            namespaces = databaseNames.split(",");
        }

        List<MetricsDescriptor.Task> tasksPayload = new ArrayList<>();
        MetricsDescriptor.Connector connectorPayload = null;

        for (int task = 0; task < Integer.parseInt(tasksMax); task++) {
            ObjectName objectName;
            if (connector.equals("sql_server") || connector.equals("mongodb")) {
                objectName = getObjectName(connector, context, serverName, String.valueOf(task));
            }
            else {
                objectName = getObjectName(connector, context, serverName);
            }

            Map<String, String> connectionAttributes = getAttributes(getConnectionAttributes(), objectName, connectorName, mBeanServer);
            connectorPayload = new MetricsDescriptor.Connector(connectionAttributes);

            List<MetricsDescriptor.Namespace> namespacesPayload = new ArrayList<>();
            Map<String, String> connectorAttributes;
            if (namespaces != null) {
                for (String namespace : namespaces) {
                    ObjectName objectNameWithNamespace = getObjectName(connector, context, serverName, String.valueOf(task), namespace);
                    connectorAttributes = getAttributes(getConnectorAttributes(), objectNameWithNamespace, connectorName, mBeanServer);
                    namespacesPayload.add(new MetricsDescriptor.Namespace(namespace, connectorAttributes));
                }
            }
            else {
                connectorAttributes = getAttributes(getConnectorAttributes(), objectName, connectorName, mBeanServer);
                namespacesPayload.add(new MetricsDescriptor.Namespace("", connectorAttributes));
            }
            tasksPayload.add(new MetricsDescriptor.Task(task, namespacesPayload));
        }

        return new MetricsDescriptor(connectorName, tasksMax, connectorPayload, tasksPayload);
    }

    @Override
    default List<String> getConnectionAttributes() {
        return CONNECTION_ATTRIBUTES;
    }

    @Override
    default List<String> getConnectorAttributes() {
        return CONNECTOR_ATTRIBUTES;
    }

    @GET
    @Path(CONNECTOR_METRICS_ENDPOINT)
    @Produces(MediaType.APPLICATION_JSON)
    default MetricsDescriptor getConnectorMetrics(@PathParam("connector-name") String connectorName)
            throws ReflectionException, MalformedObjectNameException, InstanceNotFoundException, IntrospectionException, AttributeNotFoundException, MBeanException {

        if (getConnector() == null) {
            throw new RuntimeException("Unable to fetch metrics for connector " + connectorName + " as the connector is not available.");
        }
        return getMetrics(connectorName);
    }
}
