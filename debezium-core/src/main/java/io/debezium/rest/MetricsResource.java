/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.rest;

import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.IntrospectionException;
import javax.management.MBeanException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.apache.kafka.connect.connector.Connector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.rest.metrics.MetricsDescriptor;

public interface MetricsResource {
    Logger LOGGER = LoggerFactory.getLogger(MetricsResource.class);

    String CONNECTOR_METRICS_ENDPOINT = "/connectors/{connector-name}/metrics";

    Connector getConnector();

    MetricsDescriptor getMetrics(String connectorName, MBeanServer mBeanServer)
            throws MalformedObjectNameException, InstanceNotFoundException, IntrospectionException, ReflectionException, AttributeNotFoundException, MBeanException;

    default ObjectName getObjectName(String connector, String context, String serverName) throws MalformedObjectNameException {
        return new ObjectName(String.format("debezium.%s:type=connector-metrics,context=%s,server=%s", connector, context, serverName));
    }

    default ObjectName getObjectNameWithTask(String connector, String context, String serverName, String task) throws MalformedObjectNameException {
        return new ObjectName(
                String.format("debezium.%s:type=connector-metrics,context=%s,server=%s,task=%s", connector, context, serverName, task));
    }

    default ObjectName getObjectNameWithDatabase(String connector, String context, String serverName, String task, String databaseName)
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

    @GET
    @Path(CONNECTOR_METRICS_ENDPOINT)
    @Produces(MediaType.APPLICATION_JSON)
    default MetricsDescriptor getConnectorMetrics(@PathParam("connector-name") String connectorName)
            throws ReflectionException, MalformedObjectNameException, InstanceNotFoundException, IntrospectionException, AttributeNotFoundException, MBeanException {

        if (getConnector() == null) {
            throw new RuntimeException("Unable to fetch metrics for connector " + connectorName + " as the connector is not available.");
        }

        MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
        return getMetrics(connectorName, mBeanServer);
    }
}
