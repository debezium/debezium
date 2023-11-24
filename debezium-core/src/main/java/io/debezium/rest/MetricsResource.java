/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.rest;

import java.util.List;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.apache.kafka.connect.connector.Connector;
import org.json.simple.JSONObject;

import io.debezium.rest.jolokia.JolokiaAttributes;
import io.debezium.rest.jolokia.JolokiaClient;

public interface MetricsResource {
    Connector getConnector();

    String getAttributesFilePath();

    List<JSONObject> getMetrics(String connectorName, JolokiaClient client, List<String> attributes);

    String CONNECTOR_METRICS_ENDPOINT = "/connectors/{connector-name}/metrics";

    @GET
    @Path(CONNECTOR_METRICS_ENDPOINT)
    @Produces(MediaType.APPLICATION_JSON)
    default List<JSONObject> getConnectorMetrics(@PathParam("connector-name") String connectorName) {
        if (getConnector() == null) {
            throw new RuntimeException("Unable to fetch metrics for connector " + connectorName + " as the connector is not available.");
        }
        JolokiaClient jolokiaClient = new JolokiaClient();
        JolokiaAttributes jolokiaAttributes = new JolokiaAttributes(getAttributesFilePath());
        List<String> attributes = jolokiaAttributes.getAttributeNames();
        return getMetrics(connectorName, jolokiaClient, attributes);
    }
}
