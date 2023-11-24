/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.rest.jolokia;

import java.util.ArrayList;
import java.util.List;

import javax.management.MalformedObjectNameException;

import org.jolokia.client.BasicAuthenticator;
import org.jolokia.client.J4pClient;
import org.jolokia.client.exception.J4pException;
import org.jolokia.client.request.J4pReadRequest;
import org.jolokia.client.request.J4pReadResponse;
import org.json.simple.JSONObject;

public class JolokiaClient {
    public static final Integer DEFAULT_JOLOKIA_PORT = 8778;
    public static final String DEFAULT_JOLOKIA_URL = "http://localhost:" + DEFAULT_JOLOKIA_PORT + "/jolokia";

    public List<JSONObject> getConnectorMetrics(String type, String server, Integer taskMax, List<String> attributes) {
        List<JSONObject> metrics = new ArrayList<>();
        try {
            J4pClient client = createJolokiaClient();
            String baseMbeanName = String.format("debezium.%s:type=connector-metrics,context=streaming,server=%s", type, server);

            if ("sql_server".equals(type) || "mongodb".equals(type)) {
                for (int task = 0; task < Math.max(1, taskMax); task++) {
                    String mbeanName = String.format("%s,task=%s", baseMbeanName, task);
                    addMetrics(client, metrics, mbeanName, attributes);
                }
            }
            else {
                addMetrics(client, metrics, baseMbeanName, attributes);
            }
        }
        catch (J4pException | MalformedObjectNameException e) {
            throw new RuntimeException(e);
        }
        return metrics;
    }

    private void addMetrics(J4pClient client, List<JSONObject> metrics, String mbeanName, List<String> attributes) throws J4pException, MalformedObjectNameException {
        for (String attribute : attributes) {
            J4pReadRequest request = new J4pReadRequest(mbeanName, attribute);
            J4pReadResponse response = client.execute(request);
            metrics.add(response.asJSONObject());
        }
    }

    private J4pClient createJolokiaClient() {
        return J4pClient.url(JolokiaClient.DEFAULT_JOLOKIA_URL)
                .authenticator(new BasicAuthenticator().preemptive())
                .connectionTimeout(3000)
                .build();
    }
}
