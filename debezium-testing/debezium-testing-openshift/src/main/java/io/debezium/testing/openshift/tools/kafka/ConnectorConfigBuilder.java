/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.openshift.tools.kafka;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.strimzi.api.kafka.model.KafkaConnector;
import io.strimzi.api.kafka.model.KafkaConnectorBuilder;

/**
 *
 * @author Jakub Cechacek
 */
public class ConnectorConfigBuilder {
    private final Map<String, Object> config;
    private final ObjectMapper mapper;

    public ConnectorConfigBuilder() {
        this.mapper = new ObjectMapper();
        this.config = new HashMap<>();
    }

    public ConnectorConfigBuilder put(String key, Object value) {
        config.put(key, value);
        return this;
    }

    public Map<String, Object> get() {
        return config;
    }

    /**
     * Get configuration as JSON string
     * @return JSON string of connector config
     */
    public String getJsonString() {
        try {
            return mapper.writeValueAsString(config);
        }
        catch (JsonProcessingException e) {
            throw new IllegalStateException("Unable to convert connector config to JSON String");
        }
    }

    /**
     * Get configuration as OpenShift CR of type {@link KafkaConnector}
     * @return Connector CR
     */
    public KafkaConnector getCustomResource() {
        Map<String, Object> crConfig = new HashMap<>(config);

        KafkaConnectorBuilder connectorBuilder = new KafkaConnectorBuilder();
        return connectorBuilder
                .withNewMetadata()
                .withLabels(new HashMap<>())
                .endMetadata()
                .withNewSpec()
                .withClassName((String) crConfig.remove("connector.class"))
                .withTasksMax((Integer) crConfig.remove("task.max"))
                .withConfig(crConfig)
                .endSpec()
                .build();
    }
}
