/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.testcontainers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Class that represents the config element of the configuration document.
 */
public class Configuration {

    private final ObjectMapper mapper = new ObjectMapper();
    private final ObjectNode configNode;

    protected Configuration() {
        this.configNode = this.mapper.createObjectNode();
    }

    public static Configuration create() {
        return new Configuration();
    }

    static Configuration from(JsonNode configNode) {
        final Configuration configuration = new Configuration();
        configNode.fields().forEachRemaining(e -> configuration.configNode.set(e.getKey(), e.getValue()));
        return configuration;
    }

    public Configuration with(String key, String value) {
        this.configNode.put(key, value);
        return this;
    }

    public Configuration with(String key, Integer value) {
        this.configNode.put(key, value);
        return this;
    }

    public Configuration with(String key, Long value) {
        this.configNode.put(key, value);
        return this;
    }

    public Configuration with(String key, Boolean value) {
        this.configNode.put(key, value);
        return this;
    }

    public Configuration with(String key, Double value) {
        this.configNode.put(key, value);
        return this;
    }

    ObjectNode getConfiguration() {
        return configNode;
    }

}
