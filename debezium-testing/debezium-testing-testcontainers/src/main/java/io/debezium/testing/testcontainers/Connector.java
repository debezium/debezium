/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.testcontainers;

import java.io.IOException;
import java.io.InputStream;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Class that represents the configuration document.
 */
public class Connector {

    public enum State {
        UNASSIGNED,
        RUNNING,
        RESTARTING,
        PAUSED,
        STOPPED,
        FAILED,
        DESTROYED
    }

    private static final String NAME = "name";
    private static final String CONFIGURATION = "config";

    private final String name;
    private final ConnectorConfiguration configuration;

    private final static ObjectMapper mapper = new ObjectMapper();

    private Connector(final String name, final ConnectorConfiguration configuration) {
        this.name = name;
        this.configuration = configuration;
    }

    /**
     * Loads configuration values from Debezium JSON configuration file.
     * @param inputStream of JSON configuration file.
     * @return Connector configuration.
     */
    public static Connector fromJson(final InputStream inputStream) {
        try {
            final ObjectNode connectorConfiguration = mapper.readValue(inputStream, ObjectNode.class);
            final String name = connectorConfiguration.get("name").asText();
            final ConnectorConfiguration config = ConnectorConfiguration.from(connectorConfiguration.get("config"));

            return new Connector(name, config);
        }
        catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public static Connector from(String name, ConnectorConfiguration configuration) {
        return new Connector(name, configuration);
    }

    public void appendOrOverrideConfiguration(ConnectorConfiguration newConfiguration) {
        final ObjectNode configurationNode = this.configuration.getConfiguration();
        final ObjectNode newConfigurationNode = newConfiguration.getConfiguration();

        newConfigurationNode.fields().forEachRemaining(e -> configurationNode.set(e.getKey(), e.getValue()));
    }

    public String toJson() {

        final JsonNode conf = mapper.valueToTree(this.configuration.getConfiguration());
        final ObjectNode connector = mapper.createObjectNode();

        connector.put(NAME, this.name);
        connector.set(CONFIGURATION, conf);

        try {
            return mapper.writeValueAsString(connector);
        }
        catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }

    public String getName() {
        return this.name;
    }
}
