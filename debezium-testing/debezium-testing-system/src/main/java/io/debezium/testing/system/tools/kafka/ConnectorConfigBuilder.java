/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.kafka;

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
    private final String connectorName;

    public ConnectorConfigBuilder(String connectorName) {
        this.mapper = new ObjectMapper();
        this.config = new HashMap<>();
        this.connectorName = connectorName;
    }

    public String getConnectorName() {
        return connectorName;
    }

    public String getDbServerName() {
        return connectorName.replaceAll("-", "_");
    }

    public ConnectorConfigBuilder put(String key, Object value) {
        config.put(key, value);
        return this;
    }

    public Map<String, Object> get() {
        return config;
    }

    /**
     * Adds all properties required to enable Avro serialisation via Apicurio Registry.
     * @param apicurioUrl Apicurio REST endpoint
     * @return this builder
     */
    public ConnectorConfigBuilder addApicurioV1AvroSupport(String apicurioUrl) {
        config.put("key.converter", "io.apicurio.registry.utils.converter.AvroConverter");
        config.put("key.converter.apicurio.registry.url", apicurioUrl);
        config.put("key.converter.apicurio.registry.converter.serializer", "io.apicurio.registry.utils.serde.AvroKafkaSerializer");
        config.put("key.converter.apicurio.registry.converter.deserializer", "io.apicurio.registry.utils.serde.AvroKafkaDeserializer");
        config.put("key.converter.apicurio.registry.global-id", "io.apicurio.registry.utils.serde.strategy.AutoRegisterIdStrategy");

        config.put("value.converter", "io.apicurio.registry.utils.converter.AvroConverter");
        config.put("value.converter.apicurio.registry.url", apicurioUrl);
        config.put("value.converter.apicurio.registry.converter.serializer", "io.apicurio.registry.utils.serde.AvroKafkaSerializer");
        config.put("value.converter.apicurio.registry.converter.deserializer", "io.apicurio.registry.utils.serde.AvroKafkaDeserializer");
        config.put("value.converter.apicurio.registry.global-id", "io.apicurio.registry.utils.serde.strategy.AutoRegisterIdStrategy");

        return this;
    }

    public ConnectorConfigBuilder addApicurioV2AvroSupport(String apicurioUrl) {
        config.put("key.converter", "io.apicurio.registry.utils.converter.AvroConverter");
        config.put("key.converter.apicurio.registry.url", apicurioUrl);
        config.put("key.converter.apicurio.registry.auto-register", true);
        config.put("key.converter.apicurio.registry.find-latest", true);

        config.put("value.converter", "io.apicurio.registry.utils.converter.AvroConverter");
        config.put("value.converter.apicurio.registry.url", apicurioUrl);
        config.put("value.converter.apicurio.registry.auto-register", true);
        config.put("value.converter.apicurio.registry.find-latest", true);

        return this;
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
                .withName(connectorName)
                .endMetadata()
                .withNewSpec()
                .withClassName((String) crConfig.remove("connector.class"))
                .withTasksMax((Integer) crConfig.remove("task.max"))
                .withConfig(crConfig)
                .endSpec()
                .build();
    }
}
