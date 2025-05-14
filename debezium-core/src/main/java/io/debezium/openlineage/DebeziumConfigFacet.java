package io.debezium.openlineage;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import io.openlineage.client.OpenLineage;

public class DebeziumConfigFacet implements OpenLineage.RunFacet {

    public static final String FACET_KEY_NAME = "debezium_config";

    private final Map<String, Object> configs = new HashMap<>();
    private final URI producer;

    public DebeziumConfigFacet(URI producer, Map<String, String> configurations) {
        this.producer = producer;
        configs.putAll(configurations);
    }

    @Override
    public URI get_producer() {
        return producer;
    }

    @Override
    public URI get_schemaURL() {
        // TODO define the schema
        return URI.create("https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/RunFacet");
    }

    public void setConfigs(Map<String, Object> configurations) {
        configs.putAll(configurations);
    }

    public Map<String, Object> getConfigs() {
        return configs;
    }

    @Override
    public Map<String, Object> getAdditionalProperties() {
        return Map.of();
    }
}
