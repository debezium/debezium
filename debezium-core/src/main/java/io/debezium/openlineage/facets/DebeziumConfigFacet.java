/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.openlineage.facets;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import io.openlineage.client.OpenLineage;

public class DebeziumConfigFacet implements OpenLineage.RunFacet {

    public static final String FACET_KEY_NAME = "debezium_config";

    private final Map<String, Object> configs = new HashMap<>();
    private final URI producer;

    public DebeziumConfigFacet(URI producer, Map<String, String> configurations) {
        this.producer = producer;
        configs.putAll(parseDebeziumConfigs(configurations));
    }

    @Override
    public URI get_producer() {
        return producer;
    }

    @Override
    public URI get_schemaURL() {
        return URI.create("https://github.com/debezium/debezium/tree/main/debezium-core/src/main/java/io/debezium/openlineage/facets/spec/DebeziumRunFacet.json");
    }

    public void setConfigs(Map<String, String> configurations) {
        configs.putAll(parseDebeziumConfigs(configurations));
    }

    // TODO check if the map value should be String
    public Map<String, Object> getConfigs() {
        return configs;
    }

    @Override
    public Map<String, Object> getAdditionalProperties() {
        return Map.of();
    }

    private Map<String, Object> parseDebeziumConfigs(Map<String, String> rawConfigs) {
        Map<String, Object> result = new HashMap<>();

        // Group configs by root key
        Map<String, List<Map.Entry<String, String>>> groupedConfigs = rawConfigs.entrySet()
                .stream()
                .collect(Collectors.groupingBy(entry -> entry.getKey().contains(".") ? entry.getKey().split("\\.")[0] : entry.getKey()));

        for (Map.Entry<String, List<Map.Entry<String, String>>> group : groupedConfigs.entrySet()) {
            String rootKey = group.getKey();
            List<Map.Entry<String, String>> entries = group.getValue();

            // Check if we have any nested properties
            boolean hasNestedProperties = entries.stream().anyMatch(entry -> entry.getKey().contains("."));

            if (!hasNestedProperties && entries.size() == 1) {
                // Simple key-value pair only
                result.put(rootKey, entries.get(0).getValue());
            }
            else {
                // Complex nested structure (ignore simple root value if nested properties exist)
                Map<String, Object> nested = new HashMap<>();
                for (Map.Entry<String, String> entry : entries) {
                    if (entry.getKey().contains(".")) {
                        String nestedKey = entry.getKey().substring(rootKey.length() + 1);
                        setNestedValue(nested, nestedKey, entry.getValue());
                    }
                }
                result.put(rootKey, nested);
            }
        }

        return result;
    }

    private void setNestedValue(Map<String, Object> map, String key, String value) {
        if (key.contains(".")) {
            String[] parts = key.split("\\.", 2);

            Map<String, Object> nested = (Map<String, Object>) map.computeIfAbsent(parts[0], k -> new HashMap<>());
            setNestedValue(nested, parts[1], value);
        }
        else {
            map.put(key, value);
        }
    }
}
