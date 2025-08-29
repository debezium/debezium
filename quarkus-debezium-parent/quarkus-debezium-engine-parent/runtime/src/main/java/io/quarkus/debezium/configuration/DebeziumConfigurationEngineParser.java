/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.configuration;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import io.debezium.runtime.configuration.DebeziumEngineConfiguration;

public class DebeziumConfigurationEngineParser {

    public static final String DEFAULT = "default";

    public List<MultiEngineConfiguration> parse(DebeziumEngineConfiguration globalConfiguration) {
        Map<String, Map<String, String>> configurations = globalConfiguration
                .capturing()
                .values()
                .stream()
                .filter(configuration -> configuration.groupId().isPresent())
                .collect(Collectors.toMap(configuration -> configuration.groupId().get(), DebeziumEngineConfiguration.Capturing::configurations));

        if (!globalConfiguration.defaultConfiguration().isEmpty() && configurations.get(DEFAULT) != null) {
            throw new IllegalArgumentException("impossible to have two default configuration, please remove one of them!");
        }

        List<MultiEngineConfiguration> engines = configurations
                .entrySet()
                .stream()
                .map(configuration -> new MultiEngineConfiguration(configuration.getKey(), configuration.getValue()))
                .collect(Collectors.toList());

        if (!globalConfiguration.defaultConfiguration().isEmpty()) {
            engines.add(new MultiEngineConfiguration(DEFAULT, globalConfiguration.defaultConfiguration()));
        }

        return engines;

    }

    public record MultiEngineConfiguration(String groupId, Map<String, String> configuration) {

    }
}
