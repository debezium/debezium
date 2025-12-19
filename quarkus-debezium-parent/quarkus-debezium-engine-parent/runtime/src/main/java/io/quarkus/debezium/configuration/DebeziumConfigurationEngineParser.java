/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.configuration;

import static io.debezium.runtime.configuration.QuarkusDatasourceConfiguration.DEFAULT;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.runtime.configuration.DebeziumEngineConfiguration;

public class DebeziumConfigurationEngineParser {

    private static final Logger LOGGER = LoggerFactory.getLogger(DebeziumConfigurationEngineParser.class);
    public static final String CAPTURING = "capturing";

    public List<MultiEngineConfiguration> parse(DebeziumEngineConfiguration globalConfiguration) {
        Map<String, Map<String, String>> configurations = globalConfiguration
                .capturing()
                .values()
                .stream()
                .filter(configuration -> configuration.engineId().isPresent())
                .collect(Collectors.toMap(configuration -> configuration.engineId().get(), DebeziumEngineConfiguration.Capturing::configurations));

        /*
         * SmallRye Config in Quarkus is not able to map configuration on multiple fields if are used raw types
         * removing the keys with capturing
         */
        globalConfiguration.defaultConfiguration().keySet().removeIf(key -> key.startsWith(CAPTURING));

        if (!globalConfiguration.defaultConfiguration().isEmpty() && configurations.get(DEFAULT) != null) {
            LOGGER.warn("found two default configurations for debezium engine: {} \n {}", globalConfiguration.defaultConfiguration().toString(),
                    configurations.get(DEFAULT));
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

    public record MultiEngineConfiguration(String engineId, Map<String, String> configuration) {

    }
}
