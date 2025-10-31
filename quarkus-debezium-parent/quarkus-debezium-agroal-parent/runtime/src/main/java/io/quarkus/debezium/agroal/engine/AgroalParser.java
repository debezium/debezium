/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.agroal.engine;

import static io.debezium.config.CommonConnectorConfig.NOTIFICATION_ENABLED_CHANNELS;
import static io.debezium.embedded.EmbeddedEngineConfig.CONNECTOR_CLASS;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import io.debezium.runtime.Connector;
import io.debezium.runtime.configuration.DebeziumEngineConfiguration;
import io.debezium.runtime.configuration.QuarkusDatasourceConfiguration;
import io.quarkus.debezium.agroal.configuration.AgroalDatasourceConfiguration;
import io.quarkus.debezium.configuration.DebeziumConfigurationEngineParser;
import io.quarkus.debezium.configuration.DebeziumConfigurationEngineParser.MultiEngineConfiguration;
import io.quarkus.debezium.notification.QuarkusNotificationChannel;

@Singleton
public class AgroalParser {
    private final DebeziumConfigurationEngineParser engineParser = new DebeziumConfigurationEngineParser();
    private final QuarkusNotificationChannel channel;
    private final Instance<AgroalDatasourceConfiguration> agroalInstances;

    @Inject
    public AgroalParser(QuarkusNotificationChannel channel, Instance<AgroalDatasourceConfiguration> agroalInstances) {
        this.channel = channel;
        this.agroalInstances = agroalInstances;
    }

    public List<MultiEngineConfiguration> parse(DebeziumEngineConfiguration debeziumEngineConfiguration, String dbKind, Connector connector) {
        List<MultiEngineConfiguration> multiEngineConfigurations = engineParser.parse(debeziumEngineConfiguration);

        Map<String, AgroalDatasourceConfiguration> configurationMaps = agroalInstances
                .stream()
                .filter(configuration -> configuration.getDbKind().equals(dbKind))
                .collect(Collectors.toMap(QuarkusDatasourceConfiguration::getSanitizedName, Function.identity()));

        return multiEngineConfigurations
                .stream()
                .map(engine -> enrichConfiguration(engine, configurationMaps, connector))
                .toList();
    }

    private MultiEngineConfiguration enrichConfiguration(MultiEngineConfiguration engine, Map<String, ? extends QuarkusDatasourceConfiguration> collect,
                                                         Connector connector) {
        HashMap<String, String> mutableMap = new HashMap<>(engine.configuration());

        mutableMap.compute(NOTIFICATION_ENABLED_CHANNELS.name(),
                (key, value) -> value == null ? channel.name() : value.concat("," + channel.name()));

        mutableMap.putAll(getQuarkusDatasourceConfigurationByEngineId(engine.engineId(), collect).asDebezium());
        mutableMap.put(CONNECTOR_CLASS.name(), connector.name());

        return new MultiEngineConfiguration(engine.engineId(), mutableMap);
    }

    private QuarkusDatasourceConfiguration getQuarkusDatasourceConfigurationByEngineId(String engineId, Map<String, ? extends QuarkusDatasourceConfiguration> collect) {
        QuarkusDatasourceConfiguration configuration = collect.get(engineId);

        if (configuration == null) {
            throw new IllegalArgumentException("No datasource configuration found for engine " + engineId);
        }

        return configuration;
    }
}
