/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine;

import static io.debezium.config.CommonConnectorConfig.NOTIFICATION_ENABLED_CHANNELS;
import static io.debezium.embedded.EmbeddedEngineConfig.CONNECTOR_CLASS;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;

import io.debezium.connector.sqlserver.SqlServerConnector;
import io.debezium.runtime.Connector;
import io.debezium.runtime.ConnectorProducer;
import io.debezium.runtime.Debezium;
import io.debezium.runtime.DebeziumConnectorRegistry;
import io.debezium.runtime.EngineManifest;
import io.debezium.runtime.configuration.DebeziumEngineConfiguration;
import io.debezium.runtime.configuration.QuarkusDatasourceConfiguration;
import io.quarkus.debezium.configuration.DebeziumConfigurationEngineParser;
import io.quarkus.debezium.configuration.DebeziumConfigurationEngineParser.MultiEngineConfiguration;
import io.quarkus.debezium.engine.capture.consumer.SourceRecordConsumerHandler;
import io.quarkus.debezium.notification.QuarkusNotificationChannel;
import io.quarkus.debezium.sqlserver.configuration.SqlServerDatasourceConfiguration;
import jakarta.inject.Singleton;

public class SqlServerEngineProducer implements ConnectorProducer {
    public static final Connector SQLSERVER = new Connector(SqlServerConnector.class.getName());

    private final StateHandler stateHandler;
    private final Map<String, SqlServerDatasourceConfiguration> quarkusDatasourceConfigurations;
    private final QuarkusNotificationChannel channel;
    private final SourceRecordConsumerHandler sourceRecordConsumerHandler;
    private final DebeziumConfigurationEngineParser engineParser = new DebeziumConfigurationEngineParser();

    @Inject
    public SqlServerEngineProducer(StateHandler stateHandler,
                                   Instance<SqlServerDatasourceConfiguration> configurations,
                                   QuarkusNotificationChannel channel,
                                   SourceRecordConsumerHandler sourceRecordConsumerHandler) {
        this.stateHandler = stateHandler;
        this.channel = channel;
        this.sourceRecordConsumerHandler = sourceRecordConsumerHandler;
        this.quarkusDatasourceConfigurations = configurations
                .stream()
                .collect(Collectors.toMap(QuarkusDatasourceConfiguration::getSanitizedName, Function.identity()));
    }

    public SqlServerEngineProducer(StateHandler stateHandler,
                                   Map<String, SqlServerDatasourceConfiguration> quarkusDatasourceConfigurations,
                                   QuarkusNotificationChannel channel,
                                   SourceRecordConsumerHandler sourceRecordConsumerHandler) {
        this.stateHandler = stateHandler;
        this.quarkusDatasourceConfigurations = quarkusDatasourceConfigurations;
        this.channel = channel;
        this.sourceRecordConsumerHandler = sourceRecordConsumerHandler;
    }

    @Produces
    @Singleton
    @Override
    public DebeziumConnectorRegistry engine(DebeziumEngineConfiguration debeziumEngineConfiguration) {
        List<MultiEngineConfiguration> multiEngineConfigurations = engineParser.parse(debeziumEngineConfiguration);

        /*
         * enrich Quarkus-like debezium configuration with quarkus datasource configuration
         */
        List<MultiEngineConfiguration> enrichedMultiEngineConfigurations = multiEngineConfigurations
                .stream()
                .map(engine -> enrichConfiguration(engine, quarkusDatasourceConfigurations))
                .toList();

        return new DebeziumConnectorRegistry() {
            private final Map<String, Debezium> engines = enrichedMultiEngineConfigurations
                    .stream()
                    .map(engine -> {
                        EngineManifest engineManifest = new EngineManifest(engine.engineId());

                        return Map.entry(engine.engineId(), new SourceRecordDebezium(
                                engine.configuration(),
                                stateHandler,
                                SQLSERVER,
                                sourceRecordConsumerHandler.get(engineManifest), engineManifest));
                    })
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            @Override
            public Connector connector() {
                return SQLSERVER;
            }

            @Override
            public Debezium get(EngineManifest manifest) {
                return engines.get(manifest.id());
            }

            @Override
            public List<Debezium> engines() {
                return engines.values().stream().toList();
            }
        };
    }

    private MultiEngineConfiguration enrichConfiguration(MultiEngineConfiguration engine, Map<String, ? extends QuarkusDatasourceConfiguration> collect) {
        HashMap<String, String> mutableMap = new HashMap<>(engine.configuration());

        mutableMap.compute(NOTIFICATION_ENABLED_CHANNELS.name(),
                (key, value) -> value == null ? channel.name() : value.concat("," + channel.name()));

        mutableMap.putAll(getQuarkusDatasourceConfigurationByEngineId(engine.engineId(), collect).asDebezium());
        mutableMap.put(CONNECTOR_CLASS.name(), SQLSERVER.name());

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
