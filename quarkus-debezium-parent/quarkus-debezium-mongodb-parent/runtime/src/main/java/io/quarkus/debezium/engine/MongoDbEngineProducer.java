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
import jakarta.inject.Singleton;

import io.debezium.connector.mongodb.MongoDbConnector;
import io.debezium.runtime.Connector;
import io.debezium.runtime.ConnectorProducer;
import io.debezium.runtime.Debezium;
import io.debezium.runtime.DebeziumConnectorRegistry;
import io.debezium.runtime.EngineManifest;
import io.debezium.runtime.configuration.DebeziumEngineConfiguration;
import io.debezium.runtime.configuration.QuarkusDatasourceConfiguration;
import io.quarkus.debezium.configuration.DebeziumConfigurationEngineParser;
import io.quarkus.debezium.configuration.DebeziumConfigurationEngineParser.MultiEngineConfiguration;
import io.quarkus.debezium.configuration.MongoDbDatasourceConfiguration;
import io.quarkus.debezium.engine.capture.consumer.SourceRecordConsumerHandler;
import io.quarkus.debezium.notification.QuarkusNotificationChannel;

public class MongoDbEngineProducer implements ConnectorProducer {

    public static final Connector MONGODB = new Connector(MongoDbConnector.class.getName());
    private final StateHandler stateHandler;
    private final Map<String, MongoDbDatasourceConfiguration> quarkusDatasourceConfigurations;
    private final QuarkusNotificationChannel channel;
    private final SourceRecordConsumerHandler sourceRecordConsumerHandler;
    private final DebeziumConfigurationEngineParser engineParser = new DebeziumConfigurationEngineParser();

    @Inject
    public MongoDbEngineProducer(StateHandler stateHandler,
                                 Instance<MongoDbDatasourceConfiguration> configurations,
                                 QuarkusNotificationChannel channel,
                                 SourceRecordConsumerHandler sourceRecordConsumerHandler) {
        this.stateHandler = stateHandler;
        this.channel = channel;
        this.sourceRecordConsumerHandler = sourceRecordConsumerHandler;
        this.quarkusDatasourceConfigurations = configurations
                .stream()
                .collect(Collectors.toMap(QuarkusDatasourceConfiguration::getSanitizedName, Function.identity()));
    }

    public MongoDbEngineProducer(StateHandler stateHandler,
                                 Map<String, MongoDbDatasourceConfiguration> quarkusDatasourceConfigurations,
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
                .map(engine -> merge(engine, quarkusDatasourceConfigurations))
                .toList();

        return new DebeziumConnectorRegistry() {
            private final Map<String, Debezium> engines = enrichedMultiEngineConfigurations
                    .stream()
                    .map(engine -> {
                        EngineManifest engineManifest = new EngineManifest(engine.engineId());

                        return Map.entry(engine.engineId(), new SourceRecordDebezium(
                                engine.configuration(),
                                stateHandler,
                                MONGODB,
                                sourceRecordConsumerHandler.get(engineManifest), engineManifest));
                    })
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            @Override
            public Connector connector() {
                return MONGODB;
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

    private MultiEngineConfiguration merge(MultiEngineConfiguration engine, Map<String, ? extends QuarkusDatasourceConfiguration> configurations) {
        HashMap<String, String> mutableConfigurations = new HashMap<>(engine.configuration());

        mutableConfigurations.compute(NOTIFICATION_ENABLED_CHANNELS.name(),
                (key, value) -> value == null ? channel.name() : value.concat("," + channel.name()));

        mutableConfigurations.putAll(getQuarkusDatasourceConfigurationByEngineId(engine.engineId(), configurations).asDebezium());
        mutableConfigurations.put(CONNECTOR_CLASS.name(), MONGODB.name());

        return new MultiEngineConfiguration(engine.engineId(), mutableConfigurations);
    }

    private QuarkusDatasourceConfiguration getQuarkusDatasourceConfigurationByEngineId(String engineId, Map<String, ? extends QuarkusDatasourceConfiguration> configurations) {
        QuarkusDatasourceConfiguration configuration = configurations.get(engineId);

        if (configuration == null) {
            throw new IllegalArgumentException("No datasource configuration found for engine " + engineId);
        }

        return configuration;
    }
}
