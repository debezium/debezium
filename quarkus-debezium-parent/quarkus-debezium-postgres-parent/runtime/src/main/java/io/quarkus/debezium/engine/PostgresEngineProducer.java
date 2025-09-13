/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine;

import static io.debezium.config.CommonConnectorConfig.DATABASE_CONFIG_PREFIX;
import static io.debezium.config.CommonConnectorConfig.NOTIFICATION_ENABLED_CHANNELS;
import static io.debezium.embedded.EmbeddedEngineConfig.CONNECTOR_CLASS;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import io.debezium.connector.postgresql.PostgresConnector;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.runtime.CaptureGroup;
import io.debezium.runtime.Connector;
import io.debezium.runtime.ConnectorProducer;
import io.debezium.runtime.Debezium;
import io.debezium.runtime.DebeziumConnectorRegistry;
import io.debezium.runtime.configuration.DebeziumEngineConfiguration;
import io.debezium.runtime.configuration.QuarkusDatasourceConfiguration;
import io.quarkus.debezium.configuration.DebeziumConfigurationEngineParser;
import io.quarkus.debezium.configuration.DebeziumConfigurationEngineParser.MultiEngineConfiguration;
import io.quarkus.debezium.engine.capture.consumer.SourceRecordConsumerHandler;
import io.quarkus.debezium.notification.QuarkusNotificationChannel;

@ApplicationScoped
public class PostgresEngineProducer implements ConnectorProducer {

    public static final Connector POSTGRES = new Connector(PostgresConnector.class.getName());
    public static final String DEBEZIUM_DATASOURCE_HOSTNAME = DATABASE_CONFIG_PREFIX + JdbcConfiguration.HOSTNAME.name();

    private final StateHandler stateHandler;
    private final Map<String, QuarkusDatasourceConfiguration> quarkusDatasourceConfigurations;
    private final QuarkusNotificationChannel channel;
    private final SourceRecordConsumerHandler sourceRecordConsumerHandler;
    private final DebeziumConfigurationEngineParser engineParser = new DebeziumConfigurationEngineParser();

    @Inject
    public PostgresEngineProducer(StateHandler stateHandler,
                                  Instance<QuarkusDatasourceConfiguration> configurations,
                                  QuarkusNotificationChannel channel,
                                  SourceRecordConsumerHandler sourceRecordConsumerHandler) {
        this.stateHandler = stateHandler;
        this.channel = channel;
        this.sourceRecordConsumerHandler = sourceRecordConsumerHandler;
        this.quarkusDatasourceConfigurations = configurations
                .stream()
                .collect(Collectors.toMap(QuarkusDatasourceConfiguration::getSanitizedName, Function.identity()));
    }

    public PostgresEngineProducer(StateHandler stateHandler,
                                  Map<String, QuarkusDatasourceConfiguration> quarkusDatasourceConfigurations,
                                  QuarkusNotificationChannel channel,
                                  SourceRecordConsumerHandler sourceRecordConsumerHandler) {
        this.stateHandler = stateHandler;
        this.quarkusDatasourceConfigurations = quarkusDatasourceConfigurations;
        this.channel = channel;
        this.sourceRecordConsumerHandler = sourceRecordConsumerHandler;
    }

    @Produces
    @Singleton
    public DebeziumConnectorRegistry engine(DebeziumEngineConfiguration debeziumEngineConfiguration) {

        /*
         * creates a debezium engine using the database coordinates taken from Debezium convention (legacy way)
         * in the legacy way we do not support multi-engine
         */
        if (debeziumEngineConfiguration.defaultConfiguration().get(DEBEZIUM_DATASOURCE_HOSTNAME) != null) {
            return createRegistryFromLegacyConfiguration(debeziumEngineConfiguration.defaultConfiguration());
        }

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
                        CaptureGroup captureGroup = new CaptureGroup(engine.groupId());

                        return Map.entry(engine.groupId(), new SourceRecordDebezium(
                                engine.configuration(),
                                stateHandler,
                                POSTGRES,
                                sourceRecordConsumerHandler.get(captureGroup), captureGroup));
                    })
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            @Override
            public Connector connector() {
                return POSTGRES;
            }

            @Override
            public Debezium get(CaptureGroup group) {
                return engines.get(group.id());
            }

            @Override
            public List<Debezium> engines() {
                return engines.values().stream().toList();
            }
        };
    }

    private MultiEngineConfiguration enrichConfiguration(MultiEngineConfiguration engine, Map<String, QuarkusDatasourceConfiguration> collect) {
        HashMap<String, String> mutableMap = new HashMap<>(engine.configuration());

        mutableMap.compute(NOTIFICATION_ENABLED_CHANNELS.name(),
                (key, value) -> value == null ? channel.name() : value.concat("," + channel.name()));

        mutableMap.putAll(getQuarkusDatasourceConfigurationByGroupId(engine.groupId(), collect).asDebezium());
        mutableMap.put(CONNECTOR_CLASS.name(), POSTGRES.name());

        return new MultiEngineConfiguration(engine.groupId(), mutableMap);
    }

    private QuarkusDatasourceConfiguration getQuarkusDatasourceConfigurationByGroupId(String groupId, Map<String, QuarkusDatasourceConfiguration> collect) {
        QuarkusDatasourceConfiguration configuration = collect.get(groupId);

        if (configuration == null) {
            throw new IllegalArgumentException("No datasource configuration found for group " + groupId);
        }

        return configuration;
    }

    private DebeziumConnectorRegistry createRegistryFromLegacyConfiguration(Map<String, String> configuration) {
        configuration.compute(NOTIFICATION_ENABLED_CHANNELS.name(),
                (key, value) -> value == null ? channel.name() : value.concat("," + channel.name()));
        configuration.put(CONNECTOR_CLASS.name(), POSTGRES.name());

        CaptureGroup captureGroup = new CaptureGroup("default");

        return new DebeziumConnectorRegistry() {
            private final SourceRecordDebezium engine = new SourceRecordDebezium(configuration,
                    stateHandler,
                    POSTGRES,
                    sourceRecordConsumerHandler.get(captureGroup), captureGroup);

            @Override
            public Connector connector() {
                return POSTGRES;
            }

            @Override
            public Debezium get(CaptureGroup group) {
                if (group == null || group.id() == null || !group.id().equals("default")) {
                    return null;
                }

                return engine;
            }

            @Override
            public List<Debezium> engines() {
                return List.of(engine);
            }
        };
    }

}
