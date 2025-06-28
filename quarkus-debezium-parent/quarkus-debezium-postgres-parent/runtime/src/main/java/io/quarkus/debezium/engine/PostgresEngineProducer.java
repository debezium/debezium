/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine;

import static io.debezium.config.CommonConnectorConfig.DATABASE_CONFIG_PREFIX;
import static io.debezium.config.CommonConnectorConfig.NOTIFICATION_ENABLED_CHANNELS;
import static io.debezium.embedded.EmbeddedEngineConfig.CONNECTOR_CLASS;
import static java.util.Collections.emptyMap;

import java.util.Map;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.connector.postgresql.PostgresConnector;
import io.debezium.engine.RecordChangeEvent;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.runtime.Connector;
import io.debezium.runtime.ConnectorProducer;
import io.debezium.runtime.Debezium;
import io.debezium.runtime.configuration.DebeziumEngineConfiguration;
import io.quarkus.debezium.configuration.PostgresDatasourceConfiguration;
import io.quarkus.debezium.engine.capture.CapturingInvokerRegistry;
import io.quarkus.debezium.notification.QuarkusNotificationChannel;

@ApplicationScoped
public class PostgresEngineProducer implements ConnectorProducer {

    public static final Connector POSTGRES = new Connector(PostgresConnector.class.getName());
    public static final String DEBEZIUM_DATASOURCE_HOSTNAME = DATABASE_CONFIG_PREFIX + JdbcConfiguration.HOSTNAME.name();

    private final CapturingInvokerRegistry<RecordChangeEvent<SourceRecord>> registry;
    private final StateHandler stateHandler;
    private final Instance<PostgresDatasourceConfiguration> configurations;
    private final QuarkusNotificationChannel channel;

    @Inject
    public PostgresEngineProducer(CapturingInvokerRegistry<RecordChangeEvent<SourceRecord>> registry,
                                  StateHandler stateHandler,
                                  Instance<PostgresDatasourceConfiguration> configurations, QuarkusNotificationChannel channel) {
        this.registry = registry;
        this.stateHandler = stateHandler;
        this.configurations = configurations;
        this.channel = channel;
    }

    @Produces
    @Singleton
    public Debezium engine(DebeziumEngineConfiguration debeziumEngineConfiguration) {
        Map<String, String> configurationMap = debeziumEngineConfiguration.configuration();

        configurationMap.compute(NOTIFICATION_ENABLED_CHANNELS.name(),
                (key, value) -> value == null ? channel.name() : value.concat("," + channel.name()));
        configurationMap.put(CONNECTOR_CLASS.name(), POSTGRES.name());

        if (configurationMap.get(DEBEZIUM_DATASOURCE_HOSTNAME) != null) {
            return new SourceRecordDebezium(configurationMap,
                    stateHandler,
                    POSTGRES,
                    registry);
        }

        /**
         * it's possible to manage multiple configurations and multiple Debezium Instances
         * the {@link engine(DebeziumEngineConfiguration debeziumEngineConfiguration)} should return
         * a registry of Debezium instances
         */
        configurationMap.putAll(configurations
                .stream()
                .findFirst()
                .map(PostgresDatasourceConfiguration::asDebezium)
                .orElse(emptyMap()));

        return new SourceRecordDebezium(configurationMap,
                stateHandler,
                POSTGRES,
                registry);
    }
}
