/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine;

import static io.debezium.config.CommonConnectorConfig.DATABASE_CONFIG_PREFIX;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import io.debezium.connector.mariadb.MariaDbConnector;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.runtime.Connector;
import io.debezium.runtime.ConnectorProducer;
import io.debezium.runtime.Debezium;
import io.debezium.runtime.DebeziumConnectorRegistry;
import io.debezium.runtime.EngineManifest;
import io.debezium.runtime.configuration.DebeziumEngineConfiguration;
import io.quarkus.datasource.common.runtime.DatabaseKind;
import io.quarkus.debezium.agroal.engine.AgroalParser;
import io.quarkus.debezium.configuration.DebeziumConfigurationEngineParser.MultiEngineConfiguration;
import io.quarkus.debezium.engine.capture.consumer.SourceRecordConsumerHandler;

public class MariaDbEngineProducer implements ConnectorProducer {
    public static final Connector MARIADB = new Connector(MariaDbConnector.class.getName());

    private final StateHandler stateHandler;
    private final SourceRecordConsumerHandler sourceRecordConsumerHandler;
    private final AgroalParser agroalParser;

    @Inject
    public MariaDbEngineProducer(StateHandler stateHandler,
                                 SourceRecordConsumerHandler sourceRecordConsumerHandler,
                                 AgroalParser agroalParser) {
        this.stateHandler = stateHandler;
        this.sourceRecordConsumerHandler = sourceRecordConsumerHandler;
        this.agroalParser = agroalParser;
    }

    @Produces
    @Singleton
    @Override
    public DebeziumConnectorRegistry engine(DebeziumEngineConfiguration debeziumEngineConfiguration) {
        List<MultiEngineConfiguration> multiEngineConfigurations = agroalParser.parse(debeziumEngineConfiguration, DatabaseKind.MARIADB, MARIADB);

        return new DebeziumConnectorRegistry() {
            private final Map<String, Debezium> engines = multiEngineConfigurations
                    .stream()
                    .map(engine -> {
                        EngineManifest engineManifest = new EngineManifest(engine.engineId());

                        Map<String, String> debeziumConfiguration = engine.configuration();

                        // remove unnecessary configuration for sqlserver
                        debeziumConfiguration.remove(DATABASE_CONFIG_PREFIX + JdbcConfiguration.DATABASE.name());

                        return Map.entry(engine.engineId(), new SourceRecordDebezium(
                                engine.configuration(),
                                stateHandler,
                                MARIADB,
                                sourceRecordConsumerHandler.get(engineManifest), engineManifest));
                    })
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            @Override
            public Connector connector() {
                return MARIADB;
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
}
