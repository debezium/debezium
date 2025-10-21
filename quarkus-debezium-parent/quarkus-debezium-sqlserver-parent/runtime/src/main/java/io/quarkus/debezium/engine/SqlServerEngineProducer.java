/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import io.debezium.connector.sqlserver.SqlServerConnector;
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

public class SqlServerEngineProducer implements ConnectorProducer {
    public static final Connector SQLSERVER = new Connector(SqlServerConnector.class.getName());

    private final StateHandler stateHandler;
    private final SourceRecordConsumerHandler sourceRecordConsumerHandler;
    private final AgroalParser agroalParser;

    @Inject
    public SqlServerEngineProducer(StateHandler stateHandler,
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
        List<MultiEngineConfiguration> multiEngineConfigurations = agroalParser.parse(debeziumEngineConfiguration, DatabaseKind.MSSQL, SQLSERVER);

        return new DebeziumConnectorRegistry() {
            private final Map<String, Debezium> engines = multiEngineConfigurations
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

}
