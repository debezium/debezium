/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.engine.RecordChangeEvent;
import io.debezium.runtime.Connector;
import io.debezium.runtime.ConnectorProducer;
import io.debezium.runtime.Debezium;
import io.debezium.runtime.configuration.DebeziumEngineConfiguration;
import io.quarkus.debezium.engine.capture.CapturingInvokerRegistry;

@ApplicationScoped
public class PostgresEngineProducer implements ConnectorProducer {

    public static final String CONNECTOR_CLASS = "connector.class";
    public static final Connector POSTGRES = new Connector("io.debezium.connector.postgresql.PostgresConnector");

    @Inject
    private CapturingInvokerRegistry<RecordChangeEvent<SourceRecord>> registry;

    @Inject
    StateHandler stateHandler;

    @Produces
    @Singleton
    public Debezium engine(DebeziumEngineConfiguration debeziumEngineConfiguration) {
        debeziumEngineConfiguration.configuration().put(CONNECTOR_CLASS, POSTGRES.name());

        return new SourceRecordDebezium(debeziumEngineConfiguration,
                stateHandler,
                POSTGRES,
                registry);
    }
}
