/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine;

import static io.debezium.config.CommonConnectorConfig.NOTIFICATION_ENABLED_CHANNELS;

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
import io.quarkus.debezium.notification.QuarkusNotificationChannel;

@ApplicationScoped
public class PostgresEngineProducer implements ConnectorProducer {

    public static final String CONNECTOR_CLASS = "connector.class";
    public static final Connector POSTGRES = new Connector("io.debezium.connector.postgresql.PostgresConnector");

    @Inject
    private CapturingInvokerRegistry<RecordChangeEvent<SourceRecord>> registry;

    @Inject
    StateHandler stateHandler;

    @Inject
    QuarkusNotificationChannel channel;

    @Produces
    @Singleton
    public Debezium engine(DebeziumEngineConfiguration debeziumEngineConfiguration) {
        debeziumEngineConfiguration.configuration().put(CONNECTOR_CLASS, POSTGRES.name());
        debeziumEngineConfiguration.configuration().compute(NOTIFICATION_ENABLED_CHANNELS.name(),
                (key, value) -> value == null ? channel.name() : value.concat("," + channel.name()));

        return new SourceRecordDebezium(debeziumEngineConfiguration,
                stateHandler,
                POSTGRES,
                registry);
    }
}
