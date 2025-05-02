/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.quarkus.debezium.producer;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;

import io.quarkus.debezium.configuration.DebeziumEngineConfiguration;
import io.quarkus.debezium.engine.Debezium;
import io.quarkus.debezium.engine.SourceRecordDebezium;

@ApplicationScoped
public class PostgresEngineProducer {

    public static final String CONNECTOR_CLASS = "connector.class";
    public static final String POSTGRES_CONNECTOR = "io.debezium.connector.postgresql.PostgresConnector";

    @Produces
    @Singleton
    public Debezium engine(DebeziumEngineConfiguration debeziumEngineConfiguration) {
        debeziumEngineConfiguration.configuration().put(CONNECTOR_CLASS, POSTGRES_CONNECTOR);

        return new SourceRecordDebezium(debeziumEngineConfiguration);
    }
}
