/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.cockroachdb;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;

import org.junit.Test;

import io.debezium.config.Configuration;

/**
 * Unit tests for {@link CockroachDBConnectorConfig}.
 *
 * @author Virag Tripathi
 */
public class CockroachDBConnectorConfigTest {

    @Test
    public void shouldLoadDefaults() {
        Configuration config = Configuration.create()
                .with("name", "test-connector")
                .with("database.hostname", "localhost")
                .with("database.port", 26257)
                .with("database.user", "root")
                .with("database.password", "secret")
                .with("database.dbname", "defaultdb")
                .with("database.server.name", "crdb")
                .with("kafka.bootstrap.servers", "localhost:9092")
                .with("changefeed.topic", "crdb-changes")
                .build();

        CockroachDBConnectorConfig connectorConfig = new CockroachDBConnectorConfig(config);

        assertThat(connectorConfig.getChangefeedTopic()).isEqualTo("crdb-changes");
        assertThat(connectorConfig.getKafkaBootstrapServers()).isEqualTo("localhost:9092");
        assertThat(connectorConfig.getChangefeedFormat()).isEqualTo("json"); // default
        assertThat(connectorConfig.getPollInterval()).isEqualTo(Duration.ofMillis(1000)); // default
        assertThat(connectorConfig.getSchemaRegistryUrl()).isNull(); // not required unless using Avro
        assertThat(connectorConfig.getLogicalName()).isEqualTo("crdb");
    }

    @Test
    public void shouldUseAvroFormatIfConfigured() {
        Configuration config = Configuration.create()
                .with("name", "test-connector")
                .with("database.hostname", "localhost")
                .with("database.port", 26257)
                .with("database.user", "root")
                .with("database.password", "secret")
                .with("database.dbname", "defaultdb")
                .with("database.server.name", "crdb")
                .with("kafka.bootstrap.servers", "localhost:9092")
                .with("changefeed.topic", "crdb-changes")
                .with("changefeed.format", "avro")
                .with("schema.registry.url", "http://localhost:8081")
                .build();

        CockroachDBConnectorConfig connectorConfig = new CockroachDBConnectorConfig(config);

        assertThat(connectorConfig.getChangefeedFormat()).isEqualTo("avro");
        assertThat(connectorConfig.getSchemaRegistryUrl()).isEqualTo("http://localhost:8081");
    }
}
