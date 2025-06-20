/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.cockroachdb;

import java.time.Duration;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.relational.RelationalDatabaseConnectorConfig;

/**
 * Configuration options for the CockroachDB Debezium connector.
 * Includes support for parsing enriched changefeed messages from Kafka.
 *
 * @author Virag Tripathi
 */
public class CockroachDBConnectorConfig extends RelationalDatabaseConnectorConfig {

    public static final String CHANGEFEED_TOPIC_NAME = "changefeed.topic";
    public static final String KAFKA_BOOTSTRAP_SERVERS = "kafka.bootstrap.servers";
    public static final String SCHEMA_REGISTRY_URL = "schema.registry.url";
    public static final String CHANGEFEED_FORMAT = "changefeed.format";
    public static final String POLL_INTERVAL_MS = "changefeed.poll.interval.ms";

    public static final Field CHANGEFEED_TOPIC = Field.create(CHANGEFEED_TOPIC_NAME)
            .withDisplayName("CockroachDB Changefeed Kafka Topic")
            .withType(Type.STRING)
            .withImportance(Importance.HIGH)
            .withDescription("Kafka topic where CockroachDB writes enriched changefeed messages.");

    public static final Field BOOTSTRAP_SERVERS = Field.create(KAFKA_BOOTSTRAP_SERVERS)
            .withDisplayName("Kafka Bootstrap Servers")
            .withType(Type.STRING)
            .withImportance(Importance.HIGH)
            .withDescription("Comma-separated list of Kafka bootstrap servers to connect to.");

    public static final Field FORMAT = Field.create(CHANGEFEED_FORMAT)
            .withDisplayName("Changefeed Message Format")
            .withType(Type.STRING)
            .withDefault("json")
            .withImportance(Importance.MEDIUM)
            .withDescription("Changefeed format used by CockroachDB. Supported: 'json', 'avro'.");

    public static final Field SCHEMA_REGISTRY = Field.create(SCHEMA_REGISTRY_URL)
            .withDisplayName("Schema Registry URL")
            .withType(Type.STRING)
            .withImportance(Importance.MEDIUM)
            .withDescription("Schema registry URL for Avro deserialization (if changefeed.format=avro).");

    public static final Field POLL_INTERVAL = Field.create(POLL_INTERVAL_MS)
            .withDisplayName("Kafka Poll Interval (ms)")
            .withType(Type.INT)
            .withDefault(1000)
            .withImportance(Importance.LOW)
            .withDescription("Interval (in milliseconds) between Kafka consumer polls.");

    protected static final ConfigDef CONFIG_DEF = RelationalDatabaseConnectorConfig.CONFIG_DEF
            .define(CHANGEFEED_TOPIC)
            .define(BOOTSTRAP_SERVERS)
            .define(FORMAT)
            .define(SCHEMA_REGISTRY)
            .define(POLL_INTERVAL);

    public CockroachDBConnectorConfig(Configuration config) {
        super(config, CONFIG_DEF);
    }

    public String getChangefeedTopic() {
        return getString(CHANGEFEED_TOPIC);
    }

    public String getKafkaBootstrapServers() {
        return getString(BOOTSTRAP_SERVERS);
    }

    public String getSchemaRegistryUrl() {
        return getString(SCHEMA_REGISTRY);
    }

    public String getChangefeedFormat() {
        return getString(FORMAT);
    }

    public Duration getPollInterval() {
        return Duration.ofMillis(getInteger(POLL_INTERVAL));
    }

    @Override
    public String getConnectorName() {
        return "cockroachdb";
    }
}
