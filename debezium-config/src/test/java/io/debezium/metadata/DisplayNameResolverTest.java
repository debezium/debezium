/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.metadata;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class DisplayNameResolverTest {

    // Source connectors
    @Test
    void shouldResolveMongoDbSourceConnector() {
        assertThat(DisplayNameResolver.resolve("io.debezium.connector.mongodb.MongoDbConnector", "source-connector"))
                .isEqualTo("Debezium MongoDB Connector");
    }

    @Test
    void shouldResolveMySqlSourceConnector() {
        assertThat(DisplayNameResolver.resolve("io.debezium.connector.mysql.MySqlConnector", "source-connector"))
                .isEqualTo("Debezium MySQL Connector");
    }

    @Test
    void shouldResolveOracleSourceConnector() {
        assertThat(DisplayNameResolver.resolve("io.debezium.connector.oracle.OracleConnector", "source-connector"))
                .isEqualTo("Debezium Oracle Connector");
    }

    @Test
    void shouldResolvePostgresSourceConnector() {
        assertThat(DisplayNameResolver.resolve("io.debezium.connector.postgresql.PostgresConnector", "source-connector"))
                .isEqualTo("Debezium Postgres Connector");
    }

    @Test
    void shouldResolveSqlServerSourceConnector() {
        assertThat(DisplayNameResolver.resolve("io.debezium.connector.sqlserver.SqlServerConnector", "source-connector"))
                .isEqualTo("Debezium SQL Server Connector");
    }

    @Test
    void shouldResolveMariaDbSourceConnector() {
        assertThat(DisplayNameResolver.resolve("io.debezium.connector.mariadb.MariaDbConnector", "source-connector"))
                .isEqualTo("Debezium MariaDB Connector");
    }

    @Test
    void shouldResolveDb2SourceConnector() {
        assertThat(DisplayNameResolver.resolve("io.debezium.connector.db2.Db2Connector", "source-connector"))
                .isEqualTo("Debezium DB2 Connector");
    }

    // Sink connectors
    @Test
    void shouldResolveJdbcSinkConnector() {
        assertThat(DisplayNameResolver.resolve("io.debezium.connector.jdbc.JdbcSinkConnector", "sink-connector"))
                .isEqualTo("Debezium JDBC Sink Connector");
    }

    @Test
    void shouldResolveMongoDbSinkConnector() {
        assertThat(DisplayNameResolver.resolve("io.debezium.connector.mongodb.MongoDbSinkConnector", "sink-connector"))
                .isEqualTo("Debezium MongoDB Sink Connector");
    }

    // Server sinks
    @Test
    void shouldResolveKinesisServerSink() {
        assertThat(DisplayNameResolver.resolve("io.debezium.server.kinesis.KinesisChangeConsumer", "server-sink"))
                .isEqualTo("Debezium Kinesis Server Sink");
    }

    @Test
    void shouldResolveHttpServerSink() {
        assertThat(DisplayNameResolver.resolve("io.debezium.server.http.HttpChangeConsumer", "server-sink"))
                .isEqualTo("Debezium HTTP Server Sink");
    }

    @Test
    void shouldResolveRabbitMqStreamServerSink() {
        assertThat(DisplayNameResolver.resolve("io.debezium.server.rabbitmq.RabbitMqStreamChangeConsumer", "server-sink"))
                .isEqualTo("Debezium RabbitMQ Stream Server Sink");
    }

    @Test
    void shouldResolveRabbitMqStreamNativeServerSink() {
        assertThat(DisplayNameResolver.resolve("io.debezium.server.rabbitmq.RabbitMqStreamNativeChangeConsumer", "server-sink"))
                .isEqualTo("Debezium RabbitMQ Stream Native Server Sink");
    }

    @Test
    void shouldResolveNatsJetStreamServerSink() {
        assertThat(DisplayNameResolver.resolve("io.debezium.server.nats.jetstream.NatsJetStreamChangeConsumer", "server-sink"))
                .isEqualTo("Debezium NATS JetStream Server Sink");
    }

    @Test
    void shouldResolvePubSubServerSink() {
        assertThat(DisplayNameResolver.resolve("io.debezium.server.pubsub.PubSubChangeConsumer", "server-sink"))
                .isEqualTo("Debezium PubSub Server Sink");
    }

    @Test
    void shouldResolvePubSubLiteServerSink() {
        assertThat(DisplayNameResolver.resolve("io.debezium.server.pubsub.PubSubLiteChangeConsumer", "server-sink"))
                .isEqualTo("Debezium PubSub Lite Server Sink");
    }

    @Test
    void shouldResolveInfinispanServerSink() {
        assertThat(DisplayNameResolver.resolve("io.debezium.server.infinispan.InfinispanSinkConsumer", "server-sink"))
                .isEqualTo("Debezium Infinispan Server Sink");
    }

    @Test
    void shouldResolveEventHubsServerSink() {
        assertThat(DisplayNameResolver.resolve("io.debezium.server.eventhubs.EventHubsChangeConsumer", "server-sink"))
                .isEqualTo("Debezium Event Hubs Server Sink");
    }

    @Test
    void shouldResolveRocketMqServerSink() {
        assertThat(DisplayNameResolver.resolve("io.debezium.server.rocketmq.RocketMqChangeConsumer", "server-sink"))
                .isEqualTo("Debezium RocketMQ Server Sink");
    }

    @Test
    void shouldResolveSnsServerSink() {
        assertThat(DisplayNameResolver.resolve("io.debezium.server.sns.SnsChangeConsumer", "server-sink"))
                .isEqualTo("Debezium SNS Server Sink");
    }

    @Test
    void shouldResolveSqsServerSink() {
        assertThat(DisplayNameResolver.resolve("io.debezium.server.sqs.SqsChangeConsumer", "server-sink"))
                .isEqualTo("Debezium SQS Server Sink");
    }

    @Test
    void shouldResolveRedisStreamServerSink() {
        assertThat(DisplayNameResolver.resolve("io.debezium.server.redis.RedisStreamChangeConsumer", "server-sink"))
                .isEqualTo("Debezium Redis Stream Server Sink");
    }

    @Test
    void shouldResolveInstructLabServerSink() {
        assertThat(DisplayNameResolver.resolve("io.debezium.server.instructlab.InstructLabSinkConsumer", "server-sink"))
                .isEqualTo("Debezium InstructLab Server Sink");
    }

    @Test
    void shouldResolveKafkaServerSink() {
        assertThat(DisplayNameResolver.resolve("io.debezium.server.kafka.KafkaChangeConsumer", "server-sink"))
                .isEqualTo("Debezium Kafka Server Sink");
    }

    @Test
    void shouldResolvePulsarServerSink() {
        assertThat(DisplayNameResolver.resolve("io.debezium.server.pulsar.PulsarChangeConsumer", "server-sink"))
                .isEqualTo("Debezium Pulsar Server Sink");
    }

    // Transformations - Debezium
    @Test
    void shouldResolveExtractNewRecordState() {
        assertThat(DisplayNameResolver.resolve("io.debezium.transforms.ExtractNewRecordState", "transformation"))
                .isEqualTo("Debezium Extract New Record State");
    }

    @Test
    void shouldResolveHeaderToValue() {
        assertThat(DisplayNameResolver.resolve("io.debezium.transforms.HeaderToValue", "transformation"))
                .isEqualTo("Debezium Header To Value");
    }

    @Test
    void shouldResolveTimezoneConverter() {
        assertThat(DisplayNameResolver.resolve("io.debezium.transforms.TimezoneConverter", "transformation"))
                .isEqualTo("Debezium Timezone Converter");
    }

    @Test
    void shouldResolveEventRouter() {
        assertThat(DisplayNameResolver.resolve("io.debezium.transforms.outbox.EventRouter", "transformation"))
                .isEqualTo("Debezium Event Router");
    }

    // Transformations - Kafka with inner classes
    @Test
    void shouldResolveKafkaCastValue() {
        assertThat(DisplayNameResolver.resolve("org.apache.kafka.connect.transforms.Cast$Value", "transformation"))
                .isEqualTo("Kafka Cast (Value)");
    }

    @Test
    void shouldResolveKafkaCastKey() {
        assertThat(DisplayNameResolver.resolve("org.apache.kafka.connect.transforms.Cast$Key", "transformation"))
                .isEqualTo("Kafka Cast (Key)");
    }

    @Test
    void shouldResolveKafkaFilter() {
        assertThat(DisplayNameResolver.resolve("org.apache.kafka.connect.transforms.Filter", "transformation"))
                .isEqualTo("Kafka Filter");
    }

    @Test
    void shouldResolveKafkaFlattenValue() {
        assertThat(DisplayNameResolver.resolve("org.apache.kafka.connect.transforms.Flatten$Value", "transformation"))
                .isEqualTo("Kafka Flatten (Value)");
    }

    // Converters
    @Test
    void shouldResolveCloudEventsConverter() {
        assertThat(DisplayNameResolver.resolve("io.debezium.converters.CloudEventsConverter", "converter"))
                .isEqualTo("Debezium Cloud Events Converter");
    }

    @Test
    void shouldResolveKafkaJsonConverter() {
        assertThat(DisplayNameResolver.resolve("org.apache.kafka.connect.json.JsonConverter", "converter"))
                .isEqualTo("Kafka JSON Converter");
    }

    @Test
    void shouldResolveKafkaStringConverter() {
        assertThat(DisplayNameResolver.resolve("org.apache.kafka.connect.storage.StringConverter", "converter"))
                .isEqualTo("Kafka String Converter");
    }

    // Custom converters
    @Test
    void shouldResolveRawToStringCustomConverter() {
        assertThat(DisplayNameResolver.resolve("io.debezium.connector.oracle.converters.RawToStringConverter", "custom-converter"))
                .isEqualTo("Debezium Raw To String Custom Converter");
    }

    @Test
    void shouldResolveTinyIntOneToBooleanCustomConverter() {
        assertThat(DisplayNameResolver.resolve("io.debezium.connector.binlog.converters.TinyIntOneToBooleanConverter", "custom-converter"))
                .isEqualTo("Debezium Tiny Int One To Boolean Custom Converter");
    }

    // Predicates
    @Test
    void shouldResolveHasHeaderKeyPredicate() {
        assertThat(DisplayNameResolver.resolve("org.apache.kafka.connect.transforms.predicates.HasHeaderKey", "predicate"))
                .isEqualTo("Kafka Has Header Key");
    }

    @Test
    void shouldResolveTopicNameMatchesPredicate() {
        assertThat(DisplayNameResolver.resolve("org.apache.kafka.connect.transforms.predicates.TopicNameMatches", "predicate"))
                .isEqualTo("Kafka Topic Name Matches");
    }

    // Unknown type
    @Test
    void shouldHandleUnknownType() {
        assertThat(DisplayNameResolver.resolve("io.debezium.some.CustomComponent", "unknown"))
                .isEqualTo("Debezium Custom Component");
    }

    // CamelCase splitting edge cases
    @Test
    void shouldSplitSimpleCamelCase() {
        assertThat(DisplayNameResolver.splitCamelCase("ExtractNewRecordState"))
                .containsExactly("Extract", "New", "Record", "State");
    }

    @Test
    void shouldSplitAcronymFollowedByWord() {
        assertThat(DisplayNameResolver.splitCamelCase("HTTPSink"))
                .containsExactly("HTTP", "Sink");
    }

    @Test
    void shouldKeepSingleWord() {
        assertThat(DisplayNameResolver.splitCamelCase("Oracle"))
                .containsExactly("Oracle");
    }

    @Test
    void shouldSplitWithTrailingDigit() {
        assertThat(DisplayNameResolver.splitCamelCase("Db2"))
                .containsExactly("Db2");
    }

    // Brand name merging
    @Test
    void shouldMergeMongoDb() {
        assertThat(DisplayNameResolver.mergeBrandNames(java.util.List.of("Mongo", "Db")))
                .containsExactly("MongoDB");
    }

    @Test
    void shouldMergeSqlServer() {
        assertThat(DisplayNameResolver.mergeBrandNames(java.util.List.of("Sql", "Server")))
                .containsExactly("SQL Server");
    }

    @Test
    void shouldMergeBrandWithSurroundingWords() {
        assertThat(DisplayNameResolver.mergeBrandNames(java.util.List.of("Rabbit", "Mq", "Stream")))
                .containsExactly("RabbitMQ", "Stream");
    }

    @Test
    void shouldPreferLongerBrandSequence() {
        assertThat(DisplayNameResolver.mergeBrandNames(java.util.List.of("Nats", "Jet", "Stream")))
                .containsExactly("NATS JetStream");
    }
}
