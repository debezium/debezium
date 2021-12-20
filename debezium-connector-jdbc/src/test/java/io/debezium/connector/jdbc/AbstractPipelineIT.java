/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc;

import static io.debezium.connector.jdbc.JdbcSinkConnectorConfig.CONNECTION_PASSWORD;
import static io.debezium.connector.jdbc.JdbcSinkConnectorConfig.CONNECTION_URL;
import static io.debezium.connector.jdbc.JdbcSinkConnectorConfig.CONNECTION_USER;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.storage.ConverterType;
import org.apache.kafka.connect.transforms.Transformation;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.KafkaContainer;

import io.debezium.testing.testcontainers.ConnectorConfiguration;
import io.debezium.transforms.ExtractNewRecordState;

@ExtendWith(SourcePipelineInvocationContextProvider.class)
abstract class AbstractPipelineIT {
    public static String DATABASE_HISTORY_KAFKA_TOPIC = "schema-changes.inventory";
    public static Duration TIMEOUT = Duration.ofSeconds(60);

    public final JsonConverter keyConverter = new JsonConverter();
    public final JsonConverter valueConverter = new JsonConverter();
    public final Transformation<SinkRecord> extractNewRecordState = new ExtractNewRecordState<>();

    public AbstractPipelineIT() {
        Map<String, String> keyConverterConfig = new HashMap<>();
        keyConverterConfig.put("converter.type", ConverterType.KEY.getName());

        Map<String, String> valueConverterConfig = new HashMap<>();
        valueConverterConfig.put("converter.type", ConverterType.VALUE.getName());

        keyConverter.configure(keyConverterConfig);
        valueConverter.configure(valueConverterConfig);

        extractNewRecordState.configure(new HashMap<>());
    }

    public static ConnectorConfiguration getCommonSourceConnectorConfiguration(JdbcDatabaseContainer<?> databaseContainer) {

        ConnectorConfiguration conf = ConnectorConfiguration.create();
        String databaseName = getDatabaseName(databaseContainer);

        conf.with("connector.class", getSourceConnectorClassName(databaseName));
        conf.with("tasks.max", 1);
        conf.with("database.history.kafka.topic", String.format("%s.%s", databaseName, DATABASE_HISTORY_KAFKA_TOPIC));
        conf.with("database.history.kafka.bootstrap.servers", "kafka:9092");
        conf.with("key.converter", JsonConverter.class.getName());
        conf.with("value.converter", JsonConverter.class.getName());
        conf.with("database.password", databaseContainer.getPassword());
        conf.with("database.server.id", databaseName.length()); // dummy database.server.id
        conf.with("database.server.name", databaseName);
        conf.with("database.hostname", databaseName);

        switch (databaseName) {
            case "postgres":
                conf.with("database.dbname", "test");
                conf.with("schema.include.list", "inventory");
                conf.with("database.user", databaseContainer.getUsername());
                conf.with("table.exclude.list", "postgres_inventory_spatial_ref_sys");
                break;
            case "mysql":
                conf.with("database.whitelist", "inventory");
                conf.with("database.user", "root");
                break;
        }

        return conf;
    }

    public static String getDatabaseName(JdbcDatabaseContainer<?> databaseContainer) {
        switch (databaseContainer.getDriverClassName()) {
            case "com.mysql.cj.jdbc.Driver":
            case "com.mysql.jdbc.Driver":
                return "mysql";
            case "org.postgresql.Driver":
                return "postgres";
            default:
                throw new IllegalStateException("Unexpected value: " + databaseContainer.getDriverClassName());
        }
    }

    public static Properties getCommonConsumerConfiguration(KafkaContainer kafkaContainer) {
        final Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

        return props;
    }

    public static Map<String, String> getSinkConnectorProperties(JdbcDatabaseContainer<?> databaseContainer) {
        Map<String, String> sinkConnectorProperties = new HashMap<>();
        sinkConnectorProperties.put(CONNECTION_URL, databaseContainer.getJdbcUrl());
        sinkConnectorProperties.put(CONNECTION_USER, databaseContainer.getUsername());
        sinkConnectorProperties.put(CONNECTION_PASSWORD, databaseContainer.getPassword());

        return sinkConnectorProperties;
    }

    public SinkRecord getSinkRecord(ConsumerRecord<byte[], byte[]> record) {
        SchemaAndValue valueSchemaAndValue = keyConverter.toConnectData(record.topic(), record.value());
        SchemaAndValue keySchemaAndValue = keyConverter.toConnectData(record.topic(), record.key());

        return new SinkRecord(
                record.topic(),
                record.partition(),
                keySchemaAndValue.schema(),
                keySchemaAndValue.value(),
                valueSchemaAndValue.schema(),
                valueSchemaAndValue.value(),
                record.offset());

    }

    private static String getSourceConnectorClassName(String databaseName) {
        switch (databaseName) {
            case "mysql":
                return "io.debezium.connector.mysql.MySqlConnector";
            case "postgres":
                return "io.debezium.connector.postgresql.PostgresConnector";
            default:
                throw new IllegalStateException("Unexpected value: " + databaseName);
        }
    }
}
