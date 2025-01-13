/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.e2e;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.storage.ConverterType;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.jdbc.JdbcSinkConnectorConfig;
import io.debezium.connector.jdbc.JdbcSinkConnectorTask;
import io.debezium.connector.jdbc.JdbcSinkTaskTestContext;
import io.debezium.connector.jdbc.junit.TestHelper;
import io.debezium.connector.jdbc.junit.jupiter.e2e.source.Source;
import io.debezium.testing.testcontainers.ConnectorConfiguration;

/**
 * The base abstract class for all JDBC sink connector integration tests.
 *
 * @author Chris Cranford
 */
public abstract class AbstractJdbcSinkIT {

    protected static final Logger LOGGER = LoggerFactory.getLogger(AbstractJdbcSinkIT.class);

    private JsonConverter keyConverter;
    private JsonConverter valueConverter;
    private JdbcSinkConnectorTask sinkTask;
    private KafkaConsumer<byte[], byte[]> consumer;

    private final ConcurrentLinkedQueue<SinkRecord> consumerRecords = new ConcurrentLinkedQueue<>();
    private CountDownLatch stopLatch = new CountDownLatch(1);
    private ExecutorService sinkExecutor;
    private JdbcSinkConnectorConfig currentSinkConfig;
    private TimeZone currentSinkTimeZone;

    @AfterEach
    public void afterEach() throws Exception {
        stopSink();
    }

    protected JdbcSinkConnectorConfig getCurrentSinkConfig() {
        return currentSinkConfig;
    }

    protected TimeZone getCurrentSinkTimeZone() {
        if (currentSinkTimeZone == null) {
            currentSinkTimeZone = TimeZone.getTimeZone(currentSinkConfig.useTimeZone());
        }
        return currentSinkTimeZone;
    }

    protected void startSink(Source source, Properties sinkProperties, String tableName) {
        keyConverter = new JsonConverter();
        keyConverter.configure(Map.of("converter.type", ConverterType.KEY.getName(), "schemas.enable", "true"));

        valueConverter = new JsonConverter();
        valueConverter.configure(Map.of("converter.type", ConverterType.VALUE.getName(), "schemas.enable", "true"));

        // Create sink connector
        sinkTask = new JdbcSinkConnectorTask();

        final Map<String, String> configMap = new HashMap<>();
        sinkProperties.forEach((k, v) -> configMap.put((String) k, (String) v));

        currentSinkConfig = new JdbcSinkConnectorConfig(configMap);

        // Initialize sink task with a mock context
        sinkTask.initialize(new JdbcSinkTaskTestContext(configMap));
        sinkTask.start(configMap);

        // Consumer properties
        Properties consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, source.getKafka().getBootstrapServers());
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, "false");
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "jdbc-sink-consumer");

        consumer = new KafkaConsumer<>(consumerProperties);

        stopLatch = new CountDownLatch(1);
        sinkExecutor = Executors.newFixedThreadPool(1);

        sinkExecutor.submit(new Runnable() {
            @Override
            public void run() {
                // If the topic does not yet exist and the consumer is subscribed, it does not seem to recognize
                // when the topic finally gets created and the consumer does not receive events. So this wait
                // forces the topic to be created by the producer first before starting the consumer.
                final String pattern = "^" + source.getType().getValue() + ".*" + tableName + "$";
                final Pattern regex = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE);
                Awaitility.await("Topic with pattern not created")
                        .atMost(60, TimeUnit.SECONDS)
                        .until(() -> consumer.listTopics().keySet().stream().anyMatch(t -> regex.matcher(t).matches()));

                consumer.subscribe(regex);

                LOGGER.info("KafkaConsumer thread is now polling for records.");
                while (stopLatch.getCount() == 1) {
                    try {
                        ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofSeconds(1));
                        LOGGER.info("Consumer poll returned {} records", records.count());
                        if (!records.isEmpty()) {
                            records.forEach(r -> consumerRecords.add(getSinkRecordFromConsumerRecord(r)));
                        }
                    }
                    catch (Exception e) {
                        LOGGER.error("Connector failed", e);
                        break;
                    }
                }

                LOGGER.info("Unsubscribing from KafkaConsumer and closing consumer.");
                consumer.unsubscribe();
                consumer.close();
            }
        });
    }

    protected void stopSink() throws Exception {
        if (sinkExecutor != null) {
            stopLatch.countDown();
            sinkExecutor.shutdown();
            sinkExecutor.awaitTermination(60, TimeUnit.SECONDS);

            sinkExecutor = null;
            stopLatch = null;
        }

        if (sinkTask != null) {
            sinkTask.stop();
            sinkTask = null;
        }
    }

    protected SinkRecord consumeSinkRecord() throws Exception {
        return consumeSinkRecords(1).get(0);
    }

    protected List<SinkRecord> consumeSinkRecords(int numRecords) throws Exception {
        final List<SinkRecord> records = new ArrayList<>();

        Awaitility.await("Expected to receive " + numRecords + " from source connector")
                .atMost(Duration.ofMinutes(1))
                .until(() -> {
                    if (consumerRecords.size() >= numRecords) {
                        for (int i = 0; i < numRecords; ++i) {
                            records.add(consumerRecords.poll());
                        }
                        return true;
                    }
                    return false;
                });

        sinkTask.put(records);
        if (sinkTask.getLastProcessingException() != null) {
            throw new RuntimeException("JDBC sink throw an exception processing the data", sinkTask.getLastProcessingException());
        }

        return records;
    }

    protected SinkRecord getSinkRecordFromConsumerRecord(ConsumerRecord<byte[], byte[]> record) {
        SchemaAndValue valueSchemaAndValue = valueConverter.toConnectData(record.topic(), record.value());
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

    protected ConnectorConfiguration getSourceConnectorConfig(Source source, String tableName) {
        // Common connector configuration
        final ConnectorConfiguration sourceConfig = ConnectorConfiguration.create();
        sourceConfig.with("tasks.max", 1);
        sourceConfig.with("key.converter", JsonConverter.class.getName());
        sourceConfig.with("value.converter", JsonConverter.class.getName());
        sourceConfig.with("topic.prefix", source.getType().getValue());
        sourceConfig.with("database.hostname", source.getType().getValue());
        sourceConfig.with("key.converter.schemas.enabled", "true");
        sourceConfig.with("value.converter.schemas.enabled", "true");

        // NOTE:
        // We always explicitly test with DecimalHandlingMode.DOUBLE unless overridden by the test.
        // Since changing this means that these numeric values are delivered as STRING values and
        // therefore interpreted as such, this has little impact on the numeric evaluation.
        sourceConfig.with("decimal.handling.mode", "double");

        // NOTE:
        // MySQL does not support ADAPTIVE precision mode and SourcePipelineInvocationContextProvider
        // explicitly removes that precision mode from the test matrix when the source is MySQL, but
        // keeps that option for other connector types.
        sourceConfig.with("time.precision.mode", source.getOptions().getTemporalPrecisionMode().getValue());

        if (source.getOptions().isFlatten()) {
            sourceConfig.with("transforms", "flat");
            sourceConfig.with("transforms.flat.type", "io.debezium.transforms.ExtractNewRecordState");
            sourceConfig.with("transforms.flat.drop.tombstones", "false");
        }

        switch (source.getType()) {
            case MYSQL:
                sourceConfig.with("connector.class", "io.debezium.connector.mysql.MySqlConnector");
                sourceConfig.with("database.password", source.getPassword());
                sourceConfig.with("database.user", "root");
                sourceConfig.with("database.server.id", 12345);
                sourceConfig.with("database.include.list", "test");
                sourceConfig.with("table.include.list", "test." + tableName);
                sourceConfig.with("schema.history.internal.kafka.bootstrap.servers", "kafka:9092");
                sourceConfig.with("schema.history.internal.kafka.topic", "schema-history-mysql");
                sourceConfig.with("schema.history.internal.store.only.captured.tables.ddl", "true");
                if (TestHelper.isConnectionTimeZoneUsed()) {
                    sourceConfig.with("driver.connectionTimeZone", TestHelper.getSourceTimeZone());
                    sourceConfig.with("driver.serverTimeZone", TestHelper.getSourceTimeZone());
                }
                if (source.getOptions().isColumnTypePropagated()) {
                    sourceConfig.with("column.propagate.source.type", "test.*");
                }
                break;
            case POSTGRES:
                sourceConfig.with("connector.class", "io.debezium.connector.postgresql.PostgresConnector");
                sourceConfig.with("database.password", source.getPassword());
                sourceConfig.with("database.user", source.getUsername());
                sourceConfig.with("database.dbname", "test");
                sourceConfig.with("slot.drop.on.stop", "true");
                sourceConfig.with("schema.include.list", "public");
                sourceConfig.with("table.include.list", "public." + tableName);
                if (source.getOptions().isColumnTypePropagated()) {
                    sourceConfig.with("column.propagate.source.type", "public.*");
                }
                break;
            case SQLSERVER:
                sourceConfig.with("connector.class", "io.debezium.connector.sqlserver.SqlServerConnector");
                sourceConfig.with("database.password", source.getPassword());
                sourceConfig.with("database.user", source.getUsername());
                sourceConfig.with("database.names", "testDB");
                sourceConfig.with("database.encrypt", "false");
                sourceConfig.with("schema.history.internal.kafka.bootstrap.servers", "kafka:9092");
                sourceConfig.with("schema.history.internal.kafka.topic", "schema-history-sqlserver");
                sourceConfig.with("schema.history.internal.store.only.captured.tables.ddl", "true");
                sourceConfig.with("table.include.list", "dbo." + tableName);
                if (source.getOptions().isColumnTypePropagated()) {
                    sourceConfig.with("column.propagate.source.type", ".*");
                }
                break;
            case ORACLE:
                sourceConfig.with("connector.class", "io.debezium.connector.oracle.OracleConnector");
                sourceConfig.with("database.dbname", "ORCLCDB");
                sourceConfig.with("database.pdb.name", "ORCLPDB1");
                sourceConfig.with("database.port", "1521");
                sourceConfig.with("database.password", "dbz");
                sourceConfig.with("database.user", "c##dbzuser");
                sourceConfig.with("table.include.list", "debezium." + tableName);
                sourceConfig.with("log.mining.strategy", "online_catalog");
                sourceConfig.with("schema.history.internal.kafka.bootstrap.servers", "kafka:9092");
                sourceConfig.with("schema.history.internal.kafka.topic", "schema-history-oracle");
                sourceConfig.with("schema.history.internal.store.only.captured.tables.ddl", "true");
                if (source.getOptions().isColumnTypePropagated()) {
                    sourceConfig.with("column.propagate.source.type", "debezium.*");
                }
                break;
            default:
                throw new IllegalStateException("Unsupported source type: " + source.getType());
        }

        return sourceConfig;
    }

}
