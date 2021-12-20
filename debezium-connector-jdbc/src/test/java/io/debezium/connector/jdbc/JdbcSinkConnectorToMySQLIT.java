/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc;

import static io.debezium.connector.jdbc.SourcePipelineInvocationContextProvider.DATABASE_CONTAINER_NAME;
import static io.debezium.connector.jdbc.SourcePipelineInvocationContextProvider.KAFKA_CONNECT_CONTAINER_NAME;
import static io.debezium.connector.jdbc.SourcePipelineInvocationContextProvider.KAFKA_CONTAINER_NAME;
import static io.debezium.connector.jdbc.SourcePipelineInvocationContextProvider.getNetwork;
import static org.assertj.db.api.Assertions.assertThat;
import static org.testcontainers.containers.output.OutputFrame.OutputType.STDOUT;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.connect.sink.SinkRecord;
import org.assertj.db.type.Table;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.output.WaitingConsumer;
import org.testcontainers.utility.DockerImageName;

import com.mysql.cj.jdbc.MysqlDataSource;

import io.debezium.testing.testcontainers.Connector;
import io.debezium.testing.testcontainers.ConnectorConfiguration;
import io.debezium.testing.testcontainers.DebeziumContainer;

public class JdbcSinkConnectorToMySQLIT extends AbstractPipelineIT {
    private static Logger LOGGER = LoggerFactory.getLogger(JdbcSinkConnectorToMySQLIT.class);
    private static DockerImageName MYSQL_IMAGE = DockerImageName.parse("mysql");

    private static MySQLContainer<?> sinkDatabaseContainer = new MySQLContainer<>(MYSQL_IMAGE).withNetwork(getNetwork())
            .withDatabaseName("test");

    @BeforeEach
    void before() {
        sinkDatabaseContainer.start();
    }

    @AfterEach
    void after() {
        sinkDatabaseContainer.stop();
    }

    @TestTemplate
    void createSimplePipelineFromSourceToSink(Map<String, GenericContainer<?>> containerMap) throws TimeoutException, SQLException {
        KafkaContainer kafkaContainer = (KafkaContainer) containerMap.get(KAFKA_CONTAINER_NAME);
        DebeziumContainer kafkaConnectContainer = (DebeziumContainer) containerMap.get(KAFKA_CONNECT_CONTAINER_NAME);
        JdbcDatabaseContainer sourceDatabaseContainer = (JdbcDatabaseContainer) containerMap.get(DATABASE_CONTAINER_NAME);
        String databaseName = getDatabaseName(sourceDatabaseContainer);

        // Wait for db initialization
        WaitingConsumer dbInitializeWait = new WaitingConsumer();
        sourceDatabaseContainer.followOutput(dbInitializeWait, STDOUT);
        dbInitializeWait.waitUntil(outputFrame -> outputFrame.getUtf8String().toLowerCase(Locale.ROOT).contains("ready for start up"), (int) TIMEOUT.getSeconds(),
                TimeUnit.SECONDS);

        // Register SourceConnector
        String sourceConnectorName = String.format("source-%s", databaseName);
        ConnectorConfiguration sourceConnectorConfiguration = getCommonSourceConnectorConfiguration(sourceDatabaseContainer);
        kafkaConnectContainer.registerConnector(
                sourceConnectorName,
                sourceConnectorConfiguration);

        kafkaConnectContainer.ensureConnectorState(sourceConnectorName, Connector.State.RUNNING);
        kafkaConnectContainer.ensureConnectorTaskState(sourceConnectorName, 0, Connector.State.RUNNING);

        // Initialize and start sink connector
        JdbcSinkConnectorTask sinkTask = new JdbcSinkConnectorTask();
        Map<String, String> sinkConnectorProperties = getSinkConnectorProperties(sinkDatabaseContainer);
        sinkTask.start(sinkConnectorProperties);

        // Wait to complete snapshot
        WaitingConsumer snapshotWaitConsumer = new WaitingConsumer();
        kafkaConnectContainer.followOutput(snapshotWaitConsumer, STDOUT);
        snapshotWaitConsumer.waitUntil(
                outputFrame -> outputFrame.getUtf8String().contains(String.format("%s|snapshot  Snapshot ended with SnapshotResult", databaseName)),
                (int) TIMEOUT.getSeconds(), TimeUnit.SECONDS);

        // CreateConsumer
        Properties consumerProperties = getCommonConsumerConfiguration(kafkaContainer);
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, sourceConnectorName);
        Pattern topicsPattern = Pattern.compile(String.format("%s.inventory.*", databaseName));
        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProperties);
        consumer.subscribe(topicsPattern);

        // Consumer and put data into sink connector
        List<SinkRecord> sinkRecords = new ArrayList<>();

        // True size of whole `inventory` database
        consumer.poll(TIMEOUT).forEach((ConsumerRecord<byte[], byte[]> record) -> {
            SinkRecord sinkRecord = getSinkRecord(record);
            sinkRecords.add(sinkRecord);
        });
        sinkTask.put(sinkRecords);

        MysqlDataSource dataSource = new MysqlDataSource();
        dataSource.setURL(sinkDatabaseContainer.getJdbcUrl());
        dataSource.setUser(sinkDatabaseContainer.getUsername());
        dataSource.setPassword(sinkDatabaseContainer.getPassword());

        // test inventory_addresses
        Table inventory_addresses = new Table(dataSource, String.format("%s_inventory_addresses", databaseName));
        assertThat(inventory_addresses)
                .exists()
                .column().hasColumnName("dbz_id").isNumber(false)
                .column().hasColumnName("id").isNumber(false)
                .column().hasColumnName("customer_id").isNumber(false)
                .column().hasColumnName("street").isText(false)
                .column().hasColumnName("city").isText(false)
                .column().hasColumnName("state").isText(false)
                .column().hasColumnName("zip").isText(false)
                .column().hasColumnName("type").isText(false)
                .row().hasValues(1, 10, 1001, "3183 Moore Avenue", "Euless", "Texas", "76036", "SHIPPING")
                .row().hasValues(2, 11, 1001, "2389 Hidden Valley Road", "Harrisburg", "Pennsylvania", "17116", "BILLING")
                .row().hasValues(3, 12, 1002, "281 Riverside Drive", "Augusta", "Georgia", "30901", "BILLING")
                .row().hasValues(4, 13, 1003, "3787 Brownton Road", "Columbus", "Mississippi", "39701", "SHIPPING")
                .row().hasValues(5, 14, 1003, "2458 Lost Creek Road", "Bethlehem", "Pennsylvania", "18018", "SHIPPING")
                .row().hasValues(6, 15, 1003, "4800 Simpson Square", "Hillsdale", "Oklahoma", "73743", "BILLING")
                .row().hasValues(7, 16, 1004, "1289 University Hill Road", "Canehill", "Arkansas", "72717", "LIVING");

    }
}
