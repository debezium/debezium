/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.testcontainers;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.kafka.KafkaClusterUtils;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.relational.history.SchemaHistoryMetrics;
import io.debezium.storage.kafka.history.KafkaSchemaHistory;
import io.debezium.util.Collect;
import io.debezium.util.Testing;
import io.strimzi.test.container.StrimziKafkaCluster;

/**
 * Workload test for {@link KafkaSchemaHistory} validating the buffered write
 * mode used during snapshot schema persistence (debezium/dbz#2047).
 * <p>
 * Writes 5000 DDL records (10 databases x 500 tables)
 * <p>
 * Two scenarios are tested: a single broker with acks=1 as a baseline, and
 * 3 brokers with acks=all and min.insync.replicas=2 to simulate production-like
 * replication overhead.
 */
public class KafkaSchemaHistoryWorkloadIT {

    private static final int NUM_DATABASES = 10;
    private static final int TABLES_PER_DATABASE = 500;
    private static final int TOTAL_RECORDS = NUM_DATABASES * TABLES_PER_DATABASE;

    private static StrimziKafkaCluster kafkaCluster;

    private KafkaSchemaHistory history;

    private final Map<String, Object> source = Collect.linkMapOf("server", "workload-server");

    @BeforeAll
    static void startKafka() {
        Map<String, String> props = new HashMap<>();
        props.put("auto.create.topics.enable", "false");

        kafkaCluster = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
                .withNumberOfBrokers(3)
                .withAdditionalKafkaConfiguration(props)
                .withSharedNetwork()
                .build();

        kafkaCluster.start();
    }

    @AfterAll
    static void stopKafka() {
        if (kafkaCluster != null) {
            kafkaCluster.stop();
        }
    }

    @BeforeEach
    void beforeEach() {
        history = new KafkaSchemaHistory();
    }

    @AfterEach
    void afterEach() {
        try {
            if (history != null) {
                history.stop();
            }
        }
        finally {
            history = null;
        }
    }

    @Test
    @DisplayName("Buffered schema history writes with single broker (acks=1)")
    void shouldMeasureSchemaHistoryBatchWritePerformance() throws Exception {
        String topicName = "workload-schema-history-" + UUID.randomUUID().toString().substring(0, 8);

        KafkaClusterUtils.createTopic(topicName, 1, (short) 1, kafkaCluster.getBootstrapServers());

        Configuration config = historyConfig(topicName).build();

        runSnapshotWorkload(config, "1 broker, acks=1");
    }

    @Test
    @DisplayName("Buffered schema history writes with 3 brokers (acks=all, min.insync.replicas=2)")
    void shouldMeasureSchemaHistoryBatchWritePerformanceWithReplication() throws Exception {
        String topicName = "workload-replicated-" + UUID.randomUUID().toString().substring(0, 8);

        Map<String, String> topicConfigs = new HashMap<>();
        topicConfigs.put("min.insync.replicas", "2");

        createReplicatedTopicWithConfig(topicName, topicConfigs);

        Configuration config = historyConfig(topicName)
                .with("schema.history.internal.producer." + ProducerConfig.ACKS_CONFIG, "all")
                .build();

        runSnapshotWorkload(config, "3 brokers, acks=all, min.insync.replicas=2");
    }

    private Configuration.Builder historyConfig(String topicName) {
        return Configuration.create()
                .with(KafkaSchemaHistory.BOOTSTRAP_SERVERS, kafkaCluster.getBootstrapServers())
                .with(KafkaSchemaHistory.TOPIC, topicName)
                .with(SchemaHistory.NAME, "workload-db-history")
                .with(KafkaSchemaHistory.RECOVERY_POLL_INTERVAL_MS, 500)
                .with(KafkaSchemaHistory.consumerConfigPropertyName(
                        ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG), 100)
                .with(KafkaSchemaHistory.consumerConfigPropertyName(
                        ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG), 50000)
                .with(KafkaSchemaHistory.INTERNAL_CONNECTOR_CLASS,
                        "org.apache.kafka.connect.source.SourceConnector")
                .with(KafkaSchemaHistory.INTERNAL_CONNECTOR_ID, "dbz-workload-test");
    }

    private void runSnapshotWorkload(Configuration config, String description) {
        history.configure(config, null, SchemaHistoryMetrics.NOOP, true);
        history.start();
        history.initializeStorage();

        history.startBuffering();
        long startTime = System.nanoTime();
        try {
            writeSchemaHistoryRecords();
        }
        finally {
            history.stopBuffering();
        }
        long endTime = System.nanoTime();

        long durationMs = TimeUnit.NANOSECONDS.toMillis(endTime - startTime);
        double recordsPerSecond = (TOTAL_RECORDS * 1000.0) / durationMs;
        double avgRecordMs = (double) durationMs / TOTAL_RECORDS;

        final String workloadReport = """

                KafkaSchemaHistory Workload (debezium/dbz#2047):
                  Scenario:           %s
                  Records written:    %,d
                  Total time:         %,d ms (%.1f s)
                  Avg per record:     %.2f ms
                  Throughput:         %.0f records/sec
                """.formatted(
                description,
                TOTAL_RECORDS, durationMs, durationMs / 1000.0,
                avgRecordMs, recordsPerSecond);
        Testing.Print.enable();
        Testing.print(workloadReport);

        assertThat(avgRecordMs)
                .as("Average ms per schema history record write (%s)", description)
                .isLessThan(5.0);
    }

    private void writeSchemaHistoryRecords() {
        int logPos = 0;
        for (int db = 0; db < NUM_DATABASES; db++) {
            String dbName = "db_" + db;
            for (int table = 0; table < TABLES_PER_DATABASE; table++) {
                String tableName = "table_" + table;
                String ddl = """
                        CREATE TABLE %s (\
                        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, \
                        name VARCHAR(255) NOT NULL, \
                        description TEXT, \
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, \
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, \
                        status INT DEFAULT 0, \
                        INDEX idx_%s_name (name), \
                        INDEX idx_%s_status (status)\
                        );\
                        """.formatted(tableName, tableName, tableName);

                Map<String, Object> position = Collect.linkMapOf("file", "workload.log", "position", logPos++);
                history.record(source, position, dbName, ddl);
            }
        }
    }

    private void createReplicatedTopicWithConfig(String topicName, Map<String, String> topicConfigs)
            throws Exception {
        Properties adminProps = new Properties();
        adminProps.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaCluster.getBootstrapServers());
        adminProps.setProperty(AdminClientConfig.CLIENT_ID_CONFIG, "workload-admin-" + UUID.randomUUID());

        try (AdminClient admin = AdminClient.create(adminProps)) {
            NewTopic topic = new NewTopic(topicName, 1, (short) 3);
            topic.configs(topicConfigs);
            admin.createTopics(Collections.singleton(topic)).all().get(30, TimeUnit.SECONDS);
        }
    }
}
