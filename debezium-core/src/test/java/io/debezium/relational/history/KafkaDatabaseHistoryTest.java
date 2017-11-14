/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.history;

import java.io.File;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.fest.assertions.Assertions.assertThat;

import io.debezium.config.Configuration;
import io.debezium.kafka.KafkaCluster;
import io.debezium.relational.Tables;
import io.debezium.relational.ddl.DdlParser;
import io.debezium.relational.ddl.DdlParserSql2003;
import io.debezium.text.ParsingException;
import io.debezium.util.Collect;
import io.debezium.util.Testing;

/**
 * @author Randall Hauch
 */
public class KafkaDatabaseHistoryTest {

    private KafkaDatabaseHistory history;
    private KafkaCluster kafka;
    private Map<String, String> source;
    private Map<String, Object> position;
    private String topicName;
    private String ddl;

    private static final int PARTITION_NO = 0;

    @Before
    public void beforeEach() throws Exception {
        source = Collect.hashMapOf("server", "my-server");
        setLogPosition(0);
        topicName = "schema-changes-topic";

        File dataDir = Testing.Files.createTestingDirectory("history_cluster");
        Testing.Files.delete(dataDir);

        // Configure the extra properties to
        kafka = new KafkaCluster().usingDirectory(dataDir)
                                  .deleteDataPriorToStartup(true)
                                  .deleteDataUponShutdown(true)
                                  .addBrokers(1)
                                  .startup();
        history = new KafkaDatabaseHistory();
    }

    @After
    public void afterEach() {
        try {
            if (history != null) history.stop();
        } finally {
            history = null;
            try {
                if (kafka != null) kafka.shutdown();
            } finally {
                kafka = null;
            }
        }
    }

    @Test
    public void shouldStartWithEmptyTopicAndStoreDataAndRecoverAllState() throws Exception {
        // Create the empty topic ...
        kafka.createTopic(topicName, 1, 1);
        testHistoryTopicContent(false);
    }

    private void testHistoryTopicContent(boolean skipUnparseableDDL) {
        // Start up the history ...
        Configuration config = Configuration.create()
                                            .with(KafkaDatabaseHistory.BOOTSTRAP_SERVERS, kafka.brokerList())
                                            .with(KafkaDatabaseHistory.TOPIC, topicName)
                                            .with(DatabaseHistory.NAME, "my-db-history")
                                            .with(KafkaDatabaseHistory.RECOVERY_POLL_INTERVAL_MS, 500)
                                            // new since 0.10.1.0 - we want a low value because we're running everything locally
                                            // in this test. However, it can't be so low that the broker returns the same
                                            // messages more than once.
                                            .with(KafkaDatabaseHistory.consumerConfigPropertyName(
                                                  ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG),
                                                  100)
                                            .with(KafkaDatabaseHistory.consumerConfigPropertyName(
                                                  ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG),
                                                  50000)
                                            .with(KafkaDatabaseHistory.SKIP_UNPARSEABLE_DDL_STATEMENTS, skipUnparseableDDL)
                                            .build();
        history.configure(config, null);
        history.start();

        // Should be able to call start more than once ...
        history.start();

        DdlParser recoveryParser = new DdlParserSql2003();
        DdlParser ddlParser = new DdlParserSql2003();
        ddlParser.setCurrentSchema("db1"); // recover does this, so we need to as well
        Tables tables1 = new Tables();
        Tables tables2 = new Tables();
        Tables tables3 = new Tables();

        // Recover from the very beginning ...
        setLogPosition(0);
        history.recover(source, position, tables1, recoveryParser);

        // There should have been nothing to recover ...
        assertThat(tables1.size()).isEqualTo(0);

        // Now record schema changes, which writes out to kafka but doesn't actually change the Tables ...
        setLogPosition(10);
        ddl = "CREATE TABLE foo ( name VARCHAR(255) NOT NULL PRIMARY KEY); \n" +
                "CREATE TABLE customers ( id INTEGER NOT NULL PRIMARY KEY, name VARCHAR(100) NOT NULL ); \n" +
                "CREATE TABLE products ( productId INTEGER NOT NULL PRIMARY KEY, desc VARCHAR(255) NOT NULL); \n";
        history.record(source, position, "db1", tables1, ddl);

        // Parse the DDL statement 3x and each time update a different Tables object ...
        ddlParser.parse(ddl, tables1);
        assertThat(tables1.size()).isEqualTo(3);
        ddlParser.parse(ddl, tables2);
        assertThat(tables2.size()).isEqualTo(3);
        ddlParser.parse(ddl, tables3);
        assertThat(tables3.size()).isEqualTo(3);

        // Record a drop statement and parse it for 2 of our 3 Tables...
        setLogPosition(39);
        ddl = "DROP TABLE foo;";
        history.record(source, position, "db1", tables2, ddl);
        ddlParser.parse(ddl, tables2);
        assertThat(tables2.size()).isEqualTo(2);
        ddlParser.parse(ddl, tables3);
        assertThat(tables3.size()).isEqualTo(2);

        // Record another DDL statement and parse it for 1 of our 3 Tables...
        setLogPosition(10003);
        ddl = "CREATE TABLE suppliers ( supplierId INTEGER NOT NULL PRIMARY KEY, name VARCHAR(255) NOT NULL);";
        history.record(source, position, "db1", tables3, ddl);
        ddlParser.parse(ddl, tables3);
        assertThat(tables3.size()).isEqualTo(3);

        // Stop the history (which should stop the producer) ...
        history.stop();
        history = new KafkaDatabaseHistory();
        history.configure(config, null);
        // no need to start

        // Recover from the very beginning to just past the first change ...
        Tables recoveredTables = new Tables();
        setLogPosition(15);
        history.recover(source, position, recoveredTables, recoveryParser);
        assertThat(recoveredTables).isEqualTo(tables1);

        // Recover from the very beginning to just past the second change ...
        recoveredTables = new Tables();
        setLogPosition(50);
        history.recover(source, position, recoveredTables, recoveryParser);
        assertThat(recoveredTables).isEqualTo(tables2);

        // Recover from the very beginning to just past the third change ...
        recoveredTables = new Tables();
        setLogPosition(10010);
        history.recover(source, position, recoveredTables, recoveryParser);
        assertThat(recoveredTables).isEqualTo(tables3);

        // Recover from the very beginning to way past the third change ...
        recoveredTables = new Tables();
        setLogPosition(100000010);
        history.recover(source, position, recoveredTables, recoveryParser);
        assertThat(recoveredTables).isEqualTo(tables3);
    }

    protected void setLogPosition(int index) {
        this.position = Collect.hashMapOf("filename", "my-txn-file.log",
                                          "position", index);
    }

    @Test
    public void shouldIgnoreUnparseableMessages() throws Exception {
        // Create the empty topic ...
        kafka.createTopic(topicName, 1, 1);

        // Create invalid records
        final ProducerRecord<String, String> nullRecord = new ProducerRecord<>(topicName, PARTITION_NO, null, null);
        final ProducerRecord<String, String> emptyRecord = new ProducerRecord<>(topicName, PARTITION_NO, null, "");
        final ProducerRecord<String, String> noSourceRecord = new ProducerRecord<>(topicName, PARTITION_NO, null,
                "{\"position\":{\"filename\":\"my-txn-file.log\",\"position\":39},\"databaseName\":\"db1\",\"ddl\":\"DROP TABLE foo;\"}");
        final ProducerRecord<String, String> noPositionRecord = new ProducerRecord<>(topicName, PARTITION_NO, null,
                "{\"source\":{\"server\":\"my-server\"},\"databaseName\":\"db1\",\"ddl\":\"DROP TABLE foo;\"}");
        final ProducerRecord<String, String> invalidJSONRecord1 = new ProducerRecord<>(topicName, PARTITION_NO, null,
                "{\"source\":{\"server\":\"my-server\"},\"position\":{\"filename\":\"my-txn-file.log\",\"position\":39},\"databaseName\":\"db1\",\"ddl\":\"DROP TABLE foo;\"");
        final ProducerRecord<String, String> invalidJSONRecord2 = new ProducerRecord<>(topicName, PARTITION_NO, null,
                "\"source\":{\"server\":\"my-server\"},\"position\":{\"filename\":\"my-txn-file.log\",\"position\":39},\"databaseName\":\"db1\",\"ddl\":\"DROP TABLE foo;\"}");
        final ProducerRecord<String, String> invalidSQL = new ProducerRecord<>(topicName, PARTITION_NO, null,
                "{\"source\":{\"server\":\"my-server\"},\"position\":{\"filename\":\"my-txn-file.log\",\"position\":39},\"databaseName\":\"db1\",\"ddl\":\"xxxDROP TABLE foo;\"}");

        final Configuration intruderConfig = Configuration.create()
                .withDefault(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.brokerList())
                .withDefault(ProducerConfig.CLIENT_ID_CONFIG, "intruder")
                .withDefault(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                .withDefault(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                .build();
        try (final KafkaProducer<String, String> producer = new KafkaProducer<>(intruderConfig.asProperties())) {
            producer.send(nullRecord).get();
            producer.send(emptyRecord).get();
            producer.send(noSourceRecord).get();
            producer.send(noPositionRecord).get();
            producer.send(invalidJSONRecord1).get();
            producer.send(invalidJSONRecord2).get();
            producer.send(invalidSQL).get();
        }

        testHistoryTopicContent(true);
    }

    @Test(expected = ParsingException.class)
    public void shouldStopOnUnparseableSQL() throws Exception {
        // Create the empty topic ...
        kafka.createTopic(topicName, 1, 1);

        // Create invalid records
        final ProducerRecord<String, String> invalidSQL = new ProducerRecord<>(topicName, PARTITION_NO, null,
                "{\"source\":{\"server\":\"my-server\"},\"position\":{\"filename\":\"my-txn-file.log\",\"position\":39},\"databaseName\":\"db1\",\"ddl\":\"xxxDROP TABLE foo;\"}");

        final Configuration intruderConfig = Configuration.create()
                .withDefault(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.brokerList())
                .withDefault(ProducerConfig.CLIENT_ID_CONFIG, "intruder")
                .withDefault(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                .withDefault(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                .build();
        try (final KafkaProducer<String, String> producer = new KafkaProducer<>(intruderConfig.asProperties())) {
            producer.send(invalidSQL).get();
        }

        testHistoryTopicContent(false);
    }
}
