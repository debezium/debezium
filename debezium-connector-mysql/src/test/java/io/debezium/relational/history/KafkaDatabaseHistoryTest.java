/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.history;

import static org.fest.assertions.Assertions.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.common.serialization.StringSerializer;
import org.fest.assertions.Assertions;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.mysql.antlr.MySqlAntlrDdlParser;
import io.debezium.doc.FixFor;
import io.debezium.kafka.KafkaCluster;
import io.debezium.relational.Tables;
import io.debezium.relational.ddl.DdlParser;
import io.debezium.text.ParsingException;
import io.debezium.util.Collect;
import io.debezium.util.Testing;

/**
 * @author Randall Hauch
 */
public class KafkaDatabaseHistoryTest {

    private static KafkaCluster kafka;

    private KafkaDatabaseHistory history;
    private Map<String, String> source;
    private Map<String, Object> position;

    private static final int PARTITION_NO = 0;

    @BeforeClass
    public static void startKafka() throws Exception {
        File dataDir = Testing.Files.createTestingDirectory("history_cluster");
        Testing.Files.delete(dataDir);

        // Configure the extra properties to
        kafka = new KafkaCluster().usingDirectory(dataDir)
                .deleteDataPriorToStartup(true)
                .deleteDataUponShutdown(true)
                .addBrokers(1)
                .withKafkaConfiguration(Collect.propertiesOf(
                        "auto.create.topics.enable", "false",
                        "zookeeper.session.timeout.ms", "20000"))
                .startup();
    }

    @AfterClass
    public static void stopKafka() {
        if (kafka != null) {
            kafka.shutdown();
        }
    }

    @Before
    public void beforeEach() throws Exception {
        source = Collect.hashMapOf("server", "my-server");
        setLogPosition(0);
        history = new KafkaDatabaseHistory();
    }

    @After
    public void afterEach() {
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
    public void shouldStartWithEmptyTopicAndStoreDataAndRecoverAllState() throws Exception {
        String topicName = "empty-and-recovery-schema-changes";

        // Create the empty topic ...
        kafka.createTopic(topicName, 1, 1);
        testHistoryTopicContent(topicName, false);
    }

    private void testHistoryTopicContent(String topicName, boolean skipUnparseableDDL) {
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
                .with(KafkaDatabaseHistory.INTERNAL_CONNECTOR_CLASS, "org.apache.kafka.connect.source.SourceConnector")
                .with(KafkaDatabaseHistory.INTERNAL_CONNECTOR_ID, "dbz-test")
                .build();
        history.configure(config, null, DatabaseHistoryMetrics.NOOP, true);
        history.start();

        // Should be able to call start more than once ...
        history.start();

        history.initializeStorage();

        // Calling it another time to ensure we can work with the DB history topic already existing
        history.initializeStorage();

        DdlParser recoveryParser = new MySqlAntlrDdlParser();
        DdlParser ddlParser = new MySqlAntlrDdlParser();
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
        String ddl = "CREATE TABLE foo ( name VARCHAR(255) NOT NULL PRIMARY KEY); \n" +
                "CREATE TABLE customers ( id INTEGER NOT NULL PRIMARY KEY, name VARCHAR(100) NOT NULL ); \n" +
                "CREATE TABLE products ( productId INTEGER NOT NULL PRIMARY KEY, description VARCHAR(255) NOT NULL); \n";
        history.record(source, position, "db1", ddl);

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
        history.record(source, position, "db1", ddl);
        ddlParser.parse(ddl, tables2);
        assertThat(tables2.size()).isEqualTo(2);
        ddlParser.parse(ddl, tables3);
        assertThat(tables3.size()).isEqualTo(2);

        // Record another DDL statement and parse it for 1 of our 3 Tables...
        setLogPosition(10003);
        ddl = "CREATE TABLE suppliers ( supplierId INTEGER NOT NULL PRIMARY KEY, name VARCHAR(255) NOT NULL);";
        history.record(source, position, "db1", ddl);
        ddlParser.parse(ddl, tables3);
        assertThat(tables3.size()).isEqualTo(3);

        // Stop the history (which should stop the producer) ...
        history.stop();
        history = new KafkaDatabaseHistory();
        history.configure(config, null, DatabaseHistoryListener.NOOP, true);
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
        String topicName = "ignore-unparseable-schema-changes";

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

        testHistoryTopicContent(topicName, true);
    }

    @Test(expected = ParsingException.class)
    public void shouldStopOnUnparseableSQL() throws Exception {
        String topicName = "stop-on-unparseable-schema-changes";

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

        testHistoryTopicContent(topicName, false);
    }

    @Test
    public void testExists() {
        String topicName = "exists-schema-changes";

        // happy path
        testHistoryTopicContent(topicName, true);
        assertTrue(history.exists());

        // Set history to use dummy topic
        Configuration config = Configuration.create()
                .with(KafkaDatabaseHistory.BOOTSTRAP_SERVERS, kafka.brokerList())
                .with(KafkaDatabaseHistory.TOPIC, "dummytopic")
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
                .with(KafkaDatabaseHistory.SKIP_UNPARSEABLE_DDL_STATEMENTS, true)
                .with(KafkaDatabaseHistory.INTERNAL_CONNECTOR_CLASS, "org.apache.kafka.connect.source.SourceConnector")
                .with(KafkaDatabaseHistory.INTERNAL_CONNECTOR_ID, "dbz-test")
                .build();

        history.configure(config, null, DatabaseHistoryMetrics.NOOP, true);
        history.start();

        // dummytopic should not exist yet
        assertFalse(history.exists());
    }

    @Test
    @FixFor("DBZ-1886")
    public void differentiateStorageExistsFromHistoryExists() {
        String topicName = "differentiate-storage-exists-schema-changes";

        Configuration config = Configuration.create()
                .with(KafkaDatabaseHistory.BOOTSTRAP_SERVERS, kafka.brokerList())
                .with(KafkaDatabaseHistory.TOPIC, topicName)
                .with(DatabaseHistory.NAME, "my-db-history")
                .with(KafkaDatabaseHistory.RECOVERY_POLL_INTERVAL_MS, 500)
                .with(KafkaDatabaseHistory.consumerConfigPropertyName(
                        ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG),
                        100)
                .with(KafkaDatabaseHistory.consumerConfigPropertyName(
                        ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG),
                        50000)
                .build();

        history.configure(config, null, DatabaseHistoryMetrics.NOOP, true);

        assertFalse(history.storageExists());
        history.initializeStorage();
        assertTrue(history.storageExists());

        assertFalse(history.exists());
        history.start();
        setLogPosition(0);
        String ddl = "CREATE TABLE foo ( name VARCHAR(255) NOT NULL PRIMARY KEY); \n" +
                "CREATE TABLE customers ( id INTEGER NOT NULL PRIMARY KEY, name VARCHAR(100) NOT NULL ); \n" +
                "CREATE TABLE products ( productId INTEGER NOT NULL PRIMARY KEY, desc VARCHAR(255) NOT NULL); \n";
        history.record(source, position, "db1", ddl);
        assertTrue(history.exists());
        assertTrue(history.storageExists());
    }

    @Test
    @FixFor("DBZ-2144")
    public void shouldValidateMandatoryValues() {
        Configuration config = Configuration.create()
                .build();

        final Map<String, ConfigValue> issues = config.validate(KafkaDatabaseHistory.ALL_FIELDS);
        Assertions.assertThat(issues.keySet()).isEqualTo(Collect.unmodifiableSet(
                "database.history.name",
                "database.history.connector.class",
                "database.history.kafka.topic",
                "database.history.kafka.bootstrap.servers",
                "database.history.kafka.recovery.poll.interval.ms",
                "database.history.connector.id",
                "database.history.kafka.recovery.attempts"));
    }
}
