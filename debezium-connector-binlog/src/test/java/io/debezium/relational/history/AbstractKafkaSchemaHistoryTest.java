/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.history;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.binlog.BinlogOffsetContext;
import io.debezium.connector.binlog.BinlogPartition;
import io.debezium.doc.FixFor;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.kafka.KafkaClusterUtils;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.pipeline.spi.Partition;
import io.debezium.relational.Tables;
import io.debezium.relational.ddl.DdlParser;
import io.debezium.storage.kafka.history.KafkaSchemaHistory;
import io.debezium.text.ParsingException;
import io.debezium.util.Collect;
import io.debezium.util.Loggings;
import io.strimzi.test.container.StrimziKafkaCluster;

import ch.qos.logback.classic.Level;

/**
 * @author Randall Hauch
 */
public abstract class AbstractKafkaSchemaHistoryTest<P extends BinlogPartition, O extends BinlogOffsetContext<?>> {

    private static StrimziKafkaCluster kafkaCluster;

    private KafkaSchemaHistory history;
    private Offsets<Partition, O> offsets;
    private O position;
    private LogInterceptor interceptor;
    private static final int PARTITION_NO = 0;

    @BeforeClass
    public static void startKafka() {
        Map<String, String> props = new HashMap<>();
        props.put("auto.create.topics.enable", "false");

        // Configure the extra properties to
        kafkaCluster = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
                .withNumberOfBrokers(1)
                .withAdditionalKafkaConfiguration(props)
                .withSharedNetwork()
                .build();

        kafkaCluster.start();
    }

    @AfterClass
    public static void stopKafka() {
        if (kafkaCluster != null) {
            kafkaCluster.stop();
        }
    }

    @Before
    public void beforeEach() throws Exception {
        P source = createPartition("my-server", "my-db");
        Configuration config = Configuration.empty()
                .edit()
                .with(CommonConnectorConfig.TOPIC_PREFIX, "dbserver1").build();
        position = createOffsetContext(config);
        offsets = Offsets.of(source, position);

        setLogPosition(0);
        history = new KafkaSchemaHistory();
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
        KafkaClusterUtils.createTopic(topicName, 1, (short) 1, kafkaCluster.getBootstrapServers());
        testHistoryTopicContent(topicName, false);
    }

    protected abstract P createPartition(String serverName, String databaseName);

    protected abstract O createOffsetContext(Configuration config);

    protected abstract DdlParser getDdlParser();

    private void testHistoryTopicContent(String topicName, boolean skipUnparseableDDL) throws InterruptedException {
        interceptor = new LogInterceptor(KafkaSchemaHistory.class);
        // Start up the history ...
        Configuration config = Configuration.create()
                .with(KafkaSchemaHistory.BOOTSTRAP_SERVERS, kafkaCluster.getBootstrapServers())
                .with(KafkaSchemaHistory.TOPIC, topicName)
                .with(SchemaHistory.NAME, "my-db-history")
                .with(KafkaSchemaHistory.RECOVERY_POLL_INTERVAL_MS, 500)
                // new since 0.10.1.0 - we want a low value because we're running everything locally
                // in this test. However, it can't be so low that the broker returns the same
                // messages more than once.
                .with(KafkaSchemaHistory.consumerConfigPropertyName(
                        ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG),
                        100)
                .with(KafkaSchemaHistory.consumerConfigPropertyName(
                        ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG),
                        50000)
                .with(KafkaSchemaHistory.SKIP_UNPARSEABLE_DDL_STATEMENTS, skipUnparseableDDL)
                .with(KafkaSchemaHistory.DDL_FILTER, "CREATE\\s+ROLE.*")
                .with(KafkaSchemaHistory.INTERNAL_CONNECTOR_CLASS, "org.apache.kafka.connect.source.SourceConnector")
                .with(KafkaSchemaHistory.INTERNAL_CONNECTOR_ID, "dbz-test")
                .build();
        history.configure(config, null, SchemaHistoryMetrics.NOOP, true);
        history.start();

        // Should be able to call start more than once ...
        history.start();

        history.initializeStorage();

        // Calling it another time to ensure we can work with the DB history topic already existing
        history.initializeStorage();

        DdlParser recoveryParser = getDdlParser();
        DdlParser ddlParser = getDdlParser();
        ddlParser.setCurrentSchema("db1"); // recover does this, so we need to as well
        Tables tables1 = new Tables();
        Tables tables2 = new Tables();
        Tables tables3 = new Tables();

        // Recover from the very beginning ...
        setLogPosition(0);
        history.recover(offsets, tables1, recoveryParser);

        // There should have been nothing to recover ...
        assertThat(tables1.size()).isEqualTo(0);

        // Now record schema changes, which writes out to kafka but doesn't actually change the Tables ...
        setLogPosition(10);
        String ddl = "CREATE TABLE foo ( name VARCHAR(255) NOT NULL PRIMARY KEY); \n" +
                "CREATE TABLE customers ( id INTEGER NOT NULL PRIMARY KEY, name VARCHAR(100) NOT NULL ); \n" +
                "CREATE TABLE products ( productId INTEGER NOT NULL PRIMARY KEY, description VARCHAR(255) NOT NULL); \n";
        history.record(offsets.getTheOnlyPartition().getSourcePartition(), offsets.getTheOnlyOffset().getOffset(), "db1", ddl);

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
        history.record(offsets.getTheOnlyPartition().getSourcePartition(), offsets.getTheOnlyOffset().getOffset(), "db1", ddl);
        ddlParser.parse(ddl, tables2);
        assertThat(tables2.size()).isEqualTo(2);
        ddlParser.parse(ddl, tables3);
        assertThat(tables3.size()).isEqualTo(2);

        // Record another DDL statement and parse it for 1 of our 3 Tables...
        setLogPosition(10003);
        ddl = "CREATE TABLE suppliers ( supplierId INTEGER NOT NULL PRIMARY KEY, name VARCHAR(255) NOT NULL);";
        history.record(offsets.getTheOnlyPartition().getSourcePartition(), offsets.getTheOnlyOffset().getOffset(), "db1", ddl);
        ddlParser.parse(ddl, tables3);
        assertThat(tables3.size()).isEqualTo(3);

        // Stop the history (which should stop the producer) ...
        history.stop();
        history = new KafkaSchemaHistory();
        history.configure(config, null, SchemaHistoryListener.NOOP, true);
        // no need to start

        // Recover from the very beginning to just past the first change ...
        Tables recoveredTables = new Tables();
        setLogPosition(15);
        history.recover(offsets, recoveredTables, recoveryParser);
        assertThat(recoveredTables).isEqualTo(tables1);

        // Recover from the very beginning to just past the second change ...
        recoveredTables = new Tables();
        setLogPosition(50);
        history.recover(offsets, recoveredTables, recoveryParser);
        assertThat(recoveredTables).isEqualTo(tables2);

        // Recover from the very beginning to just past the third change ...
        recoveredTables = new Tables();
        setLogPosition(10010);
        history.recover(offsets, recoveredTables, recoveryParser);
        assertThat(recoveredTables).isEqualTo(tables3);

        // Recover from the very beginning to way past the third change ...
        recoveredTables = new Tables();
        setLogPosition(100000010);
        history.recover(offsets, recoveredTables, recoveryParser);
        assertThat(recoveredTables).isEqualTo(tables3);
    }

    protected void setLogPosition(int index) {
        position.setBinlogStartPoint("my-txn-file.log", index);
    }

    @Test
    public void shouldIgnoreUnparseableMessages() throws Exception {
        String topicName = "ignore-unparseable-schema-changes";

        // Create the empty topic ...
        KafkaClusterUtils.createTopic(topicName, 1, (short) 1, kafkaCluster.getBootstrapServers());

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
        final ProducerRecord<String, String> invalidSQLProcedure = new ProducerRecord<>(topicName, PARTITION_NO, null,
                "{\"source\":{\"server\":\"my-server\"},\"position\":{\"filename\":\"my-txn-file.log\",\"position\":39},\"databaseName\":\"db1\",\"ddl\":\"CREATE DEFINER=`myUser`@`%` PROCEDURE `tableAFetchCount`(        in p_uniqueID int        )BEGINselect count(*) into @propCount from tableA  where uniqueID = p_uniqueID;    select count(*) into @completeCount from tableA  where uniqueID = p_uniqueID and isComplete = 1;       select  uniqueID,   @propCount as propCount, @completeCount as completeCount, @completeCount/ @propCount * 100 as completePct        where uniqueID = p_uniqueID;END\"}");

        final Configuration intruderConfig = Configuration.create()
                .withDefault(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaCluster.getBootstrapServers())
                .withDefault(ProducerConfig.CLIENT_ID_CONFIG, "intruder")
                .withDefault(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                .withDefault(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                .build();
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(intruderConfig.asProperties())) {
            producer.send(nullRecord).get();
            producer.send(emptyRecord).get();
            producer.send(noSourceRecord).get();
            producer.send(noPositionRecord).get();
            producer.send(invalidJSONRecord1).get();
            producer.send(invalidJSONRecord2).get();
            producer.send(invalidSQL).get();
            producer.send(invalidSQLProcedure).get();
        }

        testHistoryTopicContent(topicName, true);
    }

    @Test(expected = ParsingException.class)
    public void shouldStopOnUnparseableSQL() throws Exception {
        String topicName = "stop-on-unparseable-schema-changes";

        // Create the empty topic ...
        KafkaClusterUtils.createTopic(topicName, 1, (short) 1, kafkaCluster.getBootstrapServers());

        // Create invalid records
        final ProducerRecord<String, String> invalidSQL = new ProducerRecord<>(topicName, PARTITION_NO, null,
                "{\"source\":{\"server\":\"my-server\"},\"position\":{\"filename\":\"my-txn-file.log\",\"position\":39},\"databaseName\":\"db1\",\"ddl\":\"xxxDROP TABLE foo;\"}");

        final Configuration intruderConfig = Configuration.create()
                .withDefault(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaCluster.getBootstrapServers())
                .withDefault(ProducerConfig.CLIENT_ID_CONFIG, "intruder")
                .withDefault(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                .withDefault(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                .build();
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(intruderConfig.asProperties())) {
            producer.send(invalidSQL).get();
        }

        testHistoryTopicContent(topicName, false);
    }

    @Test
    public void shouldSkipMessageOnDDLFilter() throws Exception {
        String topicName = "stop-on-ddlfilter-schema-changes";

        final LogInterceptor logInterceptor = new LogInterceptor(Loggings.class);
        logInterceptor.setLoggerLevel(Loggings.class, Level.TRACE);

        // Create the empty topic ...
        KafkaClusterUtils.createTopic(topicName, 1, (short) 1, kafkaCluster.getBootstrapServers());

        // Create invalid records
        final ProducerRecord<String, String> invalidSQL = new ProducerRecord<>(topicName, PARTITION_NO, null,
                "{\"source\":{\"server\":\"my-server\"},\"position\":{\"filename\":\"my-txn-file.log\",\"position\":39},\"databaseName\":\"db1\",\"ddl\":\"create  role if not exists 'RL_COMPLIANCE_NSA';\"}");

        final Configuration intruderConfig = Configuration.create()
                .withDefault(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaCluster.getBootstrapServers())
                .withDefault(ProducerConfig.CLIENT_ID_CONFIG, "intruder")
                .withDefault(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                .withDefault(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                .build();
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(intruderConfig.asProperties())) {
            producer.send(invalidSQL).get();
        }

        testHistoryTopicContent(topicName, true);

        boolean result = interceptor
                .containsMessage("a DDL 'create  role if not exists 'RL_COMPLIANCE_NSA';' was filtered out of processing by regular expression 'CREATE\\s+ROLE.*");
        assertThat(result).isTrue();
    }

    @Test
    public void testExists() throws InterruptedException {
        String topicName = "exists-schema-changes";

        // happy path
        testHistoryTopicContent(topicName, true);
        assertTrue(history.exists());

        // Set history to use dummy topic
        Configuration config = Configuration.create()
                .with(KafkaSchemaHistory.BOOTSTRAP_SERVERS, kafkaCluster.getBootstrapServers())
                .with(KafkaSchemaHistory.TOPIC, "dummytopic")
                .with(SchemaHistory.NAME, "my-db-history")
                .with(KafkaSchemaHistory.RECOVERY_POLL_INTERVAL_MS, 500)
                // new since 0.10.1.0 - we want a low value because we're running everything locally
                // in this test. However, it can't be so low that the broker returns the same
                // messages more than once.
                .with(KafkaSchemaHistory.consumerConfigPropertyName(
                        ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG),
                        100)
                .with(KafkaSchemaHistory.consumerConfigPropertyName(
                        ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG),
                        50000)
                .with(KafkaSchemaHistory.SKIP_UNPARSEABLE_DDL_STATEMENTS, true)
                .with(KafkaSchemaHistory.INTERNAL_CONNECTOR_CLASS, "org.apache.kafka.connect.source.SourceConnector")
                .with(KafkaSchemaHistory.INTERNAL_CONNECTOR_ID, "dbz-test")
                .build();

        history.configure(config, null, SchemaHistoryMetrics.NOOP, true);
        history.start();

        // dummytopic should not exist yet
        assertFalse(history.exists());
    }

    @Test
    @FixFor("DBZ-1886")
    public void differentiateStorageExistsFromHistoryExists() {
        String topicName = "differentiate-storage-exists-schema-changes";

        Configuration config = Configuration.create()
                .with(KafkaSchemaHistory.BOOTSTRAP_SERVERS, kafkaCluster.getBootstrapServers())
                .with(KafkaSchemaHistory.TOPIC, topicName)
                .with(SchemaHistory.NAME, "my-db-history")
                .with(KafkaSchemaHistory.RECOVERY_POLL_INTERVAL_MS, 500)
                .with(KafkaSchemaHistory.consumerConfigPropertyName(
                        ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG),
                        100)
                .with(KafkaSchemaHistory.consumerConfigPropertyName(
                        ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG),
                        50000)
                .build();

        history.configure(config, null, SchemaHistoryMetrics.NOOP, true);

        assertFalse(history.storageExists());
        history.initializeStorage();
        assertTrue(history.storageExists());

        assertFalse(history.exists());
        history.start();
        setLogPosition(0);
        String ddl = "CREATE TABLE foo ( name VARCHAR(255) NOT NULL PRIMARY KEY); \n" +
                "CREATE TABLE customers ( id INTEGER NOT NULL PRIMARY KEY, name VARCHAR(100) NOT NULL ); \n" +
                "CREATE TABLE products ( productId INTEGER NOT NULL PRIMARY KEY, desc VARCHAR(255) NOT NULL); \n";
        history.record(offsets.getTheOnlyPartition().getSourcePartition(), offsets.getTheOnlyOffset().getOffset(), "db1", ddl);
        assertTrue(history.exists());
        assertTrue(history.storageExists());
    }

    @Test
    @FixFor("DBZ-2144")
    public void shouldValidateMandatoryValues() {
        Configuration config = Configuration.create()
                .build();

        final Map<String, ConfigValue> issues = config.validate(KafkaSchemaHistory.ALL_FIELDS);
        assertThat(issues.keySet()).isEqualTo(Collect.unmodifiableSet(
                "schema.history.internal.name",
                "schema.history.internal.connector.class",
                "schema.history.internal.kafka.topic",
                "schema.history.internal.kafka.bootstrap.servers",
                "schema.history.internal.kafka.recovery.poll.interval.ms",
                "schema.history.internal.connector.id",
                "schema.history.internal.kafka.recovery.attempts",
                "schema.history.internal.kafka.query.timeout.ms"));
    }

    @Test
    @FixFor("DBZ-4518")
    public void shouldConnectionTimeoutIfValueIsTooLow() {
        Configuration config = Configuration.create()
                .with(KafkaSchemaHistory.BOOTSTRAP_SERVERS, kafkaCluster.getBootstrapServers())
                .with(KafkaSchemaHistory.TOPIC, "this-should-not-get-created")
                .with(SchemaHistory.NAME, "my-db-history")
                .with(KafkaSchemaHistory.KAFKA_QUERY_TIMEOUT_MS, 1)
                .build();

        history.configure(config, null, SchemaHistoryMetrics.NOOP, true);
        history.start();

        try {
            history.initializeStorage();
        }
        catch (Exception ex) {
            assertEquals(TimeoutException.class, ex.getCause().getClass());
        }

        assertTrue(history.storageExists());
    }
}
