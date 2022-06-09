/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

import static org.fest.assertions.Assertions.assertThat;

import java.io.File;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.OraclePartition;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.antlr.OracleDdlParser;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.doc.FixFor;
import io.debezium.kafka.KafkaCluster;
import io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotContext;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.pipeline.spi.Partition;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.relational.Column;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.ddl.DdlParser;
import io.debezium.relational.history.DatabaseHistory;
import io.debezium.relational.history.DatabaseHistoryMetrics;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.KafkaDatabaseHistory;
import io.debezium.relational.history.TableChanges;
import io.debezium.util.Collect;
import io.debezium.util.Testing;

import oracle.jdbc.OracleTypes;

/**
 * Unit tests for Oracle's database history.
 * 
 * @author Chris Cranford
 */
public class KafkaDatabaseHistoryTest {

    private static final int PARTITION_NO = 0;
    private static KafkaCluster kafka;

    private KafkaDatabaseHistory history;
    private Offsets<Partition, OracleOffsetContext> offsets;

    @BeforeClass
    public static void startKafka() throws Exception {
        File dataDir = Testing.Files.createTestingDirectory("history_cluster");
        Testing.Files.delete(dataDir);

        // Configure the extra properties
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
        final OraclePartition source = new OraclePartition(TestHelper.SERVER_NAME, TestHelper.getDatabaseName());
        final Configuration config = Configuration.empty()
                .edit()
                .with(RelationalDatabaseConnectorConfig.SERVER_NAME, TestHelper.SERVER_NAME)
                .build();
        final OracleOffsetContext position = new OracleOffsetContext(new OracleConnectorConfig(config), Scn.valueOf(999),
                Scn.valueOf(999), null, Scn.valueOf(999), Collections.emptyMap(), false, true, new TransactionContext(),
                new SignalBasedIncrementalSnapshotContext<>());

        offsets = Offsets.of(source, position);
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
    @FixFor("DBZ-4451")
    public void shouldRecoverRenamedTableWithOnlyTheRenamedEntry() throws Exception {
        final String topicName = "schema-history-topic";
        kafka.createTopic(topicName, 1, 1);

        final TableId originalTableId = TableId.parse("ORCLPDB1.DEBEZIUM.DBZ4451A");
        final Table originalTable = Table.editor()
                .tableId(originalTableId)
                .addColumn(Column.editor()
                        .name("ID")
                        .type("NUMBER")
                        .length(9)
                        .scale(0)
                        .jdbcType(OracleTypes.NUMERIC)
                        .optional(false)
                        .create())
                .setPrimaryKeyNames("ID")
                .create();

        final TableId renamedTableId = TableId.parse("ORCLPDB1.DEBEZIUM.DBZ4451B");
        final Table renamedTable = originalTable.edit().tableId(renamedTableId).create();

        final HistoryRecord create = createHistoryRecord(
                Map.of("snapshot_scn", "1", "snapshot", true, "scn", "1", "snapshot_completed", false),
                "DEBEZIUM",
                "CREATE TABLE \"DEBEZIUM\".\"DBZ4451A\" (\"ID\" NUMBER(9,0), PRIMARY KEY(\"ID\");",
                new TableChanges().create(originalTable));

        final HistoryRecord alter = createHistoryRecord(
                Map.of("snapshot_scn", "2", "scn", "2", "commit_scn", "2"),
                "DEBEZIUM",
                "ALTER TABLE DBZ4451A RENAME TO DBZ4451B;",
                new TableChanges().alter(renamedTable, originalTableId));

        // Record the records in the database history topic
        record(topicName, create, alter);

        // Recover the history
        final Tables tables = new Tables();
        recover(topicName, tables);

        // We expect that due to rename, the Tables object only contains DBZ4451B.
        assertThat(tables.size()).isEqualTo(1);
        assertThat(tables.tableIds()).containsOnly(renamedTableId);
    }

    private HistoryRecord createHistoryRecord(Map<String, ?> position, String schemaName, String ddl, TableChanges changes) {
        return new HistoryRecord(
                Map.of("server", TestHelper.SERVER_NAME),
                position,
                TestHelper.getDatabaseName(),
                schemaName,
                ddl,
                changes,
                Instant.now());
    }

    private void record(String topicName, HistoryRecord... records) throws Exception {
        final Configuration intruderConfig = Configuration.create()
                .withDefault(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.brokerList())
                .withDefault(ProducerConfig.CLIENT_ID_CONFIG, "intruder")
                .withDefault(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                .withDefault(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                .build();

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(intruderConfig.asProperties())) {
            for (int i = 0; i < records.length; ++i) {
                final String value = records[i].toString();
                final ProducerRecord<String, String> record = new ProducerRecord<>(topicName, PARTITION_NO, null, value);
                producer.send(record).get();
            }
        }
    }

    private void recover(String topicName, Tables tables) {
        // Start up the history ...
        Configuration config = Configuration.create()
                .with(KafkaDatabaseHistory.BOOTSTRAP_SERVERS, kafka.brokerList())
                .with(KafkaDatabaseHistory.TOPIC, topicName)
                .with(DatabaseHistory.NAME, "my-db-history")
                .with(KafkaDatabaseHistory.RECOVERY_POLL_INTERVAL_MS, 500)
                // new since 0.10.1.0 - we want a low value because we're running everything locally
                // in this test. However, it can't be so low that the broker returns the same
                // messages more than once.
                .with(consumerConfigPropertyName(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG), 100)
                .with(consumerConfigPropertyName(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG), 50000)
                .with(KafkaDatabaseHistory.SKIP_UNPARSEABLE_DDL_STATEMENTS, false)
                .with(KafkaDatabaseHistory.INTERNAL_CONNECTOR_CLASS, "org.apache.kafka.connect.source.SourceConnector")
                .with(KafkaDatabaseHistory.INTERNAL_CONNECTOR_ID, "dbz-test")
                .build();

        // Configure, start, and initialize history
        history.configure(config, null, DatabaseHistoryMetrics.NOOP, true);
        history.start();
        history.initializeStorage();

        // Recover the history
        final DdlParser parser = new OracleDdlParser();
        history.recover(offsets, tables, parser);
    }

    private static String consumerConfigPropertyName(String propertyName) {
        return "database.history.consumer." + propertyName;
    }
}
