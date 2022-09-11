/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.compaction.tool.service.compaction;

import java.io.File;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.connector.mysql.MySqlOffsetContext;
import io.debezium.connector.mysql.MySqlPartition;
import io.debezium.connector.mysql.MySqlReadOnlyIncrementalSnapshotContext;
import io.debezium.connector.mysql.SourceInfo;
import io.debezium.connector.mysql.antlr.MySqlAntlrDdlParser;
import io.debezium.kafka.KafkaCluster;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.pipeline.spi.Partition;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.Tables;
import io.debezium.util.Collect;
import io.debezium.util.Testing;

public class KafkaDatabaseHistoryCompactionTest {

    private static KafkaCluster kafka;
    private KafkaDatabaseHistoryCompaction historyCompaction;
    private Offsets<Partition, MySqlOffsetContext> offsets;
    private MySqlOffsetContext position;

    private static final int PARTITION_NO = 0;
    private Configuration config;

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
        System.out.println(kafka.zkPort());
    }

    @AfterClass
    public static void stopKafka() {
        if (kafka != null) {
            kafka.shutdown();
            kafka = null;
        }
    }

    @Before
    public void beforeEach() {
        MySqlPartition source = new MySqlPartition("dbserver", "my-db");
        this.config = Configuration.empty().edit().with(RelationalDatabaseConnectorConfig.SERVER_NAME, "dbserver").build();

        position = new MySqlOffsetContext(false, true, new TransactionContext(), new MySqlReadOnlyIncrementalSnapshotContext<>(),
                new SourceInfo(new MySqlConnectorConfig(config)));

        setLogPosition(0);
        offsets = Offsets.of(source, position);

    }

    @After
    public void afterEach() {
        try {
            if (historyCompaction != null) {
                historyCompaction.stop();
            }
        }
        finally {
            historyCompaction = null;
        }
    }

    @Test
    public void shouldStartWithEmptyHistoryTopic() {
        final String historyTopicName = "my-db-history-topic";

        kafka.createTopic(historyTopicName, 1, 1);

    }

    @Test
    public void shouldPerformCompactionOnHistoryTopic() {
        final String bootstrapServers = "localhost:9092";
        final String historyTopic = "my-db-history-topic";
        final String compactedHistoryTopic = "my-compacted-db-history-topic";

        historyCompaction = new KafkaDatabaseHistoryCompaction(bootstrapServers, historyTopic, compactedHistoryTopic);

        historyCompaction.configure(config, null, DatabaseHistoryListener.NOOP, false);

        historyCompaction.start();

        historyCompaction.record(offsets.getTheOnlyPartition().getSourcePartition(), offsets.getTheOnlyOffset().getOffset(), new Tables(),
                new MySqlAntlrDdlParser());

        historyCompaction.stop();
    }

    private void setLogPosition(int index) {
        position.setBinlogStartPoint("my-txn-file.log", index);
    }
}
