/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.transforms;

import static io.debezium.transforms.partitions.ComputePartitionConfigDefinition.FIELD_TABLE_FIELD_NAME_MAPPINGS_CONF;
import static io.debezium.transforms.partitions.ComputePartitionConfigDefinition.FIELD_TABLE_PARTITION_NUM_MAPPINGS_CONF;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySqlConnector;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.connector.mysql.UniqueDatabase;
import io.debezium.embedded.AbstractConnectorTest;

/**
 * Tests Compute Partition SMT.
 *
 * @author Mario Fiore Vitale
 */
public class ComputePartitionIT extends AbstractConnectorTest {

    private static final Path SCHEMA_HISTORY_PATH = Files.createTestingPath("file-schema-history-decimal-column.txt")
            .toAbsolutePath();
    private final UniqueDatabase DATABASE = new UniqueDatabase("myserver", "compute_partition_smt_test")
            .withDbHistoryPath(SCHEMA_HISTORY_PATH);

    @Before
    public void beforeEach() {
        stopConnector();
        DATABASE.createAndInitialize();
        initializeConnectorTestFramework();
        Files.delete(SCHEMA_HISTORY_PATH);
    }

    @After
    public void afterEach() {
        try {
            stopConnector();
        }
        finally {
            Files.delete(SCHEMA_HISTORY_PATH);
        }
    }

    @Test
    public void allRecordRoutedToCorrectPartition() throws InterruptedException {

        Configuration config = DATABASE.defaultConfig()
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.INITIAL)
                .with("topic.creation.default.partitions", "2")
                .with("topic.creation.default.replication.factor", "1")
                .with("transforms", "ComputePartition")
                .with("transforms.ComputePartition.type", "io.debezium.transforms.partitions.ComputePartition")
                .with("transforms.ComputePartition." + FIELD_TABLE_FIELD_NAME_MAPPINGS_CONF, DATABASE.qualifiedTableName("products") + ":name")
                .with("transforms.ComputePartition." + FIELD_TABLE_PARTITION_NUM_MAPPINGS_CONF, DATABASE.qualifiedTableName("products") + ":2")
                .build();

        // Start the connector ...
        start(MySqlConnector.class, config);

        // ---------------------------------------------------------------------------------------------------------------
        // Consume all of the events due to startup and initialization of the database
        // ---------------------------------------------------------------------------------------------------------------

        int numDDL = 6;
        int numInserts = 9;
        SourceRecords records = consumeRecordsByTopic(numInserts + numDDL);
        stopConnector();
        assertThat(records).isNotNull();
        records.forEach(this::validate);

        List<SourceRecord> products = records.recordsForTopic(DATABASE.topicForTable("products"));
        assertThat(products).hasSize(9);

        Map<Object, Integer> partitionByRecordId = products.stream()
                .collect(Collectors.toMap(r -> ((Struct) ((Struct) r.value()).get("after")).get("id"), ConnectRecord::kafkaPartition));

        assertThat(partitionByRecordId).containsAllEntriesOf(
                Map.of(101, 1,
                        102, 1,
                        103, 0,
                        104, 0,
                        105, 0,
                        106, 0,
                        107, 0,
                        108, 0,
                        109, 1));

    }
}
