/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static io.debezium.junit.EqualityCheck.GREATER_THAN_OR_EQUAL;
import static io.debezium.junit.EqualityCheck.LESS_THAN;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.doc.FixFor;
import io.debezium.embedded.AbstractAsyncEngineConnectorTest;
import io.debezium.junit.SkipWhenDatabaseVersion;
import io.debezium.util.Testing;

/**
 * @author Chris Cranford
 */
@SkipWhenDatabaseVersion(check = LESS_THAN, major = 5, minor = 7, reason = "Generated values were not added until MySQL 5.7")
@SkipWhenDatabaseVersion(check = GREATER_THAN_OR_EQUAL, major = 11, reason = "MariaDB does not allow you to specify AS and NOT NULL")
public class MysqlDefaultGeneratedValueIT extends AbstractAsyncEngineConnectorTest {

    // 4 meta events (set character_set etc.) and then 15 tables with 3 events each (drop DDL, create DDL, insert)
    private static final int EVENT_COUNT = 4 + 15 * 3;

    private static final Path SCHEMA_HISTORY_PATH = Testing.Files.createTestingPath("file-schema-history-connect.txt").toAbsolutePath();
    private final UniqueDatabase DATABASE = new UniqueDatabase("myServer1", "default_value_generated")
            .withDbHistoryPath(SCHEMA_HISTORY_PATH);

    private Configuration config;

    @Before
    public void beforeEach() {
        stopConnector();
        DATABASE.createAndInitialize();
        initializeConnectorTestFramework();
        Testing.Files.delete(SCHEMA_HISTORY_PATH);
    }

    @After
    public void afterEach() {
        try {
            stopConnector();
        }
        finally {
            Testing.Files.delete(SCHEMA_HISTORY_PATH);
        }
    }

    @Test
    @FixFor("DBZ-1123")
    public void generatedValueTest() throws InterruptedException {
        config = DATABASE.defaultConfig()
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.INITIAL)
                .build();
        start(MySqlConnector.class, config);

        // Testing.Print.enable();

        SourceRecords records = consumeRecordsByTopic(EVENT_COUNT);
        SourceRecord record = records.recordsForTopic(DATABASE.topicForTable("GENERATED_TABLE")).get(0);
        validate(record);

        Schema schemaB = record.valueSchema().fields().get(1).schema().fields().get(1).schema();
        Integer recordB = ((Struct) record.value()).getStruct("after").getInt32("B");
        Schema schemaC = record.valueSchema().fields().get(1).schema().fields().get(2).schema();
        Integer recordC = ((Struct) record.value()).getStruct("after").getInt32("C");

        // Calculated default value is reported as null in schema
        assertThat(schemaB.isOptional()).isEqualTo(true);
        assertThat(schemaB.defaultValue()).isEqualTo(null);
        assertThat(schemaC.isOptional()).isEqualTo(false);
        assertThat(schemaC.defaultValue()).isEqualTo(null);

        assertThat(recordB).isEqualTo(30);
        assertThat(recordC).isEqualTo(45);

        validate(record);
    }
}
